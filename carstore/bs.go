package carstore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/indigo/models"

	blockformat "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	cbor "github.com/ipfs/go-ipld-cbor"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-libipfs/blocks"
	logging "github.com/ipfs/go-log"
	car "github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"gorm.io/gorm"
)

var log = logging.Logger("carstore")

const MaxSliceLength = 2 << 20

const BigShardThreshold = 2 << 20

type CarStore struct {
	meta    *gorm.DB
	rootDir string

	lscLk          sync.Mutex
	lastShardCache map[models.Uid]*CarShard
}

func NewCarStore(meta *gorm.DB, root string) (*CarStore, error) {
	if _, err := os.Stat(root); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		if err := os.Mkdir(root, 0775); err != nil {
			return nil, err
		}
	}
	if err := meta.AutoMigrate(&CarShard{}, &blockRef{}); err != nil {
		return nil, err
	}
	if err := meta.AutoMigrate(&staleRef{}); err != nil {
		return nil, err
	}

	return &CarStore{
		meta:           meta,
		rootDir:        root,
		lastShardCache: make(map[models.Uid]*CarShard),
	}, nil
}

type UserInfo struct {
	gorm.Model
	Head string
}

type CarShard struct {
	ID        uint `gorm:"primarykey"`
	CreatedAt time.Time

	Root      models.DbCID `gorm:"index"`
	DataStart int64
	Seq       int `gorm:"index:idx_car_shards_seq;index:idx_car_shards_usr_seq,priority:2,sort:desc"`
	Path      string
	Usr       models.Uid `gorm:"index:idx_car_shards_usr;index:idx_car_shards_usr_seq,priority:1"`
	Rev       string
}

type blockRef struct {
	ID     uint         `gorm:"primarykey"`
	Cid    models.DbCID `gorm:"index"`
	Shard  uint         `gorm:"index"`
	Offset int64
	//User   uint `gorm:"index"`
}

type staleRef struct {
	ID   uint `gorm:"primarykey"`
	Cid  *models.DbCID
	Cids []byte
	Usr  models.Uid `gorm:"index"`
}

func (sr *staleRef) getCids() ([]cid.Cid, error) {
	if sr.Cid != nil {
		return []cid.Cid{sr.Cid.CID}, nil
	}

	return unpackCids(sr.Cids)
}

func packCids(cids []cid.Cid) []byte {
	buf := new(bytes.Buffer)
	for _, c := range cids {
		buf.Write(c.Bytes())
	}

	return buf.Bytes()
}

func unpackCids(b []byte) ([]cid.Cid, error) {
	br := bytes.NewReader(b)
	var out []cid.Cid
	for {
		_, c, err := cid.CidFromReader(br)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		out = append(out, c)
	}

	return out, nil
}

type userView struct {
	cs   *CarStore
	user models.Uid

	cache    map[cid.Cid]blockformat.Block
	prefetch bool
}

var _ blockstore.Blockstore = (*userView)(nil)

func (uv *userView) HashOnRead(hor bool) {
	//noop
}

func (uv *userView) Has(ctx context.Context, k cid.Cid) (bool, error) {
	var count int64
	if err := uv.cs.meta.
		Model(blockRef{}).
		Select("path, block_refs.offset").
		Where("cid = ?", models.DbCID{CID: k}).
		Joins("left join car_shards on block_refs.shard = car_shards.id").
		Where("usr = ?", uv.user).
		Count(&count).Error; err != nil {
		return false, err
	}

	return count > 0, nil
}

var CacheHits int64
var CacheMiss int64

func (uv *userView) Get(ctx context.Context, k cid.Cid) (blockformat.Block, error) {
	if !k.Defined() {
		return nil, fmt.Errorf("attempted to 'get' undefined cid")
	}
	if uv.cache != nil {
		blk, ok := uv.cache[k]
		if ok {
			atomic.AddInt64(&CacheHits, 1)

			return blk, nil
		}
	}
	atomic.AddInt64(&CacheMiss, 1)

	// TODO: for now, im using a join to ensure we only query blocks from the
	// correct user. maybe it makes sense to put the user in the blockRef
	// directly? tradeoff of time vs space
	var info struct {
		Path   string
		Offset int64
	}
	if err := uv.cs.meta.
		Model(blockRef{}).
		Select("path, block_refs.offset").
		Joins("left join car_shards on block_refs.shard = car_shards.id").
		Where("usr = ? AND cid = ?", uv.user, models.DbCID{k}).
		Find(&info).Error; err != nil {
		return nil, err
	}
	if info.Path == "" {
		return nil, ipld.ErrNotFound{k}
	}

	if uv.prefetch {
		return uv.prefetchRead(ctx, k, info.Path, info.Offset)
	} else {
		return uv.singleRead(ctx, k, info.Path, info.Offset)
	}
}

const prefetchThreshold = 512 << 10

func (uv *userView) prefetchRead(ctx context.Context, k cid.Cid, path string, offset int64) (blockformat.Block, error) {
	ctx, span := otel.Tracer("carstore").Start(ctx, "getLastShard")
	defer span.End()

	fi, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fi.Close()

	st, err := fi.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file for prefetch: %w", err)
	}

	span.SetAttributes(attribute.Int64("shard_size", st.Size()))

	if st.Size() > prefetchThreshold {
		span.SetAttributes(attribute.Bool("no_prefetch", true))
		return doBlockRead(fi, k, offset)
	}

	cr, err := car.NewCarReader(fi)
	if err != nil {
		return nil, err
	}

	for {
		blk, err := cr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		uv.cache[blk.Cid()] = blk
	}

	outblk, ok := uv.cache[k]
	if !ok {
		return nil, fmt.Errorf("requested block was not found in car slice")
	}

	return outblk, nil
}

func (uv *userView) singleRead(ctx context.Context, k cid.Cid, path string, offset int64) (blockformat.Block, error) {
	fi, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fi.Close()

	return doBlockRead(fi, k, offset)
}

func doBlockRead(fi *os.File, k cid.Cid, offset int64) (blockformat.Block, error) {
	seeked, err := fi.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	if seeked != offset {
		return nil, fmt.Errorf("failed to seek to offset (%d != %d)", seeked, offset)
	}

	bufr := bufio.NewReader(fi)
	rcid, data, err := carutil.ReadNode(bufr)
	if err != nil {
		return nil, err
	}

	if rcid != k {
		return nil, fmt.Errorf("mismatch in cid on disk: %s != %s", rcid, k)
	}

	return blocks.NewBlockWithCid(data, rcid)
}

func (uv *userView) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, fmt.Errorf("not implemented")
}

func (uv *userView) Put(ctx context.Context, blk blockformat.Block) error {
	return fmt.Errorf("puts not supported to car view blockstores")
}

func (uv *userView) PutMany(ctx context.Context, blks []blockformat.Block) error {
	return fmt.Errorf("puts not supported to car view blockstores")
}

func (uv *userView) DeleteBlock(ctx context.Context, k cid.Cid) error {
	return fmt.Errorf("deletes not supported to car view blockstore")
}

func (uv *userView) GetSize(ctx context.Context, k cid.Cid) (int, error) {
	// TODO: maybe block size should be in the database record...
	blk, err := uv.Get(ctx, k)
	if err != nil {
		return 0, err
	}

	return len(blk.RawData()), nil
}

type DeltaSession struct {
	fresh    blockstore.Blockstore
	blks     map[cid.Cid]blockformat.Block
	rmcids   map[cid.Cid]bool
	base     blockstore.Blockstore
	user     models.Uid
	baseCid  cid.Cid
	seq      int
	readonly bool
	cs       *CarStore
	lastRev  string
}

func (cs *CarStore) checkLastShardCache(user models.Uid) *CarShard {
	cs.lscLk.Lock()
	defer cs.lscLk.Unlock()

	ls, ok := cs.lastShardCache[user]
	if ok {
		return ls
	}

	return nil
}

func (cs *CarStore) removeLastShardCache(user models.Uid) {
	cs.lscLk.Lock()
	defer cs.lscLk.Unlock()

	delete(cs.lastShardCache, user)
}

func (cs *CarStore) putLastShardCache(ls *CarShard) {
	cs.lscLk.Lock()
	defer cs.lscLk.Unlock()

	cs.lastShardCache[ls.Usr] = ls
}

func (cs *CarStore) getLastShard(ctx context.Context, user models.Uid) (*CarShard, error) {
	ctx, span := otel.Tracer("carstore").Start(ctx, "getLastShard")
	defer span.End()

	maybeLs := cs.checkLastShardCache(user)
	if maybeLs != nil {
		return maybeLs, nil
	}

	var lastShard CarShard
	// this is often slow (which is why we're caching it) but could be sped up with an extra index:
	// CREATE INDEX idx_car_shards_usr_id ON car_shards (usr, seq DESC);
	if err := cs.meta.WithContext(ctx).Model(CarShard{}).Limit(1).Order("seq desc").Find(&lastShard, "usr = ?", user).Error; err != nil {
		//if err := cs.meta.Model(CarShard{}).Where("user = ?", user).Last(&lastShard).Error; err != nil {
		//if err != gorm.ErrRecordNotFound {
		return nil, err
		//}
	}

	cs.putLastShardCache(&lastShard)
	return &lastShard, nil
}

var ErrRepoBaseMismatch = fmt.Errorf("attempted a delta session on top of the wrong previous head")

func (cs *CarStore) NewDeltaSession(ctx context.Context, user models.Uid, since *string) (*DeltaSession, error) {
	ctx, span := otel.Tracer("carstore").Start(ctx, "NewSession")
	defer span.End()

	// TODO: ensure that we don't write updates on top of the wrong head
	// this needs to be a compare and swap type operation
	lastShard, err := cs.getLastShard(ctx, user)
	if err != nil {
		return nil, err
	}

	if since != nil && *since != lastShard.Rev {
		return nil, fmt.Errorf("revision mismatch: %s != %s: %w", *since, lastShard.Rev, ErrRepoBaseMismatch)
	}

	return &DeltaSession{
		fresh: blockstore.NewBlockstore(datastore.NewMapDatastore()),
		blks:  make(map[cid.Cid]blockformat.Block),
		base: &userView{
			user:     user,
			cs:       cs,
			prefetch: true,
			cache:    make(map[cid.Cid]blockformat.Block),
		},
		user:    user,
		baseCid: lastShard.Root.CID,
		cs:      cs,
		seq:     lastShard.Seq + 1,
		lastRev: lastShard.Rev,
	}, nil
}

func (cs *CarStore) ReadOnlySession(user models.Uid) (*DeltaSession, error) {
	return &DeltaSession{
		base: &userView{
			user:     user,
			cs:       cs,
			prefetch: false,
			cache:    make(map[cid.Cid]blockformat.Block),
		},
		readonly: true,
		user:     user,
		cs:       cs,
	}, nil
}

func (cs *CarStore) ReadUserCar(ctx context.Context, user models.Uid, sinceRev string, incremental bool, w io.Writer) error {
	ctx, span := otel.Tracer("carstore").Start(ctx, "ReadUserCar")
	defer span.End()

	var earlySeq int
	if sinceRev != "" {
		var untilShard CarShard
		if err := cs.meta.Where("rev >= ? AND usr = ?", sinceRev, user).Order("rev").First(&untilShard).Error; err != nil {
			return fmt.Errorf("finding early shard: %w", err)
		}
		earlySeq = untilShard.Seq
	}

	var shards []CarShard
	if err := cs.meta.Order("seq desc").Where("usr = ? AND seq >= ?", user, earlySeq).Find(&shards).Error; err != nil {
		return err
	}

	if !incremental && earlySeq > 0 {
		// have to do it the ugly way
		return fmt.Errorf("nyi")
	}

	if len(shards) == 0 {
		return fmt.Errorf("no data found for user %d", user)
	}

	// fast path!
	if err := car.WriteHeader(&car.CarHeader{
		Roots:   []cid.Cid{shards[0].Root.CID},
		Version: 1,
	}, w); err != nil {
		return err
	}

	for _, sh := range shards {
		if err := cs.writeShardBlocks(ctx, &sh, w); err != nil {
			return err
		}
	}

	return nil
}

func (cs *CarStore) writeShardBlocks(ctx context.Context, sh *CarShard, w io.Writer) error {
	ctx, span := otel.Tracer("carstore").Start(ctx, "writeShardBlocks")
	defer span.End()

	fi, err := os.Open(sh.Path)
	if err != nil {
		return err
	}
	defer fi.Close()

	_, err = fi.Seek(sh.DataStart, io.SeekStart)
	if err != nil {
		return err
	}

	_, err = io.Copy(w, fi)
	if err != nil {
		return err
	}

	return nil
}

func (cs *CarStore) writeBlockFromShard(ctx context.Context, sh *CarShard, w io.Writer, c cid.Cid) error {
	fi, err := os.Open(sh.Path)
	if err != nil {
		return err
	}
	defer fi.Close()

	rr, err := car.NewCarReader(fi)
	if err != nil {
		return err
	}

	for {
		blk, err := rr.Next()
		if err != nil {
			return err
		}

		if blk.Cid() == c {
			_, err := LdWrite(w, c.Bytes(), blk.RawData())
			return err
		}
	}
}

func (cs *CarStore) iterateShardBlocks(ctx context.Context, sh *CarShard, cb func(blk blockformat.Block) error) error {
	fi, err := os.Open(sh.Path)
	if err != nil {
		return err
	}
	defer fi.Close()

	rr, err := car.NewCarReader(fi)
	if err != nil {
		return err
	}

	for {
		blk, err := rr.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if err := cb(blk); err != nil {
			return err
		}
	}
}

var _ blockstore.Blockstore = (*DeltaSession)(nil)

func (ds *DeltaSession) BaseCid() cid.Cid {
	return ds.baseCid
}

func (ds *DeltaSession) Put(ctx context.Context, b blockformat.Block) error {
	if ds.readonly {
		return fmt.Errorf("cannot write to readonly deltaSession")
	}
	ds.blks[b.Cid()] = b
	return nil
}

func (ds *DeltaSession) PutMany(ctx context.Context, bs []blockformat.Block) error {
	if ds.readonly {
		return fmt.Errorf("cannot write to readonly deltaSession")
	}

	for _, b := range bs {
		ds.blks[b.Cid()] = b
	}
	return nil
}

func (ds *DeltaSession) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, fmt.Errorf("AllKeysChan not implemented")
}

func (ds *DeltaSession) DeleteBlock(ctx context.Context, c cid.Cid) error {
	if ds.readonly {
		return fmt.Errorf("cannot write to readonly deltaSession")
	}

	if _, ok := ds.blks[c]; !ok {
		return ipld.ErrNotFound{c}
	}

	delete(ds.blks, c)
	return nil
}

func (ds *DeltaSession) Get(ctx context.Context, c cid.Cid) (blockformat.Block, error) {
	b, ok := ds.blks[c]
	if ok {
		return b, nil
	}

	return ds.base.Get(ctx, c)
}

func (ds *DeltaSession) Has(ctx context.Context, c cid.Cid) (bool, error) {
	_, ok := ds.blks[c]
	if ok {
		return true, nil
	}

	return ds.base.Has(ctx, c)
}

func (ds *DeltaSession) HashOnRead(hor bool) {
	// noop?
}

func (ds *DeltaSession) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	b, ok := ds.blks[c]
	if ok {
		return len(b.RawData()), nil
	}

	return ds.base.GetSize(ctx, c)
}

func fnameForShard(user models.Uid, seq int) string {
	return fmt.Sprintf("sh-%d-%d", user, seq)
}
func (cs *CarStore) openNewShardFile(ctx context.Context, user models.Uid, seq int) (*os.File, string, error) {
	// TODO: some overwrite protections
	fname := filepath.Join(cs.rootDir, fnameForShard(user, seq))
	fi, err := os.Create(fname)
	if err != nil {
		return nil, "", err
	}

	return fi, fname, nil
}

func (cs *CarStore) writeNewShardFile(ctx context.Context, user models.Uid, seq int, data []byte) (string, error) {
	_, span := otel.Tracer("carstore").Start(ctx, "writeNewShardFile")
	defer span.End()

	// TODO: some overwrite protections
	fname := filepath.Join(cs.rootDir, fnameForShard(user, seq))
	if err := os.WriteFile(fname, data, 0664); err != nil {
		return "", err
	}

	return fname, nil
}

func (cs *CarStore) deleteShardFile(ctx context.Context, sh *CarShard) error {
	return os.Remove(sh.Path)
}

// CloseWithRoot writes all new blocks in a car file to the writer with the
// given cid as the 'root'
func (ds *DeltaSession) CloseWithRoot(ctx context.Context, root cid.Cid, rev string) ([]byte, error) {
	ctx, span := otel.Tracer("carstore").Start(ctx, "CloseWithRoot")
	defer span.End()

	if ds.readonly {
		return nil, fmt.Errorf("cannot write to readonly deltaSession")
	}

	return ds.cs.writeNewShard(ctx, root, rev, ds.user, ds.seq, ds.blks, ds.rmcids)
}

func WriteCarHeader(w io.Writer, root cid.Cid) (int64, error) {
	h := &car.CarHeader{
		Roots:   []cid.Cid{root},
		Version: 1,
	}
	hb, err := cbor.DumpObject(h)
	if err != nil {
		return 0, err
	}

	hnw, err := LdWrite(w, hb)
	if err != nil {
		return 0, err
	}

	return hnw, nil
}

func (cs *CarStore) writeNewShard(ctx context.Context, root cid.Cid, rev string, user models.Uid, seq int, blks map[cid.Cid]blockformat.Block, rmcids map[cid.Cid]bool) ([]byte, error) {

	buf := new(bytes.Buffer)
	hnw, err := WriteCarHeader(buf, root)
	if err != nil {
		return nil, fmt.Errorf("failed to write car header: %w", err)
	}

	// TODO: writing these blocks in map traversal order is bad, I believe the
	// optimal ordering will be something like reverse-write-order, but random
	// is definitely not it

	offset := hnw
	//brefs := make([]*blockRef, 0, len(ds.blks))
	brefs := make([]map[string]interface{}, 0, len(blks))
	for k, blk := range blks {
		nw, err := LdWrite(buf, k.Bytes(), blk.RawData())
		if err != nil {
			return nil, fmt.Errorf("failed to write block: %w", err)
		}

		/*
			brefs = append(brefs, &blockRef{
				Cid:    k.String(),
				Offset: offset,
				Shard:  shard.ID,
			})
		*/
		// adding things to the db by map is the only way to get gorm to not
		// add the 'returning' clause, which costs a lot of time
		brefs = append(brefs, map[string]interface{}{
			"cid":    models.DbCID{k},
			"offset": offset,
		})

		offset += nw
	}

	path, err := cs.writeNewShardFile(ctx, user, seq, buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to write shard file: %w", err)
	}

	shard := CarShard{
		Root:      models.DbCID{root},
		DataStart: hnw,
		Seq:       seq,
		Path:      path,
		Usr:       user,
		Rev:       rev,
	}

	if err := cs.putShard(ctx, &shard, brefs, rmcids, false); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (cs *CarStore) putShard(ctx context.Context, shard *CarShard, brefs []map[string]any, rmcids map[cid.Cid]bool, nocache bool) error {
	ctx, span := otel.Tracer("carstore").Start(ctx, "putShard")
	defer span.End()

	// YOLO: minimizing transaction size by creating only a couple small records,
	// and adding the rest of records later, outside of a transaction.
	err := cs.meta.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(ctx).Create(shard).Error; err != nil {
			return fmt.Errorf("failed to create shard in DB tx: %w", err)
		}

		if !nocache {
			cs.putLastShardCache(shard)
		}

		for _, ref := range brefs {
			ref["shard"] = shard.ID
		}

		if len(brefs) > 0 {
			// Write just one inside the transaction, so we don't have an orphan shard.
			if err := createBlockRefs(ctx, tx, brefs[:1]); err != nil {
				return fmt.Errorf("failed to create block refs: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to commit shard DB transaction: %w", err)
	}

	if len(brefs) > 0 {
		if err := createBlockRefs(ctx, cs.meta, brefs[1:]); err != nil {
			return fmt.Errorf("failed to create block refs: %w", err)
		}
	}

	if len(rmcids) > 0 {
		cids := make([]cid.Cid, 0, len(rmcids))
		for c := range rmcids {
			cids = append(cids, c)
		}

		if err := cs.meta.Create(&staleRef{
			Cids: packCids(cids),
			Usr:  shard.Usr,
		}).Error; err != nil {
			return err
		}
	}

	return nil
}

func createBlockRefs(ctx context.Context, tx *gorm.DB, brefs []map[string]any) error {
	ctx, span := otel.Tracer("carstore").Start(ctx, "createBlockRefs")
	defer span.End()

	if err := createInBatches(ctx, tx, brefs, 2000); err != nil {
		return err
	}

	return nil
}

func generateInsertQuery(data []map[string]any) (string, []any) {
	placeholders := strings.Repeat("(?, ?, ?),", len(data))
	placeholders = placeholders[:len(placeholders)-1] // trim trailing comma

	query := "INSERT INTO block_refs (\"cid\", \"offset\", \"shard\") VALUES " + placeholders

	values := make([]any, 0, 3*len(data))
	for _, entry := range data {
		values = append(values, entry["cid"], entry["offset"], entry["shard"])
	}

	return query, values
}

// Function to create in batches
func createInBatches(ctx context.Context, tx *gorm.DB, data []map[string]any, batchSize int) error {
	for i := 0; i < len(data); i += batchSize {
		batch := data[i:]
		if len(batch) > batchSize {
			batch = batch[:batchSize]
		}

		query, values := generateInsertQuery(batch)

		if err := tx.WithContext(ctx).Exec(query, values...).Error; err != nil {
			return err
		}
	}
	return nil
}

func LdWrite(w io.Writer, d ...[]byte) (int64, error) {
	var sum uint64
	for _, s := range d {
		sum += uint64(len(s))
	}

	buf := make([]byte, 8)
	n := binary.PutUvarint(buf, sum)
	nw, err := w.Write(buf[:n])
	if err != nil {
		return 0, err
	}

	for _, s := range d {
		onw, err := w.Write(s)
		if err != nil {
			return int64(nw), err
		}
		nw += onw
	}

	return int64(nw), nil
}

func setToSlice(s map[cid.Cid]bool) []cid.Cid {
	out := make([]cid.Cid, 0, len(s))
	for c := range s {
		out = append(out, c)
	}

	return out
}

func BlockDiff(ctx context.Context, bs blockstore.Blockstore, oldroot cid.Cid, newcids map[cid.Cid]blockformat.Block, skipcids map[cid.Cid]bool) (map[cid.Cid]bool, error) {
	ctx, span := otel.Tracer("repo").Start(ctx, "BlockDiff")
	defer span.End()

	if !oldroot.Defined() {
		return map[cid.Cid]bool{}, nil
	}

	// walk the entire 'new' portion of the tree, marking all referenced cids as 'keep'
	keepset := make(map[cid.Cid]bool)
	for c := range newcids {
		keepset[c] = true
		oblk, err := bs.Get(ctx, c)
		if err != nil {
			return nil, fmt.Errorf("get failed in new tree: %w", err)
		}

		if err := cbg.ScanForLinks(bytes.NewReader(oblk.RawData()), func(lnk cid.Cid) {
			keepset[lnk] = true
		}); err != nil {
			return nil, err
		}
	}

	if keepset[oldroot] {
		// this should probably never happen, but is technically correct
		return nil, nil
	}

	// next, walk the old tree from the root, recursing on cids *not* in the keepset.
	dropset := make(map[cid.Cid]bool)
	dropset[oldroot] = true
	queue := []cid.Cid{oldroot}

	for len(queue) > 0 {
		c := queue[0]
		queue = queue[1:]

		if skipcids != nil && skipcids[c] {
			continue
		}

		oblk, err := bs.Get(ctx, c)
		if err != nil {
			return nil, fmt.Errorf("get failed in old tree: %w", err)
		}

		if err := cbg.ScanForLinks(bytes.NewReader(oblk.RawData()), func(lnk cid.Cid) {
			if lnk.Prefix().Codec != cid.DagCBOR {
				return
			}

			if !keepset[lnk] {
				dropset[lnk] = true
				queue = append(queue, lnk)
			}
		}); err != nil {
			return nil, err
		}
	}

	return dropset, nil
}

func (cs *CarStore) ImportSlice(ctx context.Context, uid models.Uid, since *string, carslice []byte) (cid.Cid, *DeltaSession, error) {
	ctx, span := otel.Tracer("carstore").Start(ctx, "ImportSlice")
	defer span.End()

	carr, err := car.NewCarReader(bytes.NewReader(carslice))
	if err != nil {
		return cid.Undef, nil, err
	}

	if len(carr.Header.Roots) != 1 {
		return cid.Undef, nil, fmt.Errorf("invalid car file, header must have a single root (has %d)", len(carr.Header.Roots))
	}

	ds, err := cs.NewDeltaSession(ctx, uid, since)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("new delta session failed: %w", err)
	}

	var cids []cid.Cid
	for {
		blk, err := carr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return cid.Undef, nil, err
		}

		cids = append(cids, blk.Cid())

		if err := ds.Put(ctx, blk); err != nil {
			return cid.Undef, nil, err
		}
	}

	return carr.Header.Roots[0], ds, nil
}

func (ds *DeltaSession) CalcDiff(ctx context.Context, skipcids map[cid.Cid]bool) error {
	rmcids, err := BlockDiff(ctx, ds, ds.baseCid, ds.blks, skipcids)
	if err != nil {
		return fmt.Errorf("block diff failed (base=%s,rev=%s): %w", ds.baseCid, ds.lastRev, err)
	}

	ds.rmcids = rmcids
	return nil
}

func (cs *CarStore) GetUserRepoHead(ctx context.Context, user models.Uid) (cid.Cid, error) {
	lastShard, err := cs.getLastShard(ctx, user)
	if err != nil {
		return cid.Undef, err
	}
	if lastShard.ID == 0 {
		return cid.Undef, nil
	}

	return lastShard.Root.CID, nil
}

func (cs *CarStore) GetUserRepoRev(ctx context.Context, user models.Uid) (string, error) {
	lastShard, err := cs.getLastShard(ctx, user)
	if err != nil {
		return "", err
	}
	if lastShard.ID == 0 {
		return "", nil
	}

	return lastShard.Rev, nil
}

type UserStat struct {
	Seq     int
	Root    string
	Created time.Time
}

func (cs *CarStore) Stat(ctx context.Context, usr models.Uid) ([]UserStat, error) {
	var shards []CarShard
	if err := cs.meta.Order("seq asc").Find(&shards, "usr = ?", usr).Error; err != nil {
		return nil, err
	}

	var out []UserStat
	for _, s := range shards {
		out = append(out, UserStat{
			Seq:     s.Seq,
			Root:    s.Root.CID.String(),
			Created: s.CreatedAt,
		})
	}

	return out, nil
}

func (cs *CarStore) WipeUserData(ctx context.Context, user models.Uid) error {
	var shards []*CarShard
	if err := cs.meta.Find(&shards, "usr = ?", user).Error; err != nil {
		return err
	}

	if err := cs.deleteShards(ctx, shards); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	cs.removeLastShardCache(user)

	return nil
}

func (cs *CarStore) deleteShards(ctx context.Context, shs []*CarShard) error {
	ctx, span := otel.Tracer("carstore").Start(ctx, "deleteShards")
	defer span.End()

	deleteSlice := func(ctx context.Context, subs []*CarShard) error {
		var ids []uint
		for _, sh := range subs {
			ids = append(ids, sh.ID)
		}

		err := cs.meta.Transaction(func(txn *gorm.DB) error {
			if err := txn.Delete(&CarShard{}, "id in (?)", ids).Error; err != nil {
				return err
			}

			if err := txn.Delete(&blockRef{}, "shard in (?)", ids).Error; err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}

		var outErr error
		for _, sh := range subs {
			if err := cs.deleteShardFile(ctx, sh); err != nil {
				if !os.IsNotExist(err) {
					return err
				}
				outErr = err
			}
		}

		return outErr
	}

	chunkSize := 10000
	for i := 0; i < len(shs); i += chunkSize {
		sl := shs[i:]
		if len(sl) > chunkSize {
			sl = sl[:chunkSize]
		}

		if err := deleteSlice(ctx, sl); err != nil {
			return err
		}
	}

	return nil
}

type shardStat struct {
	ID    uint
	Dirty int
	Seq   int
	Total int

	refs []blockRef
}

func (s shardStat) dirtyFrac() float64 {
	return float64(s.Dirty) / float64(s.Total)
}

func aggrRefs(brefs []blockRef, shards map[uint]CarShard, staleCids map[cid.Cid]bool) []shardStat {
	byId := make(map[uint]*shardStat)

	for _, br := range brefs {
		s, ok := byId[br.Shard]
		if !ok {
			s = &shardStat{
				ID:  br.Shard,
				Seq: shards[br.Shard].Seq,
			}
			byId[br.Shard] = s
		}

		s.Total++
		if staleCids[br.Cid.CID] {
			s.Dirty++
		}

		s.refs = append(s.refs, br)
	}

	var out []shardStat
	for _, s := range byId {
		out = append(out, *s)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Seq < out[j].Seq
	})

	return out
}

type compBucket struct {
	shards []shardStat

	cleanBlocks int
	expSize     int
}

func (cb *compBucket) shouldCompact() bool {
	if len(cb.shards) == 0 {
		return false
	}

	if len(cb.shards) > 5 {
		return true
	}

	var frac float64
	for _, s := range cb.shards {
		frac += s.dirtyFrac()
	}
	frac /= float64(len(cb.shards))

	if len(cb.shards) > 3 && frac > 0.2 {
		return true
	}

	return frac > 0.4
}

func (cb *compBucket) addShardStat(ss shardStat) {
	cb.cleanBlocks += (ss.Total - ss.Dirty)
	cb.shards = append(cb.shards, ss)
}

func (cb *compBucket) isEmpty() bool {
	return len(cb.shards) == 0
}

func (cs *CarStore) copyShardBlocksFiltered(ctx context.Context, sh *CarShard, w io.Writer, keep map[cid.Cid]bool) error {
	fi, err := os.Open(sh.Path)
	if err != nil {
		return err
	}
	defer fi.Close()

	rr, err := car.NewCarReader(fi)
	if err != nil {
		return err
	}

	for {
		blk, err := rr.Next()
		if err != nil {
			return err
		}

		if keep[blk.Cid()] {
			_, err := LdWrite(w, blk.Cid().Bytes(), blk.RawData())
			return err
		}
	}
}

func (cs *CarStore) openNewCompactedShardFile(ctx context.Context, user models.Uid, seq int) (*os.File, string, error) {
	// TODO: some overwrite protections
	fi, err := os.CreateTemp(cs.rootDir, fnameForShard(user, seq))
	if err != nil {
		return nil, "", err
	}

	return fi, fi.Name(), nil
}

type CompactionTarget struct {
	Usr       models.Uid
	NumShards int
}

func (cs *CarStore) GetCompactionTargets(ctx context.Context, shardCount int) ([]CompactionTarget, error) {
	ctx, span := otel.Tracer("carstore").Start(ctx, "GetCompactionTargets")
	defer span.End()

	var targets []CompactionTarget
	if err := cs.meta.Raw(`select usr, count(*) as num_shards from car_shards group by usr having count(*) > ? order by num_shards desc`, shardCount).Scan(&targets).Error; err != nil {
		return nil, err
	}

	return targets, nil
}

func (cs *CarStore) getBlockRefsForShards(ctx context.Context, shardIds []uint) ([]blockRef, error) {
	ctx, span := otel.Tracer("carstore").Start(ctx, "getBlockRefsForShards")
	defer span.End()

	span.SetAttributes(attribute.Int("shards", len(shardIds)))

	chunkSize := 10000
	out := make([]blockRef, 0, len(shardIds))
	for i := 0; i < len(shardIds); i += chunkSize {
		sl := shardIds[i:]
		if len(sl) > chunkSize {
			sl = sl[:chunkSize]
		}

		var brefs []blockRef
		if err := cs.meta.Raw(`select * from block_refs where shard in (?)`, sl).Scan(&brefs).Error; err != nil {
			return nil, err
		}

		out = append(out, brefs...)
	}

	span.SetAttributes(attribute.Int("refs", len(out)))

	return out, nil
}

func shardSize(sh *CarShard) (int64, error) {
	st, err := os.Stat(sh.Path)
	if err != nil {
		return 0, fmt.Errorf("stat %q: %w", sh.Path, err)
	}

	return st.Size(), nil
}

type CompactionStats struct {
	TotalRefs     int `json:"totalRefs"`
	StartShards   int `json:"startShards"`
	NewShards     int `json:"newShards"`
	SkippedShards int `json:"skippedShards"`
	ShardsDeleted int `json:"shardsDeleted"`
	DupeCount     int `json:"dupeCount"`
}

func (cs *CarStore) CompactUserShards(ctx context.Context, user models.Uid, skipBigShards bool) (*CompactionStats, error) {
	ctx, span := otel.Tracer("carstore").Start(ctx, "CompactUserShards")
	defer span.End()

	span.SetAttributes(attribute.Int64("user", int64(user)))

	var shards []CarShard
	if err := cs.meta.WithContext(ctx).Find(&shards, "usr = ?", user).Error; err != nil {
		return nil, err
	}
	totalShards := len(shards)

	sort.Slice(shards, func(i, j int) bool {
		return shards[i].Seq < shards[j].Seq
	})

	if skipBigShards {
		// Since we generally expect shards to start bigger and get smaller,
		// and because we want to avoid compacting non-adjacent shards
		// together, and because we want to avoid running a stat on every
		// single shard (can be expensive for repos that haven't been compacted
		// in a while) we only skip a prefix of shard files that are over the
		// threshold. this may end up not skipping some shards that are over
		// the threshold if a below-threshold shard occurs before them, but
		// since this is a heuristic and imperfect optimization, that is
		// acceptable.
		var skip int
		for i, sh := range shards {
			size, err := shardSize(&sh)
			if err != nil {
				return nil, fmt.Errorf("could not check size of shard file: %w", err)
			}

			if size > BigShardThreshold {
				skip = i + 1
			} else {
				break
			}
		}
		shards = shards[skip:]
	}

	log.Infow("got the list of shards, getting the list of refs", "shardsToCompact", len(shards), "totalShards", totalShards)
	span.SetAttributes(attribute.Int("shards", len(shards)))

	var shardIds []uint
	for _, s := range shards {
		shardIds = append(shardIds, s.ID)
	}

	shardsById := make(map[uint]CarShard)
	for _, s := range shards {
		shardsById[s.ID] = s
	}

	brefs, err := cs.getBlockRefsForShards(ctx, shardIds)
	if err != nil {
		return nil, fmt.Errorf("getting block refs failed: %w", err)
	}

	span.SetAttributes(attribute.Int("blockRefs", len(brefs)))

	var staleRefs []staleRef
	if err := cs.meta.WithContext(ctx).Find(&staleRefs, "usr = ?", user).Error; err != nil {
		return nil, err
	}

	span.SetAttributes(attribute.Int("staleRefs", len(staleRefs)))

	stale := make(map[cid.Cid]bool)
	for _, br := range staleRefs {
		cids, err := br.getCids()
		if err != nil {
			return nil, fmt.Errorf("failed to unpack cids from staleRefs record (%d): %w", br.ID, err)
		}
		for _, c := range cids {
			stale[c] = true
		}
	}

	// if we have a staleRef that references multiple blockRefs, we consider that block a 'dirty duplicate'
	var dupes []cid.Cid
	var hasDirtyDupes bool
	seenBlocks := make(map[cid.Cid]bool)
	for _, br := range brefs {
		if seenBlocks[br.Cid.CID] {
			dupes = append(dupes, br.Cid.CID)
			hasDirtyDupes = true
			delete(stale, br.Cid.CID)
		} else {
			seenBlocks[br.Cid.CID] = true
		}
	}

	for _, dupe := range dupes {
		delete(stale, dupe) // remove dupes from stale list, see comment below
	}

	if hasDirtyDupes {
		// if we have no duplicates, then the keep set is simply all the 'clean' blockRefs
		// in the case we have duplicate dirty references we have to compute
		// the keep set by walking the entire repo to check if anything is
		// still referencing the dirty block in question

		// we could also just add the duplicates to the keep set for now and
		// focus on compacting everything else. it leaves *some* dirty blocks
		// still around but we're doing that anyways since compaction isn't a
		// perfect process

		log.Debugw("repo has dirty dupes", "count", len(dupes), "uid", user, "staleRefs", len(staleRefs), "blockRefs", len(brefs))

		//return nil, fmt.Errorf("WIP: not currently handling this case")
	}

	keep := make(map[cid.Cid]bool)
	for _, br := range brefs {
		if !stale[br.Cid.CID] {
			keep[br.Cid.CID] = true
		}
	}

	for _, dupe := range dupes {
		keep[dupe] = true
	}

	results := aggrRefs(brefs, shardsById, stale)
	var sum int
	for _, r := range results {
		sum += r.Total
	}

	lowBound := 20
	N := 10
	// we want to *aim* for N shards per user
	// the last several should be left small to allow easy loading from disk
	// for updates (since recent blocks are most likely needed for edits)
	// the beginning of the list should be some sort of exponential fall-off
	// with the area under the curve targeted by the total number of blocks we
	// have
	var threshs []int
	tot := len(brefs)
	for i := 0; i < N; i++ {
		v := tot / 2
		if v < lowBound {
			v = lowBound
		}
		tot = tot / 2
		threshs = append(threshs, v)
	}

	thresholdForPosition := func(i int) int {
		if i >= len(threshs) {
			return lowBound
		}
		return threshs[i]
	}

	cur := new(compBucket)
	cur.expSize = thresholdForPosition(0)
	var compactionQueue []*compBucket
	for i, r := range results {
		cur.addShardStat(r)

		if cur.cleanBlocks > cur.expSize || i > len(results)-3 {
			compactionQueue = append(compactionQueue, cur)
			cur = &compBucket{
				expSize: thresholdForPosition(len(compactionQueue)),
			}
		}
	}
	if !cur.isEmpty() {
		compactionQueue = append(compactionQueue, cur)
	}

	stats := &CompactionStats{
		StartShards: len(shards),
		TotalRefs:   len(brefs),
	}

	removedShards := make(map[uint]bool)
	for i, b := range compactionQueue {
		if !b.shouldCompact() {
			stats.SkippedShards += len(b.shards)
			continue
		}

		if err := cs.compactBucket(ctx, user, b, shardsById, keep); err != nil {
			return nil, err
		}

		stats.NewShards++

		var todelete []*CarShard
		for _, s := range b.shards {
			removedShards[s.ID] = true
			sh, ok := shardsById[s.ID]
			if !ok {
				return nil, fmt.Errorf("missing shard to delete")
			}

			todelete = append(todelete, &sh)
		}

		log.Infof("compacted bucket %d/%d, deleting %d stale shards", i+1, len(compactionQueue), len(todelete))
		stats.ShardsDeleted += len(todelete)
		if err := cs.deleteShards(ctx, todelete); err != nil {
			return nil, fmt.Errorf("deleting shards: %w", err)
		}
	}

	log.Infof("deleting stale refs")
	// now we need to delete the staleRefs we successfully cleaned up
	// we can safely delete a staleRef if all the shards that have blockRefs with matching stale refs were processed
	if err := cs.deleteStaleRefs(ctx, user, brefs, staleRefs, removedShards); err != nil {
		return nil, err
	}

	stats.DupeCount = len(dupes)

	return stats, nil
}

func (cs *CarStore) deleteStaleRefs(ctx context.Context, uid models.Uid, brefs []blockRef, staleRefs []staleRef, removedShards map[uint]bool) error {
	ctx, span := otel.Tracer("carstore").Start(ctx, "deleteStaleRefs")
	defer span.End()

	brByCid := make(map[cid.Cid][]blockRef)
	for _, br := range brefs {
		brByCid[br.Cid.CID] = append(brByCid[br.Cid.CID], br)
	}

	var staleToKeep []cid.Cid
	for _, sr := range staleRefs {
		cids, err := sr.getCids()
		if err != nil {
			return fmt.Errorf("getCids on staleRef failed (%d): %w", sr.ID, err)
		}

		for _, c := range cids {
			brs := brByCid[c]
			del := true
			for _, br := range brs {
				if !removedShards[br.Shard] {
					del = false
					break
				}
			}

			if !del {
				staleToKeep = append(staleToKeep, c)
			}
		}
	}

	err := cs.meta.Transaction(func(txn *gorm.DB) error {
		if err := txn.Delete(&staleRef{}, "usr = ?", uid).Error; err != nil {
			return err
		}

		// now create a new staleRef with all the refs we couldn't clear out
		if len(staleToKeep) > 0 {
			if err := txn.Create(&staleRef{
				Usr:  uid,
				Cids: packCids(staleToKeep),
			}).Error; err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to commit staleRef updates: %w", err)
	}

	return nil
}

func (cs *CarStore) compactBucket(ctx context.Context, user models.Uid, b *compBucket, shardsById map[uint]CarShard, keep map[cid.Cid]bool) error {
	ctx, span := otel.Tracer("carstore").Start(ctx, "compactBucket")
	defer span.End()

	span.SetAttributes(attribute.Int("shards", len(b.shards)))

	last := b.shards[len(b.shards)-1]
	lastsh := shardsById[last.ID]
	fi, path, err := cs.openNewCompactedShardFile(ctx, user, last.Seq)
	if err != nil {
		return err
	}

	defer fi.Close()
	root := lastsh.Root.CID

	hnw, err := WriteCarHeader(fi, root)
	if err != nil {
		return err
	}

	offset := hnw
	var nbrefs []map[string]any
	written := make(map[cid.Cid]bool)
	for _, s := range b.shards {
		sh := shardsById[s.ID]
		if err := cs.iterateShardBlocks(ctx, &sh, func(blk blockformat.Block) error {
			if written[blk.Cid()] {
				return nil
			}

			if keep[blk.Cid()] {
				nw, err := LdWrite(fi, blk.Cid().Bytes(), blk.RawData())
				if err != nil {
					return fmt.Errorf("failed to write block: %w", err)
				}

				nbrefs = append(nbrefs, map[string]interface{}{
					"cid":    models.DbCID{blk.Cid()},
					"offset": offset,
				})

				offset += nw
				written[blk.Cid()] = true
			}
			return nil
		}); err != nil {
			return err
		}
	}

	shard := CarShard{
		Root:      models.DbCID{root},
		DataStart: hnw,
		Seq:       lastsh.Seq,
		Path:      path,
		Usr:       user,
		Rev:       lastsh.Rev,
	}

	if err := cs.putShard(ctx, &shard, nbrefs, nil, true); err != nil {
		// if writing the shard fails, we should also delete the file
		_ = fi.Close()

		if err2 := os.Remove(fi.Name()); err2 != nil {
			log.Errorf("failed to remove shard file (%s) after failed db transaction: %w", fi.Name(), err2)
		}

		return err
	}
	return nil
}
