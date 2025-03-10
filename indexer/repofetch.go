package indexer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/models"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/bluesky-social/indigo/xrpc"
	ipld "github.com/ipfs/go-ipld-format"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/time/rate"
	"gorm.io/gorm"
)

func NewRepoFetcher(db *gorm.DB, rm *repomgr.RepoManager) *RepoFetcher {
	return &RepoFetcher{
		repoman:                rm,
		db:                     db,
		Limiters:               make(map[uint]*rate.Limiter),
		ApplyPDSClientSettings: func(*xrpc.Client) {},
	}
}

type RepoFetcher struct {
	repoman *repomgr.RepoManager
	db      *gorm.DB

	Limiters map[uint]*rate.Limiter
	LimitMux sync.RWMutex

	ApplyPDSClientSettings func(*xrpc.Client)
}

func (rf *RepoFetcher) GetLimiter(pdsID uint) *rate.Limiter {
	rf.LimitMux.RLock()
	defer rf.LimitMux.RUnlock()

	return rf.Limiters[pdsID]
}

func (rf *RepoFetcher) GetOrCreateLimiter(pdsID uint, pdsrate float64) *rate.Limiter {
	rf.LimitMux.RLock()
	defer rf.LimitMux.RUnlock()

	lim, ok := rf.Limiters[pdsID]
	if !ok {
		lim = rate.NewLimiter(rate.Limit(pdsrate), 1)
		rf.Limiters[pdsID] = lim
	}

	return lim
}

func (rf *RepoFetcher) SetLimiter(pdsID uint, lim *rate.Limiter) {
	rf.LimitMux.Lock()
	defer rf.LimitMux.Unlock()

	rf.Limiters[pdsID] = lim
}

func (rf *RepoFetcher) fetchRepo(ctx context.Context, c *xrpc.Client, pds *models.PDS, did string, rev string) ([]byte, error) {
	ctx, span := otel.Tracer("indexer").Start(ctx, "fetchRepo")
	defer span.End()

	span.SetAttributes(
		attribute.String("pds", pds.Host),
		attribute.String("did", did),
		attribute.String("rev", rev),
	)

	limiter := rf.GetOrCreateLimiter(pds.ID, pds.CrawlRateLimit)

	// Wait to prevent DOSing the PDS when connecting to a new stream with lots of active repos
	limiter.Wait(ctx)

	log.Debugw("SyncGetRepo", "did", did, "since", rev)
	// TODO: max size on these? A malicious PDS could just send us a petabyte sized repo here and kill us
	repo, err := atproto.SyncGetRepo(ctx, c, did, rev)
	if err != nil {
		reposFetched.WithLabelValues("fail").Inc()
		return nil, fmt.Errorf("failed to fetch repo (did=%s,rev=%s,host=%s): %w", did, rev, pds.Host, err)
	}
	reposFetched.WithLabelValues("success").Inc()

	return repo, nil
}

// TODO: since this function is the only place we depend on the repomanager, i wonder if this should be wired some other way?
func (rf *RepoFetcher) FetchAndIndexRepo(ctx context.Context, job *crawlWork) error {
	ctx, span := otel.Tracer("indexer").Start(ctx, "FetchAndIndexRepo")
	defer span.End()

	span.SetAttributes(
		attribute.Int("catchup", len(job.catchup)),
		attribute.String("did", job.act.Did),
	)

	ai := job.act

	var pds models.PDS
	if err := rf.db.First(&pds, "id = ?", ai.PDS).Error; err != nil {
		return fmt.Errorf("expected to find pds record (%d) in db for crawling one of their users: %w", ai.PDS, err)
	}

	rev, err := rf.repoman.GetRepoRev(ctx, ai.Uid)
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("failed to get repo root: %w", err)
	}

	// attempt to process buffered events
	if !job.initScrape && len(job.catchup) > 0 {
		first := job.catchup[0]
		if first.evt != nil && (first.evt.Since == nil || rev == *first.evt.Since) {
			resync := false
			for i, j := range job.catchup {
				if j.evt == nil {
					log.Errorw("job.evt == nil", "job", job)
					continue
				}

				catchupEventsProcessed.Inc()
				if err := rf.repoman.HandleExternalUserEvent(ctx, pds.ID, ai.Uid, ai.Did, j.evt.Since, j.evt.Rev, j.evt.Blocks, j.evt.Ops); err != nil {
					log.Errorw("buffered event catchup failed", "error", err, "did", ai.Did, "i", i, "jobCount", len(job.catchup), "seq", j.evt.Seq)
					resync = true // fall back to a repo sync
					break
				}
			}

			if !resync {
				return nil
			}
		}
	}

	if rev == "" {
		span.SetAttributes(attribute.Bool("full", true))
	}

	c := models.ClientForPds(&pds)
	rf.ApplyPDSClientSettings(c)

	repo, err := rf.fetchRepo(ctx, c, &pds, ai.Did, rev)
	if err != nil {
		return err
	}

	if err := rf.repoman.ImportNewRepo(ctx, ai.Uid, ai.Did, bytes.NewReader(repo), &rev); err != nil {
		span.RecordError(err)

		if ipld.IsNotFound(err) || errors.Is(err, os.ErrNotExist) {
			log.Errorw("partial repo fetch was missing data", "did", ai.Did, "pds", pds.Host, "rev", rev, "error", err)
			repo, err := rf.fetchRepo(ctx, c, &pds, ai.Did, "")
			if err != nil {
				log.Errorw("failed to fetch the full repo", "did", ai.Did, "pds", pds.Host)
				return err
			}

			if err := rf.repoman.ImportNewRepo(ctx, ai.Uid, ai.Did, bytes.NewReader(repo), nil); err != nil {
				span.RecordError(err)
				log.Errorw("full repo import failed", "error", err, "did", ai.Did, "pds", pds.Host)
				return fmt.Errorf("failed to import backup repo (%s): %w", ai.Did, err)
			}
			log.Warnw("full repo import completed successfully", "did", ai.Did, "pds", pds.Host)

			return nil
		}
		return fmt.Errorf("importing fetched repo (curRev: %s): %w", rev, err)
	}

	return nil
}
