package testing

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/carstore"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	"github.com/bluesky-social/indigo/labeler"
	"github.com/bluesky-social/indigo/util"
	"github.com/bluesky-social/indigo/xrpc"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func testLabelMaker(t *testing.T) *labeler.Server {

	tempdir, err := os.MkdirTemp("", "labelmaker-test-")
	if err != nil {
		t.Fatal(err)
	}
	sharddir := filepath.Join(tempdir, "shards")
	if err := os.MkdirAll(sharddir, 0775); err != nil {
		t.Fatal(err)
	}

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{SkipDefaultTransaction: true})
	if err != nil {
		t.Fatal(err)
	}

	cs, err := carstore.NewCarStore(db, sharddir)
	if err != nil {
		t.Fatal(err)
	}

	repoKeyPath := filepath.Join(tempdir, "labelmaker.key")
	serkey, err := labeler.LoadOrCreateKeyFile(repoKeyPath, "auto-labelmaker")
	if err != nil {
		t.Fatal(err)
	}

	plcURL := "http://did-plc-test.dummy"
	blobPdsURL := "http://pds-test.dummy"
	repoUser := labeler.RepoConfig{
		Handle:     "test.handle.dummy",
		Did:        "did:plc:testdummy",
		Password:   "test-admin-pass",
		SigningKey: serkey,
		UserId:     1,
	}
	xrpcProxyURL := "http://proxy-test.dummy"
	xrpcProxyAdminPassword := "test-dummy-password"

	lm, err := labeler.NewServer(db, cs, repoUser, plcURL, blobPdsURL, xrpcProxyURL, xrpcProxyAdminPassword, false)
	if err != nil {
		t.Fatal(err)
	}
	return lm
}

func labelEvents(t *testing.T, lm *labeler.Server, since int64) *EventStream {
	d := websocket.Dialer{}
	h := http.Header{}
	bgsHost := "localhost:1234"

	q := ""
	if since >= 0 {
		q = fmt.Sprintf("?cursor=%d", since)
	}

	con, resp, err := d.Dial("ws://"+bgsHost+"/xrpc/com.atproto.sync.subscribeLabels"+q, h)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != 101 {
		t.Fatal("expected http 101 response, got: ", resp.StatusCode)
	}

	ctx, cancel := context.WithCancel(context.Background())

	es := &EventStream{
		Cancel: cancel,
	}

	go func() {
		<-ctx.Done()
		con.Close()
	}()

	go func() {
		rsc := &events.RepoStreamCallbacks{
			LabelLabels: func(evt *comatproto.LabelSubscribeLabels_Labels) error {
				fmt.Println("received event: ", evt.Seq)
				es.Lk.Lock()
				es.Events = append(es.Events, &events.XRPCStreamEvent{LabelLabels: evt})
				es.Lk.Unlock()
				return nil
			},
		}
		seqScheduler := sequential.NewScheduler("test", rsc.EventHandler)
		if err := events.HandleRepoStream(ctx, con, seqScheduler); err != nil {
			fmt.Println(err)
		}
	}()

	return es
}

/*
labelmaker interop:
- create golang PDS+BGS+labelmaker
- create user and posts
- check labelmaker state
*/

func TestLabelmakerBasic(t *testing.T) {
	assert := assert.New(t)
	_ = assert
	ctx := context.TODO()
	didr := TestPLC(t)
	p1 := MustSetupPDS(t, ".tpds", didr)
	p1.Run(t)

	b1 := MustSetupBGS(t, didr)
	b1.Run(t)

	p1.RequestScraping(t, b1)

	l1 := testLabelMaker(t)
	l1.AddKeywordLabeler(labeler.KeywordLabeler{Value: "definite-article", Keywords: []string{"the"}})
	go l1.RunAPI(":7711")
	defer l1.Shutdown(ctx)

	time.Sleep(time.Millisecond * 50)

	evts := b1.Events(t, -1)
	defer evts.Cancel()

	bob := p1.MustNewUser(t, "bob.tpds")
	alice := p1.MustNewUser(t, "alice.tpds")
	fmt.Println("bob:", bob.DID())
	fmt.Println("alice:", alice.DID())

	bp1 := bob.Post(t, "cats for cats")
	ap1 := alice.Post(t, "no i like dogs")
	_ = bp1
	_ = ap1

	xrpcc := xrpc.Client{
		Host:   "http://localhost:7711",
		Client: util.TestingHTTPClient(),
	}

	// no auth required
	queryOut, err := comatproto.LabelQueryLabels(ctx, &xrpcc, "", 20, []string{}, []string{"*"})
	assert.NoError(err)
	assert.Equal(0, len(queryOut.Labels))
	assert.Nil(queryOut.Cursor)

	// TODO: many more tests
}
