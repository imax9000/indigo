package engine

import (
	"context"
	"fmt"
	"testing"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/automod/countstore"

	"github.com/stretchr/testify/assert"
)

func alwaysTakedownRecordRule(c *RecordContext) error {
	c.TakedownRecord()
	return nil
}

func alwaysReportRecordRule(c *RecordContext) error {
	c.ReportRecord(ReportReasonOther, "test report")
	return nil
}

func TestTakedownCircuitBreaker(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	eng := EngineTestFixture()
	dir := identity.NewMockDirectory()
	eng.Directory = &dir
	// note that this is a record-level action, not account-level
	eng.Rules = RuleSet{
		RecordRules: []RecordRuleFunc{
			alwaysTakedownRecordRule,
		},
	}

	cid1 := syntax.CID("cid123")
	p1 := appbsky.FeedPost{Text: "some post blah"}

	// generate double the quote of events; expect to only count the quote worth of actions
	for i := 0; i < 2*QuotaModTakedownDay; i++ {
		ident := identity.Identity{
			DID:    syntax.DID(fmt.Sprintf("did:plc:abc%d", i)),
			Handle: syntax.Handle("handle.example.com"),
		}
		dir.Insert(ident)
		op := RecordOp{
			Action:     CreateOp,
			DID:        ident.DID,
			Collection: syntax.NSID("app.bsky.feed.post"),
			RecordKey:  syntax.RecordKey("abc123"),
			CID:        &cid1,
			Value:      &p1,
		}
		assert.NoError(eng.ProcessRecordOp(ctx, op))
	}

	takedowns, err := eng.GetCount("automod-quota", "takedown", countstore.PeriodDay)
	assert.NoError(err)
	assert.Equal(QuotaModTakedownDay, takedowns)

	reports, err := eng.GetCount("automod-quota", "report", countstore.PeriodDay)
	assert.NoError(err)
	assert.Equal(0, reports)
}

func TestReportCircuitBreaker(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	eng := EngineTestFixture()
	dir := identity.NewMockDirectory()
	eng.Directory = &dir
	eng.Rules = RuleSet{
		RecordRules: []RecordRuleFunc{
			alwaysReportRecordRule,
		},
	}

	cid1 := syntax.CID("cid123")
	p1 := appbsky.FeedPost{Text: "some post blah"}

	// generate double the quota of events; expect to only count the quota worth of actions
	for i := 0; i < 2*QuotaModReportDay; i++ {
		ident := identity.Identity{
			DID:    syntax.DID(fmt.Sprintf("did:plc:abc%d", i)),
			Handle: syntax.Handle("handle.example.com"),
		}
		dir.Insert(ident)
		op := RecordOp{
			Action:     CreateOp,
			DID:        ident.DID,
			Collection: syntax.NSID("app.bsky.feed.post"),
			RecordKey:  syntax.RecordKey("abc123"),
			CID:        &cid1,
			Value:      &p1,
		}
		assert.NoError(eng.ProcessRecordOp(ctx, op))
	}

	takedowns, err := eng.GetCount("automod-quota", "takedown", countstore.PeriodDay)
	assert.NoError(err)
	assert.Equal(0, takedowns)

	reports, err := eng.GetCount("automod-quota", "report", countstore.PeriodDay)
	assert.NoError(err)
	assert.Equal(QuotaModReportDay, reports)
}
