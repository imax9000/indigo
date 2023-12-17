package bgs

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/models"
	"github.com/bluesky-social/indigo/repo"
	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
)

func (s *BGS) recrawlScheduler(ctx context.Context) {
	var users []User
	if err := s.db.Find(&users).Error; err != nil {
		log.Errorw("failed to lookup users", "error", err)
		return
	}

	const (
		queueLimit = 20000
		batchSize  = 5000
	)
	total := len(users)

	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for range t.C {
		if s.Index.Crawler.TODOQueueLen() > queueLimit {
			continue
		}

		c := batchSize
		if len(users) < c {
			c = len(users)
		}
		batch := users[:c]
		users = users[c:]

		log.Infof("Adding %d users to crawl queue. %d/%d left for later", len(batch), len(users), total)
		for _, u := range batch {
			ai, err := s.Index.LookupUser(ctx, u.ID)
			if err != nil {
				log.Errorf("failed to look up user: %w", err)
				continue
			}
			var pds models.PDS
			if err := s.db.Find(&pds, "id = ?", u.PDS).Error; err != nil {
				log.Errorf("failed to lookup PDS: %w", err)
				continue
			}
			if err := s.Index.Crawler.AddToCatchupQueue(ctx, &pds, ai, nil); err != nil {
				log.Errorf("scheduling a crawl: %w", err)
				continue
			}
		}
	}
}

func (s *BGS) handleTest(e echo.Context) error {
	var did = "did:plc:gt4dgeuazvhn4z2hgw4ylkm6"
	ctx := e.Request().Context()

	go s.recrawlScheduler(context.Background())
	return e.String(http.StatusOK, "OK")

	var users []User
	if err := s.db.Find(&users).Error; err != nil {
		return e.String(http.StatusInternalServerError, err.Error())
	}

	for _, u := range users {
		ai, err := s.Index.LookupUser(ctx, u.ID)
		if err != nil {
			return fmt.Errorf("failed to look up user: %w", err)
		}
		var pds models.PDS
		if err := s.db.Find(&pds, "id = ?", u.PDS).Error; err != nil {
			return fmt.Errorf("failed to lookup PDS: %w", err)
		}
		if err := s.Index.Crawler.AddToCatchupQueue(ctx, &pds, ai, nil); err != nil {
			return fmt.Errorf("scheduling a crawl: %w", err)
		}
	}
	return e.String(http.StatusOK, "OK")

	if e.FormValue("did") != "" {
		did = e.FormValue("did")
	}

	u, err := s.lookupUserByDid(ctx, did)
	if err != nil {
		return fmt.Errorf("looking up user by DID: %w", err)
	}

	buf := bytes.NewBuffer(nil)
	if err := s.repoman.ReadRepo(ctx, u.ID, "", buf); err != nil {
		return fmt.Errorf("reading repo: %w", err)
	}

	r, err := repo.ReadRepoFromCar(ctx, buf)
	if err != nil {
		return fmt.Errorf("parsing repo: %w", err)
	}

	keys := map[string]map[string]bool{}
	err = r.ForEach(ctx, "", func(k string, v cid.Cid) error {
		parts := strings.SplitN(k, "/", 2)
		if len(parts) != 2 {
			return fmt.Errorf("incorrect number of parts in key %q", k)
		}
		if keys[parts[0]] == nil {
			keys[parts[0]] = map[string]bool{}
		}
		keys[parts[0]][parts[1]] = true
		return nil
	})
	if err != nil {
		return err
	}

	var existing []models.FollowRecord
	if err := s.db.Select("rkey").Find(&existing, "follower = ?", u.ID).Error; err != nil {
		return fmt.Errorf("selecting all follow records: %w", err)
	}
	extraFollows := 0
	for _, f := range existing {
		if !keys["app.bsky.graph.follow"][f.Rkey] {
			extraFollows++
		}
		delete(keys["app.bsky.graph.follow"], f.Rkey)
	}
	missingFollows := len(keys["app.bsky.graph.follow"])

	ai, err := s.Index.LookupUser(ctx, u.ID)
	if err != nil {
		return fmt.Errorf("failed to look up user: %w", err)
	}
	var pds models.PDS
	if err := s.db.Find(&pds, "id = ?", u.PDS).Error; err != nil {
		return fmt.Errorf("failed to lookup PDS: %w", err)
	}
	if err := s.Index.Crawler.AddToCatchupQueue(ctx, &pds, ai, nil); err != nil {
		return fmt.Errorf("scheduling a crawl: %w", err)
	}

	if extraFollows > 0 || missingFollows > 0 {
		return e.String(http.StatusOK, fmt.Sprintf("extra: %d, missing: %d", extraFollows, missingFollows))
	}

	return e.String(http.StatusOK, "OK")
}
