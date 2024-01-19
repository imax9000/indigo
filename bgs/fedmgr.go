package bgs

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/bluesky-social/indigo/models"
	"go.opentelemetry.io/otel"
	"golang.org/x/time/rate"

	"github.com/gorilla/websocket"
	pq "github.com/lib/pq"
	"gorm.io/gorm"
)

type IndexCallback func(context.Context, *models.PDS, *events.XRPCStreamEvent) error

// TODO: rename me
type Slurper struct {
	cb IndexCallback

	db *gorm.DB

	lk     sync.Mutex
	active map[string]*activeSub

	LimitMux          sync.RWMutex
	Limiters          map[uint]*rate.Limiter
	DefaultLimit      rate.Limit
	DefaultCrawlLimit rate.Limit

	newSubsDisabled bool
	trustedDomains  []string

	shutdownChan   chan bool
	shutdownResult chan []error

	ssl bool
}

type SlurperOptions struct {
	SSL                bool
	DefaultIngestLimit rate.Limit
	DefaultCrawlLimit  rate.Limit
}

func DefaultSlurperOptions() *SlurperOptions {
	return &SlurperOptions{
		SSL:                false,
		DefaultIngestLimit: rate.Limit(50),
		DefaultCrawlLimit:  rate.Limit(5),
	}
}

type activeSub struct {
	pds    *models.PDS
	lk     sync.RWMutex
	ctx    context.Context
	cancel func()
}

func NewSlurper(db *gorm.DB, cb IndexCallback, opts *SlurperOptions) (*Slurper, error) {
	if opts == nil {
		opts = DefaultSlurperOptions()
	}
	db.AutoMigrate(&SlurpConfig{})
	s := &Slurper{
		cb:                cb,
		db:                db,
		active:            make(map[string]*activeSub),
		Limiters:          make(map[uint]*rate.Limiter),
		DefaultLimit:      opts.DefaultIngestLimit,
		DefaultCrawlLimit: opts.DefaultCrawlLimit,
		ssl:               opts.SSL,
		shutdownChan:      make(chan bool),
		shutdownResult:    make(chan []error),
	}
	if err := s.loadConfig(); err != nil {
		return nil, err
	}

	// Start a goroutine to flush cursors to the DB every 30s
	go func() {
		for {
			select {
			case <-s.shutdownChan:
				log.Info("flushing PDS cursors on shutdown")
				ctx := context.Background()
				ctx, span := otel.Tracer("feedmgr").Start(ctx, "CursorFlusherShutdown")
				defer span.End()
				var errs []error
				if errs = s.flushCursors(ctx); len(errs) > 0 {
					for _, err := range errs {
						log.Errorf("failed to flush cursors on shutdown: %s", err)
					}
				}
				log.Info("done flushing PDS cursors on shutdown")
				s.shutdownResult <- errs
				return
			case <-time.After(time.Second * 10):
				log.Debug("flushing PDS cursors")
				ctx := context.Background()
				ctx, span := otel.Tracer("feedmgr").Start(ctx, "CursorFlusher")
				defer span.End()
				if errs := s.flushCursors(ctx); len(errs) > 0 {
					for _, err := range errs {
						log.Errorf("failed to flush cursors: %s", err)
					}
				}
				log.Debug("done flushing PDS cursors")
			}
		}
	}()

	return s, nil
}

func (s *Slurper) GetLimiter(pdsID uint) *rate.Limiter {
	s.LimitMux.RLock()
	defer s.LimitMux.RUnlock()
	return s.Limiters[pdsID]
}

func (s *Slurper) GetOrCreateLimiter(pdsID uint, nlimit float64) *rate.Limiter {
	s.LimitMux.RLock()
	defer s.LimitMux.RUnlock()
	lim, ok := s.Limiters[pdsID]
	if !ok {
		lim = rate.NewLimiter(rate.Limit(nlimit), 1)
		s.Limiters[pdsID] = lim
	}

	return lim
}

func (s *Slurper) SetLimiter(pdsID uint, limiter *rate.Limiter) {
	s.LimitMux.Lock()
	defer s.LimitMux.Unlock()
	s.Limiters[pdsID] = limiter
}

// Shutdown shuts down the slurper
func (s *Slurper) Shutdown() []error {
	s.shutdownChan <- true
	log.Info("waiting for slurper shutdown")
	errs := <-s.shutdownResult
	if len(errs) > 0 {
		for _, err := range errs {
			log.Errorf("shutdown error: %s", err)
		}
	}
	log.Info("slurper shutdown complete")
	return errs
}

func (s *Slurper) loadConfig() error {
	var sc SlurpConfig
	if err := s.db.Find(&sc).Error; err != nil {
		return err
	}

	if sc.ID == 0 {
		if err := s.db.Create(&SlurpConfig{}).Error; err != nil {
			return err
		}
	}

	s.newSubsDisabled = sc.NewSubsDisabled
	s.trustedDomains = sc.TrustedDomains

	return nil
}

type SlurpConfig struct {
	gorm.Model

	NewSubsDisabled bool
	TrustedDomains  pq.StringArray `gorm:"type:text[]"`
}

func (s *Slurper) SetNewSubsDisabled(dis bool) error {
	s.lk.Lock()
	defer s.lk.Unlock()

	if err := s.db.Model(SlurpConfig{}).Where("id = 1").Update("new_subs_disabled", dis).Error; err != nil {
		return err
	}

	s.newSubsDisabled = dis
	return nil
}

func (s *Slurper) GetNewSubsDisabledState() bool {
	s.lk.Lock()
	defer s.lk.Unlock()
	return s.newSubsDisabled
}

func (s *Slurper) AddTrustedDomain(domain string) error {
	s.lk.Lock()
	defer s.lk.Unlock()

	if err := s.db.Model(SlurpConfig{}).Where("id = 1").Update("trusted_domains", gorm.Expr("array_append(trusted_domains, ?)", domain)).Error; err != nil {
		return err
	}

	s.trustedDomains = append(s.trustedDomains, domain)
	return nil
}

func (s *Slurper) RemoveTrustedDomain(domain string) error {
	s.lk.Lock()
	defer s.lk.Unlock()

	if err := s.db.Model(SlurpConfig{}).Where("id = 1").Update("trusted_domains", gorm.Expr("array_remove(trusted_domains, ?)", domain)).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}

	for i, d := range s.trustedDomains {
		if d == domain {
			s.trustedDomains = append(s.trustedDomains[:i], s.trustedDomains[i+1:]...)
			break
		}
	}

	return nil
}

func (s *Slurper) SetTrustedDomains(domains []string) error {
	s.lk.Lock()
	defer s.lk.Unlock()

	if err := s.db.Model(SlurpConfig{}).Where("id = 1").Update("trusted_domains", domains).Error; err != nil {
		return err
	}

	s.trustedDomains = domains
	return nil
}

func (s *Slurper) GetTrustedDomains() []string {
	s.lk.Lock()
	defer s.lk.Unlock()
	return s.trustedDomains
}

var ErrNewSubsDisabled = fmt.Errorf("new subscriptions temporarily disabled")

// Checks whether a host is allowed to be subscribed to
// must be called with the slurper lock held
func (s *Slurper) canSlurpHost(host string) bool {
	// Check if the host is a trusted domain
	for _, d := range s.trustedDomains {
		// If the domain starts with a *., it's a wildcard
		if strings.HasPrefix(d, "*.") {
			// Cut off the * so we have .domain.com
			if strings.HasSuffix(host, strings.TrimPrefix(d, "*")) {
				return true
			}
		} else {
			if host == d {
				return true
			}
		}
	}

	return !s.newSubsDisabled
}

func (s *Slurper) SubscribeToPds(ctx context.Context, host string, reg bool) error {
	// TODO: for performance, lock on the hostname instead of global
	s.lk.Lock()
	defer s.lk.Unlock()

	if !s.canSlurpHost(host) {
		return ErrNewSubsDisabled
	}

	_, ok := s.active[host]
	if ok {
		return nil
	}

	var peering models.PDS
	if err := s.db.Find(&peering, "host = ?", host).Error; err != nil {
		return err
	}

	if peering.Blocked {
		return fmt.Errorf("cannot subscribe to blocked pds")
	}

	if peering.ID == 0 {
		// New PDS!
		npds := models.PDS{
			Host:           host,
			SSL:            s.ssl,
			Registered:     reg,
			RateLimit:      float64(s.DefaultLimit),
			CrawlRateLimit: float64(s.DefaultCrawlLimit),
		}
		if err := s.db.Create(&npds).Error; err != nil {
			if err2 := s.db.Find(&peering, "host = ?", host).Error; err2 != nil {
				return err
			}
		} else {
			peering = npds
		}
	}

	if !peering.Registered && reg {
		peering.Registered = true
		if err := s.db.Model(models.PDS{}).Where("id = ?", peering.ID).Update("registered", true).Error; err != nil {
			return err
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	sub := activeSub{
		pds:    &peering,
		ctx:    ctx,
		cancel: cancel,
	}
	s.active[host] = &sub

	s.GetOrCreateLimiter(peering.ID, peering.RateLimit)

	go s.subscribeWithRedialer(ctx, &peering, &sub)

	return nil
}

func (s *Slurper) RestartAll() error {
	s.lk.Lock()
	defer s.lk.Unlock()

	var all []models.PDS
	if err := s.db.Find(&all, "registered = true AND blocked = false").Error; err != nil {
		return err
	}

	for _, pds := range all {
		pds := pds

		ctx, cancel := context.WithCancel(context.Background())
		sub := activeSub{
			pds:    &pds,
			ctx:    ctx,
			cancel: cancel,
		}
		s.active[pds.Host] = &sub

		// Check if we've already got a limiter for this PDS
		s.GetOrCreateLimiter(pds.ID, pds.RateLimit)
		go s.subscribeWithRedialer(ctx, &pds, &sub)
	}

	return nil
}

func (s *Slurper) subscribeWithRedialer(ctx context.Context, host *models.PDS, sub *activeSub) {
	defer func() {
		s.lk.Lock()
		defer s.lk.Unlock()

		delete(s.active, host.Host)
	}()

	d := websocket.Dialer{
		HandshakeTimeout: time.Second * 5,
	}

	protocol := "ws"
	if s.ssl {
		protocol = "wss"
	}

	cursor := host.Cursor

	var backoff int
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		url := fmt.Sprintf("%s://%s/xrpc/com.atproto.sync.subscribeRepos?cursor=%d", protocol, host.Host, cursor)
		con, res, err := d.DialContext(ctx, url, nil)
		if err != nil {
			log.Warnw("dialing failed", "host", host.Host, "err", err, "backoff", backoff)
			time.Sleep(sleepForBackoff(backoff))
			backoff++

			if backoff > 15 {
				log.Warnw("pds does not appear to be online, disabling for now", "host", host.Host)
				if err := s.db.Model(&models.PDS{}).Where("id = ?", host.ID).Update("registered", false).Error; err != nil {
					log.Errorf("failed to unregister failing pds: %w", err)
				}

				return
			}

			continue
		}
		backoff = 0

		log.Info("event subscription response code: ", res.StatusCode)

		curCursor := cursor
		if err := s.handleConnection(ctx, host, con, &cursor, sub); err != nil {
			if errors.Is(err, ErrTimeoutShutdown) {
				log.Infof("shutting down pds subscription to %s, no activity after %s", host.Host, EventsTimeout)
				return
			}
			log.Warnf("connection to %q failed: %s", host.Host, err)
		}

		if cursor > curCursor {
			backoff = 0
		}
	}
}

func sleepForBackoff(b int) time.Duration {
	if b == 0 {
		return 0
	}

	if b < 10 {
		return (time.Duration(b) * 2) + (time.Millisecond * time.Duration(rand.Intn(1000)))
	}

	return time.Second * 30
}

var ErrTimeoutShutdown = fmt.Errorf("timed out waiting for new events")

var EventsTimeout = time.Minute

func (s *Slurper) handleConnection(ctx context.Context, host *models.PDS, con *websocket.Conn, lastCursor *int64, sub *activeSub) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rsc := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *comatproto.SyncSubscribeRepos_Commit) error {
			log.Debugw("got remote repo event", "host", host.Host, "repo", evt.Repo, "seq", evt.Seq)
			if err := s.cb(context.TODO(), host, &events.XRPCStreamEvent{
				RepoCommit: evt,
			}); err != nil {
				log.Errorf("failed handling event from %q (%d): %s", host.Host, evt.Seq, err)
			}
			*lastCursor = evt.Seq

			if err := s.updateCursor(sub, *lastCursor); err != nil {
				return fmt.Errorf("updating cursor: %w", err)
			}

			return nil
		},
		RepoHandle: func(evt *comatproto.SyncSubscribeRepos_Handle) error {
			log.Infow("got remote handle update event", "host", host.Host, "did", evt.Did, "handle", evt.Handle)
			if err := s.cb(context.TODO(), host, &events.XRPCStreamEvent{
				RepoHandle: evt,
			}); err != nil {
				log.Errorf("failed handling event from %q (%d): %s", host.Host, evt.Seq, err)
			}
			*lastCursor = evt.Seq

			if err := s.updateCursor(sub, *lastCursor); err != nil {
				return fmt.Errorf("updating cursor: %w", err)
			}

			return nil
		},
		RepoMigrate: func(evt *comatproto.SyncSubscribeRepos_Migrate) error {
			log.Infow("got remote repo migrate event", "host", host.Host, "did", evt.Did, "migrateTo", evt.MigrateTo)
			if err := s.cb(context.TODO(), host, &events.XRPCStreamEvent{
				RepoMigrate: evt,
			}); err != nil {
				log.Errorf("failed handling event from %q (%d): %s", host.Host, evt.Seq, err)
			}
			*lastCursor = evt.Seq

			if err := s.updateCursor(sub, *lastCursor); err != nil {
				return fmt.Errorf("updating cursor: %w", err)
			}

			return nil
		},
		RepoTombstone: func(evt *comatproto.SyncSubscribeRepos_Tombstone) error {
			log.Infow("got remote repo tombstone event", "host", host.Host, "did", evt.Did)
			if err := s.cb(context.TODO(), host, &events.XRPCStreamEvent{
				RepoTombstone: evt,
			}); err != nil {
				log.Errorf("failed handling event from %q (%d): %s", host.Host, evt.Seq, err)
			}
			*lastCursor = evt.Seq

			if err := s.updateCursor(sub, *lastCursor); err != nil {
				return fmt.Errorf("updating cursor: %w", err)
			}

			return nil
		},
		RepoInfo: func(info *comatproto.SyncSubscribeRepos_Info) error {
			log.Infow("info event", "name", info.Name, "message", info.Message, "host", host.Host)
			return nil
		},
		// TODO: all the other event types (handle change, migration, etc)
		Error: func(errf *events.ErrorFrame) error {
			switch errf.Error {
			case "FutureCursor":
				// if we get a FutureCursor frame, reset our sequence number for this host
				if err := s.db.Table("pds").Where("id = ?", host.ID).Update("cursor", 0).Error; err != nil {
					return err
				}

				*lastCursor = 0
				return fmt.Errorf("got FutureCursor frame, reset cursor tracking for host")
			default:
				return fmt.Errorf("error frame: %s: %s", errf.Error, errf.Message)
			}
		},
	}

	limiter := s.GetOrCreateLimiter(host.ID, host.RateLimit)

	instrumentedRSC := events.NewInstrumentedRepoStreamCallbacks(limiter, rsc.EventHandler)

	pool := parallel.NewScheduler(20, 10000, con.RemoteAddr().String(), instrumentedRSC.EventHandler)
	return events.HandleRepoStream(ctx, con, pool)
}

func (s *Slurper) updateCursor(sub *activeSub, curs int64) error {
	sub.lk.Lock()
	defer sub.lk.Unlock()
	sub.pds.Cursor = curs
	return nil
}

type cursorSnapshot struct {
	id     uint
	cursor int64
}

// flushCursors updates the PDS cursors in the DB for all active subscriptions
func (s *Slurper) flushCursors(ctx context.Context) []error {
	ctx, span := otel.Tracer("feedmgr").Start(ctx, "flushCursors")
	defer span.End()

	var cursors []cursorSnapshot

	s.lk.Lock()
	// Iterate over active subs and copy the current cursor
	for _, sub := range s.active {
		sub.lk.RLock()
		cursors = append(cursors, cursorSnapshot{
			id:     sub.pds.ID,
			cursor: sub.pds.Cursor,
		})
		sub.lk.RUnlock()
	}
	s.lk.Unlock()

	errs := []error{}

	tx := s.db.WithContext(ctx).Begin()
	for _, cursor := range cursors {
		if err := tx.WithContext(ctx).Model(models.PDS{}).Where("id = ?", cursor.id).UpdateColumn("cursor", cursor.cursor).Error; err != nil {
			errs = append(errs, err)
		}
	}
	if err := tx.WithContext(ctx).Commit().Error; err != nil {
		errs = append(errs, err)
	}

	return errs
}

func (s *Slurper) GetActiveList() []string {
	s.lk.Lock()
	defer s.lk.Unlock()
	var out []string
	for k := range s.active {
		out = append(out, k)
	}

	return out
}

var ErrNoActiveConnection = fmt.Errorf("no active connection to host")

func (s *Slurper) KillUpstreamConnection(host string, block bool) error {
	s.lk.Lock()
	defer s.lk.Unlock()

	ac, ok := s.active[host]
	if !ok {
		return fmt.Errorf("killing connection %q: %w", host, ErrNoActiveConnection)
	}
	ac.cancel()

	if block {
		if err := s.db.Model(models.PDS{}).Where("id = ?", ac.pds.ID).UpdateColumn("blocked", true).Error; err != nil {
			return fmt.Errorf("failed to set host as blocked: %w", err)
		}
	}

	return nil
}
