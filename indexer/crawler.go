package indexer

import (
	"context"
	"fmt"
	"sync"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/models"
	"github.com/prometheus/client_golang/prometheus"

	"go.opentelemetry.io/otel"
)

type CrawlDispatcher struct {
	ingest chan *models.ActorInfo

	repoSync chan *crawlWork

	catchup chan *crawlWork

	complete chan models.Uid

	maplk      sync.Mutex
	todo       map[models.Uid]*crawlWork
	inProgress map[models.Uid]*crawlWork

	doRepoCrawl func(context.Context, *crawlWork) error

	concurrency int
}

func NewCrawlDispatcher(repoFn func(context.Context, *crawlWork) error, concurrency int) (*CrawlDispatcher, error) {
	if concurrency < 1 {
		return nil, fmt.Errorf("must specify a non-zero positive integer for crawl dispatcher concurrency")
	}

	return &CrawlDispatcher{
		ingest:      make(chan *models.ActorInfo),
		repoSync:    make(chan *crawlWork),
		complete:    make(chan models.Uid),
		catchup:     make(chan *crawlWork),
		doRepoCrawl: repoFn,
		concurrency: concurrency,
		todo:        make(map[models.Uid]*crawlWork),
		inProgress:  make(map[models.Uid]*crawlWork),
	}, nil
}

func (c *CrawlDispatcher) Run() {
	go c.mainLoop()

	for i := 0; i < c.concurrency; i++ {
		go c.fetchWorker()
	}
}

type catchupJob struct {
	evt  *comatproto.SyncSubscribeRepos_Commit
	host *models.PDS
	user *models.ActorInfo
}

type crawlWork struct {
	act        *models.ActorInfo
	initScrape bool

	// for events that come in while this actor's crawl is enqueued
	// catchup items are processed during the crawl
	catchup []*catchupJob

	// for events that come in while this actor is being processed
	// next items are processed after the crawl
	next []*catchupJob
}

func (c *CrawlDispatcher) mainLoop() {
	var nextDispatchedJob *crawlWork
	var jobsAwaitingDispatch []*crawlWork

	// dispatchQueue represents the repoSync worker channel to which we dispatch crawl work
	var dispatchQueue chan *crawlWork

	for {
		select {
		case actorToCrawl := <-c.ingest:
			// TODO: max buffer size
			crawlJob := c.enqueueJobForActor(actorToCrawl)
			if crawlJob == nil {
				break
			}

			if nextDispatchedJob == nil {
				nextDispatchedJob = crawlJob
				dispatchQueue = c.repoSync
			} else {
				jobsAwaitingDispatch = append(jobsAwaitingDispatch, crawlJob)
			}
		case dispatchQueue <- nextDispatchedJob:
			c.dequeueJob(nextDispatchedJob)

			if len(jobsAwaitingDispatch) > 0 {
				nextDispatchedJob = jobsAwaitingDispatch[0]
				jobsAwaitingDispatch = jobsAwaitingDispatch[1:]
			} else {
				nextDispatchedJob = nil
				dispatchQueue = nil
			}
		case catchupJob := <-c.catchup:
			// CatchupJobs are for processing events that come in while a crawl is in progress
			// They are lower priority than new crawls so we only add them to the queue if there isn't already a job in progress
			if nextDispatchedJob == nil {
				nextDispatchedJob = catchupJob
				dispatchQueue = c.repoSync
			} else {
				jobsAwaitingDispatch = append(jobsAwaitingDispatch, catchupJob)
			}
		case uid := <-c.complete:
			c.maplk.Lock()

			job, ok := c.inProgress[uid]
			if !ok {
				panic("should not be possible to not have a job in progress we receive a completion signal for")
			}
			delete(c.inProgress, uid)

			// If there are any subsequent jobs for this UID, add it back to the todo list or buffer.
			// We're basically pumping the `next` queue into the `catchup` queue and will do this over and over until the `next` queue is empty.
			if len(job.next) > 0 {
				c.todo[uid] = job
				job.initScrape = false
				job.catchup = job.next
				job.next = nil
				if nextDispatchedJob == nil {
					nextDispatchedJob = job
					dispatchQueue = c.repoSync
				} else {
					jobsAwaitingDispatch = append(jobsAwaitingDispatch, job)
				}
			}
			crawlTasks.With(prometheus.Labels{"state": "inProgress"}).Set(float64(len(c.inProgress)))
			crawlTasks.With(prometheus.Labels{"state": "todo"}).Set(float64(len(c.todo)))
			c.maplk.Unlock()
		}
	}
}

// enqueueJobForActor adds a new crawl job to the todo list if there isn't already a job in progress for this actor
func (c *CrawlDispatcher) enqueueJobForActor(ai *models.ActorInfo) *crawlWork {
	c.maplk.Lock()
	defer c.maplk.Unlock()
	_, ok := c.inProgress[ai.Uid]
	if ok {
		return nil
	}

	_, has := c.todo[ai.Uid]
	if has {
		return nil
	}

	crawlJob := &crawlWork{
		act:        ai,
		initScrape: true,
	}
	c.todo[ai.Uid] = crawlJob
	return crawlJob
}

func (c *CrawlDispatcher) TODOQueueLen() int {
	c.maplk.Lock()
	defer c.maplk.Unlock()
	return len(c.todo)
}

// dequeueJob removes a job from the todo list and adds it to the inProgress list
func (c *CrawlDispatcher) dequeueJob(job *crawlWork) {
	c.maplk.Lock()
	defer c.maplk.Unlock()
	delete(c.todo, job.act.Uid)
	c.inProgress[job.act.Uid] = job
	crawlTasks.With(prometheus.Labels{"state": "inProgress"}).Set(float64(len(c.inProgress)))
	crawlTasks.With(prometheus.Labels{"state": "todo"}).Set(float64(len(c.todo)))
}

func (c *CrawlDispatcher) addToCatchupQueue(catchup *catchupJob) *crawlWork {
	catchupEventsEnqueued.Inc()
	c.maplk.Lock()
	defer c.maplk.Unlock()

	// If the actor crawl is enqueued, we can append to the catchup queue which gets emptied during the crawl
	job, ok := c.todo[catchup.user.Uid]
	if ok {
		job.catchup = append(job.catchup, catchup)
		return nil
	}

	// If the actor crawl is in progress, we can append to the nextr queue which gets emptied after the crawl
	job, ok = c.inProgress[catchup.user.Uid]
	if ok {
		job.next = append(job.next, catchup)
		return nil
	}

	// Otherwise, we need to create a new crawl job for this actor and enqueue it
	cw := &crawlWork{
		act:     catchup.user,
		catchup: []*catchupJob{catchup},
	}
	c.todo[catchup.user.Uid] = cw

	crawlTasks.With(prometheus.Labels{"state": "inProgress"}).Set(float64(len(c.inProgress)))
	crawlTasks.With(prometheus.Labels{"state": "todo"}).Set(float64(len(c.todo)))

	return cw
}

func (c *CrawlDispatcher) fetchWorker() {
	for {
		select {
		case job := <-c.repoSync:
			if err := c.doRepoCrawl(context.TODO(), job); err != nil {
				log.Errorf("failed to perform repo crawl of %q: %s", job.act.Did, err)
				crawlTasksCompleted.With(prometheus.Labels{"status": "fail"}).Inc()
			} else {
				crawlTasksCompleted.With(prometheus.Labels{"status": "success"}).Inc()
			}

			// TODO: do we still just do this if it errors?
			c.complete <- job.act.Uid
		}
	}
}

func (c *CrawlDispatcher) Crawl(ctx context.Context, ai *models.ActorInfo) error {
	if ai.PDS == 0 {
		panic("must have pds for user in queue")
	}

	ctx, span := otel.Tracer("crawler").Start(ctx, "addToCrawler")
	defer span.End()

	select {
	case c.ingest <- ai:
		userCrawlsEnqueued.Inc()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *CrawlDispatcher) AddToCatchupQueue(ctx context.Context, host *models.PDS, u *models.ActorInfo, evt *comatproto.SyncSubscribeRepos_Commit) error {
	if u.PDS == 0 {
		panic("must have pds for user in queue")
	}

	catchup := &catchupJob{
		evt:  evt,
		host: host,
		user: u,
	}

	cw := c.addToCatchupQueue(catchup)
	if cw == nil {
		return nil
	}

	select {
	case c.catchup <- cw:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *CrawlDispatcher) RepoInSlowPath(ctx context.Context, uid models.Uid) bool {
	c.maplk.Lock()
	defer c.maplk.Unlock()
	if _, ok := c.todo[uid]; ok {
		return true
	}

	if _, ok := c.inProgress[uid]; ok {
		return true
	}

	return false
}
