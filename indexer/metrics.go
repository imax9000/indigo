package indexer

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var referencesCrawled = promauto.NewCounter(prometheus.CounterOpts{
	Name: "indexer_references_crawled",
	Help: "Number of references crawled",
})

var externalUserCreationAttempts = promauto.NewCounter(prometheus.CounterOpts{
	Name: "indexer_external_user_creation_attempts",
	Help: "Number of external user creation attempts",
})

var userCrawlsEnqueued = promauto.NewCounter(prometheus.CounterOpts{
	Name: "indexer_user_crawls_enqueued",
	Help: "Number of user crawls enqueued",
})

var crawlTasks = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "indexer_crawl_tasks",
	Help: "Current number of crawl tasks",
}, []string{"state"})

var crawlTasksCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "indexer_crawl_tasks_completed",
	Help: "Number of completed crawl tasks",
}, []string{"status"})

var reposFetched = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "indexer_repos_fetched",
	Help: "Number of repos fetched",
}, []string{"status"})

var catchupEventsEnqueued = promauto.NewCounter(prometheus.CounterOpts{
	Name: "indexer_catchup_events_enqueued",
	Help: "Number of catchup events enqueued",
})

var catchupEventsProcessed = promauto.NewCounter(prometheus.CounterOpts{
	Name: "indexer_catchup_events_processed",
	Help: "Number of catchup events processed",
})
