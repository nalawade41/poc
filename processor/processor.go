package processor

import (
	"github.com/nalawade41/poc/types"
	"github.com/vartanbeno/go-reddit/v2/reddit"
)

type ProcessorFactory struct {
	redditClient      *reddit.Client
	jobQueue          chan types.Job
	operator          *Operator
	postProcessor     *PostProcessor
	commentsProcessor *CommentsProcessor
}

func NewProcessorFactory(client *reddit.Client, jobQueue chan types.Job, operator *Operator) *ProcessorFactory {
	return &ProcessorFactory{
		redditClient:      client,
		jobQueue:          jobQueue,
		operator:          operator,
		postProcessor:     NewPostProcessor(client, jobQueue, operator),
		commentsProcessor: NewCommentsProcessor(client, operator),
	}
}

// GetPostProcessor returns a configured PostProcessor instance
func (pf *ProcessorFactory) GetPostProcessor() *PostProcessor {
	return &PostProcessor{
		RedditClient: pf.redditClient,
		JobQueue:     pf.jobQueue,
		Operator:     pf.operator,
	}
}

// GetCommentsProcessor returns a configured CommentsProcessor instance
func (pf *ProcessorFactory) GetCommentsProcessor() *CommentsProcessor {
	return &CommentsProcessor{
		RedditClient: pf.redditClient,
		Operator:     pf.operator,
	}
}
