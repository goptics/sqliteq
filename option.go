package sqliteq

// Option is a function type that can be used to configure a Queue
type Option func(*Queue)

// WithRemoveOnComplete sets whether acknowledged items should be deleted
// from the database when true, or just marked as completed when false
func WithRemoveOnComplete(remove bool) Option {
	return func(q *Queue) {
		q.removeOnComplete = remove
	}
}
