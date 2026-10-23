package sqliteq

// PriorityQueue extends Queue with priority-based dequeuing: lower numbers
// come out first, and equal priorities in insert order
type PriorityQueue struct {
	*Queue
}

// newPriorityQueue creates a new SQLite-based priority queue
func newPriorityQueue(f *file, name string, opts ...Option) (*PriorityQueue, error) {
	q, err := newQueue(f, name, true, opts...)
	if err != nil {
		return nil, err
	}

	return &PriorityQueue{Queue: q}, nil
}

// Enqueue adds an item to the queue with a specified priority
// Lower priority numbers will be dequeued first (0 is highest priority)
// Returns true if the operation was successful
func (pq *PriorityQueue) Enqueue(item any, priority int) bool {
	return pq.enqueue(item, priority, "")
}

// EnqueueOnce implements varmq.IDedupPriorityQueue: Queue.EnqueueOnce with a
// priority. A repeat leaves the stored item and its priority as they are.
func (pq *PriorityQueue) EnqueueOnce(item any, priority int, id string) bool {
	return pq.enqueue(item, priority, id)
}
