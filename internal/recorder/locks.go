package recorder

import "sync"

// keyedLocks hands out one mutex per key; entries disappear once unused.
//
// It serialises everything that touches one object key: two sessions of the
// same recording that finish at the same time would otherwise HEAD the object,
// both decide to merge and both replace it, and the loser's conditional write
// would fail on every attempt. Lock order is always keyed lock -> Manager.mu,
// never the reverse.
type keyedLocks struct {
	mu    sync.Mutex
	locks map[string]*keyedLock
}

type keyedLock struct {
	mu   sync.Mutex
	refs int
}

// lock blocks until the key's mutex is held and returns its unlock function.
func (k *keyedLocks) lock(key string) (unlock func()) {
	k.mu.Lock()
	if k.locks == nil {
		k.locks = map[string]*keyedLock{}
	}
	l := k.locks[key]
	if l == nil {
		l = &keyedLock{}
		k.locks[key] = l
	}
	l.refs++
	k.mu.Unlock()

	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		k.mu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(k.locks, key)
		}
		k.mu.Unlock()
	}
}
