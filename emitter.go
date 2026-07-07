package realtime

import (
	"sort"
	"sync"
)

// dispatcher runs user callbacks one at a time, in enqueue order, off the goroutines
// that produce events (the socket read loop, timers, API calls). This gives listeners
// the same serialized ordering the JS SDK gets from its single thread, and means a
// listener that calls a blocking SDK method (like Publish) can never deadlock the read
// loop that must deliver that method's response.
type dispatcher struct {
	mu      sync.Mutex
	queue   []func()
	running bool
}

// enqueue appends fn to the callback queue, starting a drain goroutine when none is
// running. The goroutine exits once the queue empties, so an idle client holds no
// goroutine.
func (d *dispatcher) enqueue(fn func()) {
	d.mu.Lock()
	d.queue = append(d.queue, fn)
	if !d.running {
		d.running = true
		go d.run()
	}
	d.mu.Unlock()
}

func (d *dispatcher) run() {
	for {
		d.mu.Lock()
		if len(d.queue) == 0 {
			d.running = false
			d.mu.Unlock()
			return
		}
		fn := d.queue[0]
		d.queue = d.queue[1:]
		d.mu.Unlock()
		fn()
	}
}

// registration is one registered listener, marked once when it should fire a single
// time.
type registration[T any] struct {
	fn   func(T)
	once bool
}

// emitter is a small typed event emitter used by the SDK surfaces that need both
// catch-all and event-specific listeners (connection state, channel state, messages,
// presence). Listener registration returns an unsubscribe function, and emit snapshots the
// listeners and drops one-shot ones before invoking, so a re-entrant emit from inside a
// listener cannot fire them twice.
type emitter[E comparable, T any] struct {
	mu      sync.Mutex
	nextID  int
	all     map[int]*registration[T]
	byEvent map[E]map[int]*registration[T]
	// onRemoved, when set, runs after listeners are removed (by an unsubscribe
	// function, offAll, or a one-shot firing). Presence uses it to close its server
	// watcher when the last presence listener leaves.
	onRemoved func()
}

func newEmitter[E comparable, T any]() *emitter[E, T] {
	return &emitter[E, T]{
		all:     make(map[int]*registration[T]),
		byEvent: make(map[E]map[int]*registration[T]),
	}
}

// on registers a catch-all listener and returns its unsubscribe function.
func (e *emitter[E, T]) on(fn func(T)) func() {
	e.mu.Lock()
	e.nextID++
	id := e.nextID
	e.all[id] = &registration[T]{fn: fn}
	e.mu.Unlock()
	return func() { e.remove(id, nil) }
}

// onEvent registers a listener for one event and returns its unsubscribe function. once
// makes it fire a single time.
func (e *emitter[E, T]) onEvent(event E, fn func(T), once bool) func() {
	e.mu.Lock()
	e.nextID++
	id := e.nextID
	listeners := e.byEvent[event]
	if listeners == nil {
		listeners = make(map[int]*registration[T])
		e.byEvent[event] = listeners
	}
	listeners[id] = &registration[T]{fn: fn, once: once}
	e.mu.Unlock()
	return func() { e.remove(id, &event) }
}

// offAll removes every listener.
func (e *emitter[E, T]) offAll() {
	e.mu.Lock()
	removed := len(e.all) > 0
	for _, listeners := range e.byEvent {
		removed = removed || len(listeners) > 0
	}
	e.all = make(map[int]*registration[T])
	e.byEvent = make(map[E]map[int]*registration[T])
	onRemoved := e.onRemoved
	e.mu.Unlock()
	if removed && onRemoved != nil {
		onRemoved()
	}
}

// hasAny reports whether any listener (catch-all or per-event) is still registered.
func (e *emitter[E, T]) hasAny() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.all) > 0 {
		return true
	}
	for _, listeners := range e.byEvent {
		if len(listeners) > 0 {
			return true
		}
	}
	return false
}

// emit invokes the catch-all listeners then the event's listeners, each group in
// registration order, dropping one-shot registrations before any listener runs.
func (e *emitter[E, T]) emit(event E, value T) {
	e.mu.Lock()
	removed := false
	catchAll := snapshotOrdered(e.all, &removed)
	var forEvent []func(T)
	if listeners := e.byEvent[event]; listeners != nil {
		forEvent = snapshotOrdered(listeners, &removed)
	}
	onRemoved := e.onRemoved
	e.mu.Unlock()
	for _, fn := range catchAll {
		fn(value)
	}
	for _, fn := range forEvent {
		fn(value)
	}
	if removed && onRemoved != nil {
		onRemoved()
	}
}

// snapshotOrdered copies a listener map into a slice sorted by registration id (Go maps
// iterate in random order, and listeners must fire in registration order), deleting
// one-shot registrations as it goes.
func snapshotOrdered[T any](listeners map[int]*registration[T], removed *bool) []func(T) {
	ids := make([]int, 0, len(listeners))
	for id := range listeners {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	ordered := make([]func(T), 0, len(ids))
	for _, id := range ids {
		reg := listeners[id]
		ordered = append(ordered, reg.fn)
		if reg.once {
			delete(listeners, id)
			*removed = true
		}
	}
	return ordered
}

func (e *emitter[E, T]) remove(id int, event *E) {
	e.mu.Lock()
	removed := false
	if event == nil {
		if _, ok := e.all[id]; ok {
			delete(e.all, id)
			removed = true
		}
	} else if listeners := e.byEvent[*event]; listeners != nil {
		if _, ok := listeners[id]; ok {
			delete(listeners, id)
			removed = true
		}
	}
	onRemoved := e.onRemoved
	e.mu.Unlock()
	if removed && onRemoved != nil {
		onRemoved()
	}
}
