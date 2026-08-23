package ibkr

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/ThomasMarcelis/ibkr-go/v2/internal/codec"
	"github.com/ThomasMarcelis/ibkr-go/v2/internal/transport"
)

type shutdownCancel struct {
	opKind  OpKind
	request codec.Message
}

type gracefulShutdown struct {
	waiters         []chan error
	transport       *transport.Conn
	remaining       []shutdownCancel
	pending         map[transport.WriteID]OpKind
	err             error
	completed       bool
	waitingWritable bool
}

// Shutdown drains the cancellation side of active subscriptions before
// terminally closing the client. Request IDs stay actor-owned: callers only
// provide the time budget and never need to enumerate subscriptions.
func (e *engine) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if cause := context.Cause(ctx); cause != nil {
		e.Close()
		return cause
	}
	response := make(chan error, 1)
	select {
	case <-e.done:
		return e.Wait()
	case <-ctx.Done():
		e.Close()
		return context.Cause(ctx)
	case e.cmds <- func() { e.startGracefulShutdown(response) }:
	}

	select {
	case err := <-response:
		return err
	case <-e.done:
		select {
		case err := <-response:
			return err
		default:
			return e.Wait()
		}
	case <-ctx.Done():
		select {
		case err := <-response:
			return err
		default:
		}
		// A graceful shutdown is best-effort only within the caller's budget.
		// Closing the transport is the final broker-side cleanup boundary when
		// a cancellation frame cannot be flushed in time.
		e.Close()
		return context.Cause(ctx)
	}
}

func (e *engine) startGracefulShutdown(waiter chan error) {
	if e.shutdown != nil {
		e.shutdown.waiters = append(e.shutdown.waiters, waiter)
		return
	}
	e.shuttingDown = true
	state := &gracefulShutdown{
		waiters:   []chan error{waiter},
		transport: e.transport,
		pending:   make(map[transport.WriteID]OpKind),
	}
	e.shutdown = state
	for write, opKind := range e.pendingSubscriptionCancels {
		if write.transport == state.transport {
			state.pending[write.id] = opKind
		}
	}

	if !e.hasReadyTransport() {
		e.finishGracefulShutdown(nil)
		return
	}
	state.remaining = e.activeSubscriptionCancellations()
	e.continueGracefulShutdown()
}

func (e *engine) activeSubscriptionCancellations() []shutdownCancel {
	result := make([]shutdownCancel, 0)
	reqIDs := make([]int, 0, len(e.keyed))
	for reqID := range e.keyed {
		reqIDs = append(reqIDs, reqID)
	}
	sort.Ints(reqIDs)
	for _, reqID := range reqIDs {
		route := e.keyed[reqID]
		if !e.shutdownCanCancel(route) {
			continue
		}
		result = append(result, shutdownCancel{opKind: route.opKind, request: route.cancelRequest})
	}

	keys := make([]string, 0, len(e.singletons))
	for key := range e.singletons {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		route := e.singletons[key]
		if !e.shutdownCanCancel(route) {
			continue
		}
		result = append(result, shutdownCancel{opKind: route.opKind, request: route.cancelRequest})
	}
	return result
}

func (e *engine) shutdownCanCancel(route *route) bool {
	return route != nil && route.subscription && route.cancelRequest != nil &&
		route.generation == e.transportGeneration && !e.routeAwaitingResume(route)
}

func (e *engine) continueGracefulShutdown() {
	state := e.shutdown
	if state == nil || state.completed {
		return
	}
	if e.transport == nil || e.transport != state.transport {
		e.finishGracefulShutdown(ErrInterrupted)
		return
	}

	for len(state.remaining) > 0 {
		cancel := state.remaining[0]
		payload, err := codec.Encode(e.serverVersion, cancel.request)
		if err != nil {
			state.err = errors.Join(state.err, fmt.Errorf("ibkr: encode shutdown cancel %s: %w", cancel.opKind, err))
			state.remaining = state.remaining[1:]
			continue
		}
		writeID, err := state.transport.SendTracked(context.Background(), payload)
		if errors.Is(err, transport.ErrSendQueueFull) {
			e.waitForShutdownWritable(state)
			return
		}
		state.remaining = state.remaining[1:]
		if err != nil {
			state.err = errors.Join(state.err, fmt.Errorf("ibkr: admit shutdown cancel %s: %w", cancel.opKind, err))
			continue
		}
		state.pending[writeID] = cancel.opKind
	}
	if len(state.pending) == 0 {
		e.finishGracefulShutdown(nil)
	}
}

func (e *engine) waitForShutdownWritable(state *gracefulShutdown) {
	if state.waitingWritable {
		return
	}
	state.waitingWritable = true
	tr := state.transport
	go func() {
		select {
		case <-tr.Writable():
			e.enqueue(func() {
				if e.shutdown != state {
					return
				}
				state.waitingWritable = false
				e.continueGracefulShutdown()
			})
		case <-tr.Done():
		case <-e.done:
		}
	}()
}

func (e *engine) handleShutdownWrite(write transportWrite) bool {
	state := e.shutdown
	if state == nil || write.transport != state.transport {
		return false
	}
	opKind, ok := state.pending[write.result.ID]
	if !ok {
		return false
	}
	delete(state.pending, write.result.ID)
	delete(e.pendingSubscriptionCancels, transportWriteKey{transport: write.transport, id: write.result.ID})
	if write.result.Outcome != transport.WriteCompleteLocal {
		cause := write.result.Err
		if cause == nil {
			cause = ErrInterrupted
		}
		state.err = errors.Join(state.err, fmt.Errorf("ibkr: write shutdown cancel %s: %w", opKind, cause))
	}
	e.continueGracefulShutdown()
	return true
}

func (e *engine) finishGracefulShutdown(err error) {
	state := e.shutdown
	if state == nil || state.completed {
		return
	}
	state.err = errors.Join(state.err, err)
	state.completed = true
	e.closeEngine(ErrClosed, nil)
}
