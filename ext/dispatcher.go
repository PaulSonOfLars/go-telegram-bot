package ext

import (
	"encoding/json"
	"errors"
	"log"
	"runtime/debug"
	"sort"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/PaulSonOfLars/gotgbot/v2"
)

const DefaultMaxRoutines = 50

type DispatcherAction int64

type (
	DispatcherErrorHandler func(b *gotgbot.Bot, ctx *Context, err error) DispatcherAction
	DispatcherPanicHandler func(b *gotgbot.Bot, ctx *Context, stack []byte)
)

const (
	// DispatcherActionNoop stops iteration of current group and moves to the next one.
	// This is the default action, and the same as would happen if the handler had completed successfully.
	DispatcherActionNoop DispatcherAction = iota
	// DispatcherActionContinueGroups continues iterating over current group as if the current handler did not match.
	// Functionally the same as returning ContinueGroups.
	DispatcherActionContinueGroups
	// DispatcherActionEndGroups ends all group iteration.
	// Functionally the same as returning EndGroups.
	DispatcherActionEndGroups
)

var EndGroups = errors.New("group iteration ended")
var ContinueGroups = errors.New("group iteration continued")

var (
	updateProcessingDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "gotgbot_update_processing_time_seconds",
			Help: "Time to process each update",
		},
	)
)

type Dispatcher struct {
	// Error handles any errors that occur during handler execution. The return type determines how to handle the
	// current group iteration. Default is DispatcherActionNoop; move to next group.
	Error DispatcherErrorHandler
	// Panic handles any panics that occur during handler execution.
	// If this field is nil, the stack is logged to ErrorLog.
	Panic DispatcherPanicHandler
	// ErrorLog is the output to log to when handling a library error, or recovering from a panic from user code.
	ErrorLog *log.Logger

	// handlerGroups represents the list of available handler groups, numerically sorted.
	handlerGroups []int
	// handlers represents all available handles, split into groups (see handlerGroups).
	handlers map[int][]Handler

	// updatesChan is the channel that the dispatcher receives all new updates on.
	updatesChan chan json.RawMessage
	// limiter is how we limit the maximum number of goroutines for handling updates.
	limiter chan struct{}
	// waitGroup handles the number of running operations to allow for clean shutdowns.
	waitGroup sync.WaitGroup
}

type DispatcherOpts struct {
	// Error handles any errors that occur during handler execution.
	Error DispatcherErrorHandler
	// Panic handles any panics that occur during handler execution.
	// If no panic handlers are defined, the stack is logged to ErrorLog.
	Panic DispatcherPanicHandler
	// ErrorLog is the output to log to when handling a library error, or recovering from a panic from user code.
	ErrorLog *log.Logger

	// MaxRoutines is used to decide how to limit the number of goroutines spawned by the dispatcher.
	// If MaxRoutines == 0, DefaultMaxRoutines is used instead.
	// If MaxRoutines < 0, no limits are imposed.
	// If MaxRoutines > 0, that value is used.
	MaxRoutines int
}

// NewDispatcher creates a new dispatcher, which process and handles incoming updates from the updates channel.
func NewDispatcher(updates chan json.RawMessage, opts *DispatcherOpts) *Dispatcher {
	var limiter chan struct{}

	var errFunc DispatcherErrorHandler
	var panicFunc DispatcherPanicHandler

	maxRoutines := DefaultMaxRoutines
	errLog := errorLog

	if opts != nil {
		if opts.MaxRoutines != 0 {
			maxRoutines = opts.MaxRoutines
		}

		if opts.ErrorLog != nil {
			errLog = opts.ErrorLog
		}

		errFunc = opts.Error
		panicFunc = opts.Panic
	}

	if maxRoutines >= 0 {
		if maxRoutines == 0 {
			maxRoutines = DefaultMaxRoutines
		}

		limiter = make(chan struct{}, maxRoutines)
	}

	return &Dispatcher{
		Error:       errFunc,
		Panic:       panicFunc,
		ErrorLog:    errLog,
		updatesChan: updates,
		handlers:    make(map[int][]Handler),
		limiter:     limiter,
		waitGroup:   sync.WaitGroup{},
	}
}

// Start to handle incoming updates
func (d *Dispatcher) Start(b *gotgbot.Bot) {
	if d.limiter == nil {
		d.limitlessDispatcher(b)
		return
	}

	d.limitedDispatcher(b)
}

// Stop waits for all currently processing updates to finish, and then returns.
func (d *Dispatcher) Stop() {
	d.waitGroup.Wait()
}

func (d *Dispatcher) limitedDispatcher(b *gotgbot.Bot) {
	for upd := range d.updatesChan {
		d.waitGroup.Add(1)

		// Send empty data to limiter.
		// if limiter buffer is full, it blocks until another update finishes processing.
		d.limiter <- struct{}{}
		go func(upd json.RawMessage) {
			d.ProcessRawUpdate(b, upd)

			<-d.limiter
			d.waitGroup.Done()
		}(upd)
	}
}

func (d *Dispatcher) limitlessDispatcher(b *gotgbot.Bot) {
	for upd := range d.updatesChan {
		d.waitGroup.Add(1)

		go func(upd json.RawMessage) {
			d.ProcessRawUpdate(b, upd)
			d.waitGroup.Done()
		}(upd)
	}
}

// AddHandler adds a new handler to the dispatcher. The dispatcher will call CheckUpdate() to see whether the handler
// should be executed, and then HandleUpdate() to execute it.
func (d *Dispatcher) AddHandler(handler Handler) {
	d.AddHandlerToGroup(handler, 0)
}

// AddHandlerToGroup adds a handler to a specific group; lowest number will be processed first.
func (d *Dispatcher) AddHandlerToGroup(handler Handler, group int) {
	currHandlers, ok := d.handlers[group]
	if !ok {
		d.handlerGroups = append(d.handlerGroups, group)
		sort.Ints(d.handlerGroups)
	}
	d.handlers[group] = append(currHandlers, handler)
}

func (d *Dispatcher) ProcessRawUpdate(b *gotgbot.Bot, r json.RawMessage) {
	var upd gotgbot.Update
	if err := json.Unmarshal(r, &upd); err != nil {
		d.ErrorLog.Println("failed to process raw update: " + err.Error())
		return
	}

	d.ProcessUpdate(b, &upd, nil)
}

// ProcessUpdate iterates over the list of groups to execute the matching handlers.
func (d *Dispatcher) ProcessUpdate(b *gotgbot.Bot, update *gotgbot.Update, data map[string]interface{}) {
	var ctx *Context

	timer := prometheus.NewTimer(updateProcessingDuration)
	defer timer.ObserveDuration()

	defer func() {
		if r := recover(); r != nil {
			if d.Panic != nil {
				d.Panic(b, ctx, debug.Stack())
				return
			}

			d.ErrorLog.Println(string(debug.Stack()))
		}
	}()

	for _, groupNum := range d.handlerGroups {
		for _, handler := range d.handlers[groupNum] {
			if !handler.CheckUpdate(b, update) {
				continue
			}

			if ctx == nil {
				ctx = NewContext(update, data)
			}

			err := handler.HandleUpdate(b, ctx)
			if err != nil {
				switch err {
				case ContinueGroups:
					// Continue handling current group.
					continue

				case EndGroups:
					// Stop all group handling.
					return

				default:
					action := DispatcherActionNoop
					if d.Error != nil {
						action = d.Error(b, ctx, err)
					}

					switch action {
					case DispatcherActionNoop:
						// Move on to next group; same action as if group had been successful.
					case DispatcherActionContinueGroups:
						// Continue handling current group.
						continue
					case DispatcherActionEndGroups:
						// Stop all group handling.
						return
					default:
						d.ErrorLog.Printf("unknown action %d, ending groups here", action)
						return
					}
				}

			}

			break // move to next group
		}
	}

	return
}
