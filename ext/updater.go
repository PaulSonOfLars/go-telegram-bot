package ext

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/PaulSonOfLars/gotgbot/v2"
)

type Updater struct {
	Dispatcher *Dispatcher
	UpdateChan chan json.RawMessage
	ErrorLog   *log.Logger

	idle    bool
	running bool
	server  *http.Server
}

var (
	totalUpdates = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gotgbot_updates_total",
			Help: "Number of incoming updates.",
		},
		[]string{
			"mode",
		},
	)
)

var errorLog = log.New(os.Stderr, "ERROR", log.LstdFlags)

type UpdaterOpts struct {
	ErrorLog *log.Logger

	DispatcherOpts DispatcherOpts
}

// NewUpdater Creates a new Updater, as well as the necessary structures for
func NewUpdater(opts *UpdaterOpts) Updater {
	errLog := errorLog
	var dispatcherOpts DispatcherOpts

	if opts != nil {
		if opts.ErrorLog != nil {
			errLog = opts.ErrorLog
		}

		dispatcherOpts = opts.DispatcherOpts
	}

	updateChan := make(chan json.RawMessage)
	return Updater{
		ErrorLog:   errLog,
		Dispatcher: NewDispatcher(updateChan, &dispatcherOpts),
		UpdateChan: updateChan,
	}
}

type PollingOpts struct {
	DropPendingUpdates bool
	Timeout            time.Duration
	GetUpdatesOpts     gotgbot.GetUpdatesOpts
}

// StartPolling Starts the polling logic
func (u *Updater) StartPolling(b *gotgbot.Bot, opts *PollingOpts) error {
	// TODO: De-duplicate this code.
	// This logic is currently mostly duplicated over from the generated getUpdates code.
	// This is a performance improvement to avoid:
	// - needing to re-allocate new url.values structs.
	// - needing to convert the opt values to strings to pass to the values.
	// - unnecessary unmarshalling of the (possibly multiple) full Update structs.
	// Yes, this also makes me sad. :/
	v := url.Values{}
	dropPendingUpdates := false
	pollTimeout := time.Second * 10

	if opts != nil {
		dropPendingUpdates = opts.DropPendingUpdates
		if opts.Timeout != 0 {
			pollTimeout = opts.Timeout
		}

		v.Add("offset", strconv.FormatInt(opts.GetUpdatesOpts.Offset, 10))
		v.Add("limit", strconv.FormatInt(opts.GetUpdatesOpts.Limit, 10))
		v.Add("timeout", strconv.FormatInt(opts.GetUpdatesOpts.Timeout, 10))
		if opts.GetUpdatesOpts.AllowedUpdates != nil {
			bytes, err := json.Marshal(opts.GetUpdatesOpts.AllowedUpdates)
			if err != nil {
				return fmt.Errorf("failed to marshal field allowed_updates: %w", err)
			}
			v.Add("allowed_updates", string(bytes))
		}
	}

	// Copy bot, such that we can edit values for polling
	pollingBot := *b
	pollingBot.GetTimeout = pollTimeout

	go u.Dispatcher.Start(b)
	go u.pollingLoop(pollingBot, dropPendingUpdates, v)

	return nil
}

func (u *Updater) pollingLoop(b gotgbot.Bot, dropPendingUpdates bool, v url.Values) {
	u.running = true

	// if dropPendingUpdates, force the offset to -1
	if dropPendingUpdates {
		v.Set("offset", "-1")
	}

	var offset int64
	for u.running {
		// note: this bot instance uses a custom http.Client with longer timeouts
		r, err := b.Get("getUpdates", v)
		if err != nil {
			u.ErrorLog.Println("failed to get updates; sleeping 1s: " + err.Error())
			time.Sleep(time.Second)
			continue

		} else if r == nil {
			dropPendingUpdates = false
			continue
		}

		var rawUpdates []json.RawMessage
		if err := json.Unmarshal(r, &rawUpdates); err != nil {
			u.ErrorLog.Println("failed to unmarshal updates: " + err.Error())
			continue
		}

		if len(rawUpdates) == 0 {
			dropPendingUpdates = false
			continue
		}

		var lastUpdate struct {
			UpdateId int64 `json:"update_id"`
		}

		if err := json.Unmarshal(rawUpdates[len(rawUpdates)-1], &lastUpdate); err != nil {
			u.ErrorLog.Println("failed to unmarshal last update: " + err.Error())
			continue
		}

		offset = lastUpdate.UpdateId + 1
		v.Set("offset", strconv.FormatInt(offset, 10))
		if dropPendingUpdates {
			// Setting the offset to -1 gets just the last update; this should be skipped too.
			dropPendingUpdates = false
			continue
		}

		for _, updData := range rawUpdates {
			totalUpdates.With(prometheus.Labels{"mode": "polling"}).Inc()
			temp := updData // use new mem address to avoid loop conflicts
			u.UpdateChan <- temp
		}
	}
}

// Idle starts an infinite loop to avoid the program exciting while the background threads handle updates.
func (u *Updater) Idle() {
	u.idle = true

	for u.idle {
		time.Sleep(1 * time.Second)
	}
}

// Stop stops the current updater and dispatcher instances.
func (u *Updater) Stop() error {
	// if server, this is running on webhooks; shutdown the server
	if u.server != nil {
		err := u.server.Shutdown(context.Background())
		if err != nil {
			return fmt.Errorf("failed to shutdown server: %w", err)
		}
	}

	// stop the polling loop
	u.running = false

	close(u.UpdateChan)

	u.Dispatcher.Stop()

	// stop idling
	u.idle = false
	return nil
}

// StartWebhook Starts the webhook server with the relevant settings (ie, TLS or not)
func (u *Updater) StartWebhook(b *gotgbot.Bot, opts WebhookOpts) error {
	var tls bool
	if opts.CertFile == "" && opts.KeyFile == "" {
		tls = false
	} else if opts.CertFile != "" && opts.KeyFile != "" {
		tls = true
	} else {
		return fmt.Errorf("missing certfile or keyfile")
	}

	go u.Dispatcher.Start(b)

	mux := http.NewServeMux()
	mux.HandleFunc("/"+opts.URLPath, func(w http.ResponseWriter, r *http.Request) {
		totalUpdates.With(prometheus.Labels{"mode": "webhook"}).Inc()

		bytes, _ := ioutil.ReadAll(r.Body)
		u.UpdateChan <- bytes
	})

	u.server = &http.Server{
		Addr:    opts.GetListenAddr(),
		Handler: mux,
	}

	go func() {
		var err error
		if tls {
			err = u.server.ListenAndServeTLS(opts.CertFile, opts.KeyFile)
		} else {
			err = u.server.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			panic("http server failed: " + err.Error())
		}
	}()

	return nil
}

type WebhookOpts struct {
	Listen  string
	Port    int
	URLPath string

	CertFile string
	KeyFile  string
}

// GetListenAddr returns the local listening address, including port.
func (w *WebhookOpts) GetListenAddr() string {
	if w.Listen == "" {
		w.Listen = "0.0.0.0"
	}
	if w.Port == 0 {
		w.Port = 443
	}
	return fmt.Sprintf("%s:%d", w.Listen, w.Port)
}

// GetWebhookURL returns the domain in the form domain/path.
// eg: example.com/super_secret_token
func (w *WebhookOpts) GetWebhookURL(domain string) string {
	return fmt.Sprintf("%s/%s", domain, w.URLPath)
}
