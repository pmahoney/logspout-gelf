package gelf

import (
	"compress/flate"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Graylog2/go-gelf/gelf"
	"github.com/gliderlabs/logspout/router"
)

var hostname string

func init() {
	hostname, _ = os.Hostname()
	router.AdapterFactories.Register(New, "gelf")
}

// Adapter is an adapter that streams UDP JSON to Graylog
type Adapter struct {
	address string
}

// New creates a GelfAdapter with UDP as the default transport.
func New(route *router.Route) (router.LogAdapter, error) {
	// test the address, but don't use this writer
	writer, err := gelf.NewWriter(route.Address)
	if err != nil {
		return nil, err
	}
	writer.Close()

	return &Adapter{address: route.Address}, nil
}

func streamSome(writer *gelf.Writer, logstream chan *router.Message) error {
	for msg := range logstream {
		if err := writer.WriteMessage(&gelf.Message{
			Version:  "1.1",
			Host:     hostname, // Running as a container cannot discover the Docker Hostname
			Short:    msg.Data,
			TimeUnix: float64(msg.Time.UnixNano()/int64(time.Millisecond)) / 1000.0,
			Level:    gelf.LOG_INFO, // TODO: be smarter about this? stdout vs. stderr are not log levels
			Extra: map[string]interface{}{
				"_container_id":   msg.Container.ID,
				"_container_name": msg.Container.Name,
				"_image_id":       msg.Container.Image,
				"_image_name":     msg.Container.Config.Image,
				"_command":        strings.Join(msg.Container.Config.Cmd, " "),
				"_created":        msg.Container.Created,
			},
		}); err != nil {
			log.Printf("gelf: error sending message: %s", err)
			drop(msg)
			return err
		}
	}

	return nil
}

func drop(msg *router.Message) {
	log.Printf("gelf: dropping log message: %s", msg.Data)
}

func dropSome(logstream chan *router.Message, cancel <-chan time.Time) {
	for {
		select {
		case msg := <-logstream:
			drop(msg)
		case <-cancel:
			return
		}
	}
}

// newGoodWriter makes a new gelf writer. On error dialing, the logstream is
// drained for 5 seconds, dropping logs during that period.
func newGoodWriter(address string, logstream chan *router.Message) *gelf.Writer {
	for {
		writer, err := gelf.NewWriter(address)
		if err != nil {
			log.Printf("gelf: error dialing %s: %s", address, err)

			// Try redial after 5 seconds
			dropSome(logstream, time.After(5*time.Second))
		}

		log.Printf("gelf: dialed %s", address)

		// intended to forward logs to a service on localhost
		writer.CompressionLevel = flate.NoCompression

		return writer
	}
}

// Stream implements the router.LogAdapter interface. It redials the server if
// writes ever fail.
func (a *Adapter) Stream(logstream chan *router.Message) {
	f := func() error {
		writer := newGoodWriter(a.address, logstream)
		defer writer.Close()
		return streamSome(writer, logstream)
	}

	for {
		if err := f(); err != nil {
			continue // try again with new writer
		}
		return // end of stream
	}
}
