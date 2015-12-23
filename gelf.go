// NOTE: run this in a docker container with LOGSPOUT=ignore to avoid log
// message storm.

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

var logger *log.Logger

func init() {
	hostname, _ = os.Hostname()
	logger = log.New(os.Stderr, "gelf: ", 0)
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

// streamSome receives log messages from logstream and writes them to writer.
// Returns false at the end of the stream. If there is an error writing, or a
// cancel message is received, returns true to indicate that streaming should be
// continued with a new writer.
func streamSome(writer *gelf.Writer, logstream chan *router.Message, cancel <-chan time.Time) bool {
	for {
		select {

		case msg, ok := <-logstream:
			if !ok {
				return false // end of stream
			}

			err := writer.WriteMessage(&gelf.Message{
				Version:  "1.1",
				Host:     hostname, // Running as a container cannot discover the Docker Hostname
				Short:    msg.Data,
				TimeUnix: float64(msg.Time.Unix()) + float64(msg.Time.Nanosecond())/float64(1e9),
				Level:    gelf.LOG_INFO, // TODO: be smarter about this? stdout vs. stderr are not log levels
				Extra: map[string]interface{}{
					"_container_id":   msg.Container.ID,
					"_container_name": msg.Container.Name,
					"_image_id":       msg.Container.Image,
					"_image_name":     msg.Container.Config.Image,
					"_command":        strings.Join(msg.Container.Config.Cmd, " "),
					"_created":        msg.Container.Created,
				},
			})
			if err != nil {
				logger.Printf("error sending message: %s", err)
				drop(msg)
				return true // try again with new writer
			}

		case <-cancel:
			return true // try again with new writer

		}

	}
}

func drop(msg *router.Message) {
	logger.Printf("dropping log message [%.12s %s %s]: %s",
		msg.Container.ID, msg.Container.Config.Image, msg.Container.Name, msg.Data)
}

func dropSome(logstream chan *router.Message, cancel <-chan time.Time) {
	for {
		select {
		case msg, ok := <-logstream:
			if !ok {
				return
			}
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
			logger.Printf("error dialing %s: %s", address, err)

			// Try redial after 5 seconds
			dropSome(logstream, time.After(5*time.Second))
		}

		logger.Printf("dialed %s", address)

		// intended to forward logs to a service on localhost
		writer.CompressionLevel = flate.NoCompression
		writer.CompressionType = gelf.CompressGzip

		return writer
	}
}

// Stream implements the router.LogAdapter interface. It redials the server if
// writes ever fail.
func (a *Adapter) Stream(logstream chan *router.Message) {
	f := func() bool {
		writer := newGoodWriter(a.address, logstream)
		defer writer.Close()

		// refresh writer every 5 minutes to accomodate DNS changes
		return streamSome(writer, logstream, time.After(5*time.Minute))
	}

	// keep streaming, refreshing the writer, until streamSome() returns false
	for f() {
	}
}
