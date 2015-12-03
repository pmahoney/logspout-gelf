package gelf

import (
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
	route *router.Route
}

// New creates a GelfAdapter with UDP as the default transport.
func New(route *router.Route) (router.LogAdapter, error) {
	// test the address, but don't use this writer
	writer, err := gelf.NewWriter(route.Address)
	if err != nil {
		return nil, err
	}
	writer.Close()

	return &Adapter{
		route: route,
	}, nil
}

func streamSome(writer *gelf.Writer, logstream chan *router.Message, cancel <-chan time.Time) {
	for {
		select {
		case msg := <-logstream:
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
				log.Printf("error sending GELF message: %s", err)
			}

		case <-cancel:
			return
		}
	}
}

func dropSome(logstream chan *router.Message, cancel <-chan time.Time) {
	for {
		select {
		case msg := <-logstream:
			log.Printf("dropping log message: %s", msg.Data)
		case <-cancel:
			return
		}
	}
}

// Stream implements the router.LogAdapter interface.
func (a *Adapter) Stream(logstream chan *router.Message) {
	for {
		writer, err := gelf.NewWriter(a.route.Address)
		if err != nil {
			log.Printf("error dialing %s: %s", a.route.Address, err)

			// Try redial after 5 seconds
			dropSome(logstream, time.After(5*time.Second))
		} else {
			log.Printf("dialed %s", a.route.Address)

			// Redial after 1 minute. Maybe DNS changed?
			streamSome(writer, logstream, time.After(1*time.Minute))
		}
	}
}
