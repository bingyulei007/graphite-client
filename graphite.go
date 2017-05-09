package graphite

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"
)

const (
	nop = iota
	plain
	pickle
)

// Metric represents a graphite metric
// NOTE Value should be valid number types, e.g. int, int32, int64, float, etc.
//      We do not check Value type, and just format with "%v" and send to graphite server.
type Metric struct {
	Name      string
	Value     interface{}
	Timestamp int64
}

// Plain marshalls metric using plain text protocol
func (m *Metric) Plain(prefix string) string {
	return fmt.Sprintf("%s%s %v %v\n", prefix, m.Name, m.Value, m.Timestamp)
}

// PlainB marshalls metric using plain text protocol, into byte slice
func (m *Metric) PlainB(prefix string) []byte {
	return []byte(m.Plain(prefix))
}

// Pickle marshalls metric using pickle protocol
func (m *Metric) Pickle(prefix string) string {
	// TODO implement this
	return ""
}

// PickleB marshalls metric using pickle protocol, into byte slice
func (m *Metric) PickleB(prefix string) []byte {
	return []byte(m.PickleB(prefix))
}

// Client represents a graphite client
type Client struct {
	prefix   string // prefix metrics' name before send to server
	network  string // ip networks, should be "tcp", "udp"
	address  string // graphite server address, in format "$host:$port"
	protocol int    // plain or pickle
	delay    time.Duration

	metricPool chan *Metric
	conn       net.Conn

	shutdown      bool
	workerStopped chan struct{}
	stopSendChans []chan struct{}
}

// SendMetric sends a metric to the server.
// SendMetric does not block the caller, it just enqueues the metric to a send buffer and return,
// metric is not guaranteed to be sent.
func (c *Client) SendMetric(metric *Metric) {
	if c.protocol == nop || c.shutdown == true {
		return
	}
	defer func() {
		// edge case when panic would happen:
		//   1. user called SendMetric(), the above c.shutdown check has passed, and this routine paused
		//   2. user called Shutdown(), metricPool closed
		//   3. this routine regain control, and tries to send the metric to metricPool, panic happens
		recover()
	}()
	c.metricPool <- metric
}

// SendMetrics sends a bunch of metrics to the server.
// SendMetrics does not block the caller, it just enqueues the metrics to a send buffer and return,
// metrics are not guaranteed to be sent.
func (c *Client) SendMetrics(metrics []*Metric) {
	for _, metric := range metrics {
		c.SendMetric(metric)
	}
}

// SendSimple does not require the caller to provide a Metric object.
// SendSimple does not block the caller, it just enqueues the metric to a send buffer and return,
// metric is not guaranteed to be sent.
// if timestamp is 0, time.Now() is used
func (c *Client) SendSimple(name string, value interface{}, timestamp int64) {
	if timestamp == 0 {
		timestamp = time.Now().Unix()
	}
	c.SendMetric(&Metric{
		Name:      name,
		Value:     value,
		Timestamp: timestamp,
	})
}

// SendChan receives metrics from chan and send continually.
// SendChan will block the caller until the chan is closed.
func (c *Client) SendChan(ch chan *Metric) {
	for {
		if metric, ok := <-ch; ok {
			c.SendMetric(metric)
		} else {
			break
		}
	}
}

// Shutdown closes the client.
// After shutdown, the client won't accept any metrics to send,
// Shutdown will block the caller until all metrics in buffer have been sent, or timeout occurs.
func (c *Client) Shutdown(timeout time.Duration) {
	// set the shutdown mark
	c.shutdown = true
	close(c.metricPool)

	// wait worker routine to exit or timeout
	select {
	case <-c.workerStopped:
		// shutdown normally
		return
	case <-time.After(timeout):
		// timeout happens, forcely close connection and return
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
		return
	}
}

// cleanPrefix cleans up caller passed prefix, removes leading and trailing white spaces and dots
func cleanPrefix(prefix string) string {
	// TODO check for invalid characters
	return strings.Trim(prefix, " ")
}

func findStringSubmatchMap(r *regexp.Regexp, s string) map[string]string {
	captures := make(map[string]string)

	match := r.FindStringSubmatch(s)
	if match == nil {
		return captures
	}

	for i, name := range r.SubexpNames() {
		if i == 0 || name == "" {
			continue
		}

		captures[name] = match[i]
	}
	return captures
}

// NewClient creates a graphite client. If prefix is specified, all metrics' name will be
// prefixed before sending to graphite.
func NewClient(url string, prefix string, reconnectDelay time.Duration) (*Client, error) {
	// parse url, formats: ${protocol}://${host}:${port}
	// ${protocol} is optional, valid values: tcp, udp, pickle (via tcp), default to tcp
	// ${host} is mandatary
	// ${port} is optional, default to 2003
	captures := findStringSubmatchMap(
		regexp.MustCompile(`^((?P<n>(tcp|udp|pickle))://)?(?P<h>[^:]+)(:(?P<p>\d+))?$`),
		url,
	)
	var net, host, port = captures["n"], captures["h"], captures["p"]

	if host == "" {
		return nil, errors.New("Invalid graphite url: " + url)
	}

	if port == "" {
		port = "2003"
	}

	var network string
	var protocol int
	switch net {
	case "tcp":
		network, protocol = "tcp", plain
	case "udp":
		network, protocol = "udp", plain
	case "pickle":
		network, protocol = "tcp", pickle
	}

	client := &Client{
		prefix:   cleanPrefix(prefix),
		network:  network,
		address:  fmt.Sprintf("%s:%s", host, port),
		protocol: protocol,
		delay:    reconnectDelay,

		metricPool: make(chan *Metric, 10000),
		conn:       nil,

		shutdown:      false,
		workerStopped: make(chan struct{}, 1),
	}
	go client.worker()
	return client, nil
}

// NewNopClient creates a graphite client that does nothing
func NewNopClient() (*Client, error) {
	return &Client{
		protocol: nop,
		shutdown: false,
	}, nil
}

// reconnect establish connection to server, blocks the caller until success or client shutdown
func (c *Client) reconnect() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	for {
		// if client was shutdown, and connection is not established, we just give up
		if c.shutdown {
			return
		}

		var err error
		if c.conn, err = net.Dial(c.network, c.address); err == nil {
			return
		}

		// connection failed, wait a minute and retry
		time.Sleep(c.delay)
	}
}

// worker pulls data from metricPool, and send them to graphite server
func (c *Client) worker() {
	defer func() {
		close(c.workerStopped)
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
	}()
	for {
		if m, ok := <-c.metricPool; ok {
			if c.conn == nil {
				// no connection established, or last write failed
				c.reconnect()
			}

			if c.conn == nil {
				// reconnect() should block until successfully connected,
				// unless the client is shutdown, in which case we should give up
				return
			}

			switch c.protocol {
			case plain:
				if _, err := c.conn.Write(m.PlainB(c.prefix)); err != nil {
					// if write fails, don't resend this metric
					// we do not guarantee all metrics are sent
					c.conn.Close()
					c.conn = nil
				}
			case pickle:
				if _, err := c.conn.Write(m.PickleB(c.prefix)); err != nil {
					c.conn.Close()
					c.conn = nil
				}
			default:
				// wrong protocol
			}
		} else {
			// !ok means metricPool was closed, so was client shutdown
			return
		}
	}
}
