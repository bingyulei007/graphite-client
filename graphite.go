package graphite

import (
	"errors"
	"fmt"
	"net"
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
	if prefix == "" {
		return fmt.Sprintf("%s %v %v\n", m.Name, m.Value, m.Timestamp)
	}
	return fmt.Sprintf("%s.%s %v %v\n", prefix, m.Name, m.Value, m.Timestamp)
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
	return strings.Trim(prefix, ". ")
}

// NewTCPClient creates a graphite client that sends metric using plain text protocol over tcp
// if prefix is specified, all metrics' name will be prefixed before sending to graphite
func NewTCPClient(host string, port int, prefix string, reconnectDelay time.Duration) (*Client, error) {
	// TODO check if host and port is valid
	client := &Client{
		prefix:   cleanPrefix(prefix),
		network:  "tcp",
		address:  fmt.Sprintf("%s:%d", host, port),
		protocol: plain,
		delay:    reconnectDelay,

		metricPool: make(chan *Metric, 10000),
		conn:       nil,

		shutdown:      false,
		workerStopped: make(chan struct{}, 1),
	}
	go client.worker()
	return client, nil
}

// NewUDPClient creates a graphite client that sends metric using plain text protocol over udp
func NewUDPClient(host string, port int, prefix string, reconnectDelay time.Duration) (*Client, error) {
	// TODO check if host and port is valid
	client := &Client{
		prefix:   cleanPrefix(prefix),
		network:  "udp",
		address:  fmt.Sprintf("%s:%d", host, port),
		protocol: plain,
		delay:    reconnectDelay,

		metricPool: make(chan *Metric, 10000),
		conn:       nil,

		shutdown:      false,
		workerStopped: make(chan struct{}, 1),
	}
	go client.worker()
	return client, nil
}

// NewPickleClient creates a graphite client that sends metric using pickle protocol
func NewPickleClient() (*Client, error) {
	return nil, errors.New("Pickle client is not implemented")
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
