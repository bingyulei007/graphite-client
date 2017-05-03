package graphite

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

var metricTestData = []struct {
	name      string      // Metric.Name
	value     interface{} // Metric.Value
	timestamp int64       // Metric.Timestamp
	prefix    string      // Client.prefix
	plain     string      // expected plain value
}{
	// test int value types
	{"test.int", int(12), int64(1493712949), "", "test.int 12 1493712949\n"},
	{"test.int16", int16(12), int64(1493712949), "", "test.int16 12 1493712949\n"},
	{"test.int32", int32(12), int64(1493712949), "", "test.int32 12 1493712949\n"},
	{"test.int64", int64(12), int64(1493712949), "", "test.int64 12 1493712949\n"},
	{"test.uint", uint(12), int64(1493712949), "", "test.uint 12 1493712949\n"},
	// test uint value types
	{"test.uint16", uint16(12), int64(1493712949), "", "test.uint16 12 1493712949\n"},
	{"test.uint32", uint32(12), int64(1493712949), "", "test.uint32 12 1493712949\n"},
	{"test.uint64", uint64(12), int64(1493712949), "", "test.uint64 12 1493712949\n"},
	// test float types
	{"test.float32", float32(12.3), int64(1493712949), "", "test.float32 12.3 1493712949\n"},
	{"test.float64", float64(12.3), int64(1493712949), "", "test.float64 12.3 1493712949\n"},
	{"test.float64", float64(12.34567890), int64(1493712949), "", "test.float64 12.3456789 1493712949\n"},
	// test prefix
	{"test.int", int(34), int64(1493712949), "abc", "abc.test.int 34 1493712949\n"},
	{"test.int", int(34), int64(1493712949), "def.", "def.test.int 34 1493712949\n"},
	{"test.int", int(34), int64(1493712949), "ghi ", "ghi.test.int 34 1493712949\n"},
	{"test.int", int(34), int64(1493712949), ". jkl ", "jkl.test.int 34 1493712949\n"},
}

var tcpServer = NewGraphiteServer("tcp", "127.0.0.1:62003")
var udpServer = NewGraphiteServer("udp", "127.0.0.1:62003")

type GraphiteServer struct {
	network       string
	address       string
	messageBuffer chan string
	shutdown      bool

	listener *net.TCPListener
	tcpConns []*net.TCPConn
	udpConn  *net.UDPConn

	sync.Mutex
	workerStarted chan struct{}
	workerStopped chan struct{}
}

func NewGraphiteServer(network string, address string) *GraphiteServer {
	server := &GraphiteServer{
		network:       network,
		address:       address,
		messageBuffer: make(chan string, 100),
		shutdown:      false,

		workerStarted: make(chan struct{}, 1),
		workerStopped: make(chan struct{}, 1),
	}
	go server.receiveWorker()
	return server
}

func (s *GraphiteServer) GetMessage(timeout time.Duration) (string, error) {
	// if server has already shutdown, and no message in buffer, just return empty imediately
	if s.shutdown == true && len(s.messageBuffer) == 0 {
		return "", nil
	}

	select {
	case message := <-s.messageBuffer:
		return message, nil
	case <-time.After(timeout):
		return "", errors.New("timeout waiting for data")
	}
}

func (s *GraphiteServer) Shutdown() {
	// Wait for worker to be run (and listener started)
	// otherwise, listener may be started after Shutdown()
	<-s.workerStarted

	s.Lock()
	if s.network == "tcp" {
		if s.listener != nil {
			s.listener.Close()
			s.listener = nil
		}
		for _, tcpConn := range s.tcpConns {
			tcpConn.Close()
		}
	}
	if s.network == "udp" && s.udpConn != nil {
		s.udpConn.Close()
		s.udpConn = nil
	}
	s.Unlock()

	// wait for worker stop
	<-s.workerStopped
	s.shutdown = true
}

func (s *GraphiteServer) receiveWorker() {
	defer func() {
		// close workerStarted in case some error happend before listener started
		close(s.workerStarted)
		close(s.workerStopped)
	}()

	if s.network == "tcp" {
		tcpAddr, err := net.ResolveTCPAddr(s.network, s.address)
		if err != nil {
			fmt.Println("net.ResolveTCPAddr() failed with:", err)
			return
		}

		listener, err := net.ListenTCP(s.network, tcpAddr)
		if err != nil {
			fmt.Println("net.ListenTCP() failed with:", err)
			return
		}
		s.Lock()
		s.listener = listener
		s.Unlock()
		s.workerStarted <- struct{}{}

		for {
			if s.listener == nil {
				// server was shutdown
				return
			}
			tcpConn, err := s.listener.AcceptTCP()
			if err != nil {
				if !strings.Contains(err.Error(), "use of closed network connection") {
					fmt.Println("listener.AcceptTCP() failed with:", err)
				}
				break
			}

			s.Lock()
			s.tcpConns = append(s.tcpConns, tcpConn)
			s.Unlock()

			go func(conn *net.TCPConn) {
				defer conn.Close()
				for {
					buf := make([]byte, 1500)
					n, err := conn.Read(buf)
					if err != nil {
						if err != io.EOF {
							fmt.Println("conn.Read() failed with:", err)
						}
						break
					}
					messages := bytes.Split(buf[:n], []byte("\n"))
					for _, message := range messages {
						if len(message) > 0 {
							messageString := string(message)
							messageString = messageString + "\n"
							s.messageBuffer <- messageString
						}
					}
				}
			}(tcpConn)
		}
	} else {
		udpAddr, err := net.ResolveUDPAddr(s.network, s.address)
		if err != nil {
			fmt.Println("net.ResolveUDPAddr() failed with:", err)
			return
		}

		conn, err := net.ListenUDP(s.network, udpAddr)
		if err != nil {
			fmt.Println("net.ListenUDP() failed with:", err)
			return
		}

		s.Lock()
		s.udpConn = conn
		s.Unlock()
		s.workerStarted <- struct{}{}

		for {
			if s.udpConn == nil {
				// server was shutdown
				return
			}
			buf := make([]byte, 1500)
			if l, _, err := conn.ReadFromUDP(buf); err == nil {
				s.messageBuffer <- string(buf[:l])
			} else {
				if !strings.Contains(err.Error(), "use of closed network connection") {
					fmt.Println("conn.ReadFromUDP() failed with:", err)
				}
				break
			}
		}
	}
}

func TestMetricPlain(t *testing.T) {
	for _, data := range metricTestData {
		metric := &Metric{Name: data.name, Value: data.value, Timestamp: data.timestamp}
		ret := metric.Plain(cleanPrefix(data.prefix))
		if ret != data.plain {
			t.Errorf(
				"Metric:Plain() was incorrect, got: %s, want: %s",
				strings.Replace(ret, "\n", "\\n", -1),
				strings.Replace(data.plain, "\n", "\\n", -1),
			)
		}
	}
}

func TestMetricPlainB(t *testing.T) {
	for _, data := range metricTestData {
		metric := &Metric{Name: data.name, Value: data.value, Timestamp: data.timestamp}
		ret := metric.PlainB(cleanPrefix(data.prefix))
		plain := []byte(data.plain)
		if bytes.Compare(ret, plain) != 0 {
			t.Errorf(
				"Metric:PlainB() was incorrect, got: %s, want: %s",
				bytes.Replace(ret, []byte("\n"), []byte("\\n"), -1),
				bytes.Replace(plain, []byte("\n"), []byte("\\n"), -1),
			)
		}
	}
}

func TestNopClient(t *testing.T) {
	// nop client does nothing, this test is just for code coverage
	c, err := NewNopClient()
	if err != nil {
		t.Errorf("Failed to create Nop Client")
		return
	}

	// send 100000 metrics in total, ensure nop client won't block
	size := 100000
	for i := 0; i < size; i++ {
		c.SendMetric(&Metric{Name: "test.nop", Value: i, Timestamp: 0})
		c.SendSimple("test.nop", 0, 0)
	}

	metrics := make([]*Metric, size)
	for i := 0; i < size; i++ {
		metrics[i] = &Metric{Name: "test.nop", Value: i, Timestamp: 0}
	}
	c.SendMetrics(metrics)
}

func checkServerMessage(server *GraphiteServer, expected string, t *testing.T) {
	message, err := server.GetMessage(1 * time.Second)
	if err != nil {
		t.Errorf("server.GetMessage() failed with: %s", err)
	} else if message != message {
		t.Errorf(
			"server got incorrect message, got: %s, want: %s",
			strings.Replace(message, "\n", "\\n", -1),
			strings.Replace(expected, "\n", "\\n", -1),
		)
	}
}

func testClient(client *Client, server *GraphiteServer, t *testing.T) {
	var metric *Metric
	var err error

	for _, data := range metricTestData {
		if data.prefix != "" {
			continue
		}

		// test SendMetric()
		metric = &Metric{Name: data.name, Value: data.value, Timestamp: data.timestamp}
		client.SendMetric(metric)
		checkServerMessage(server, data.plain, t)

		// test SendSimple()
		client.SendSimple(data.name, data.value, data.timestamp)
		checkServerMessage(server, data.plain, t)
	}

	// after Shutdown(), should not receive anything
	client.Shutdown(1 * time.Second)
	client.SendMetric(&Metric{Name: "test.shutdown", Value: 0, Timestamp: 0})
	_, err = server.GetMessage(1 * time.Second)
	if err == nil {
		t.Errorf("Clien still sends data to server after shutdown")
	}
}

func TestTCPClient(t *testing.T) {
	client, err := NewTCPClient("127.0.0.1", 62003, "", 1*time.Second)
	if err != nil {
		t.Errorf("Failed to create TCP Client")
		return
	}

	testClient(client, tcpServer, t)
}

func TestUDPClient(t *testing.T) {
	client, err := NewUDPClient("127.0.0.1", 62003, "", 1*time.Second)
	if err != nil {
		t.Errorf("Failed to create UDP Client")
		return
	}

	testClient(client, udpServer, t)
}

func testClientWithPrefix(newClientFunc func(string) (*Client, error), server *GraphiteServer, t *testing.T) {
	var metric *Metric

	for _, data := range metricTestData {
		if data.prefix == "" {
			continue
		}

		//client, err := NewTCPClient("127.0.0.1", 62003, data.prefix, 1*time.Second)
		client, err := newClientFunc(data.prefix)
		if err != nil {
			t.Errorf("Failed to create Client")
			continue
		}

		metric = &Metric{Name: data.name, Value: data.value, Timestamp: data.timestamp}
		client.SendMetric(metric)
		checkServerMessage(server, data.plain, t)

		client.SendSimple(data.name, data.value, data.timestamp)
		checkServerMessage(server, data.plain, t)

		client.Shutdown(1 * time.Second)
	}
}

func TestTCPClientWithPrefix(t *testing.T) {
	testClientWithPrefix(
		func(prefix string) (*Client, error) {
			return NewTCPClient("127.0.0.1", 62003, prefix, 1*time.Second)
		},
		tcpServer,
		t,
	)
}

func TestUDPClientWithPrefix(t *testing.T) {
	testClientWithPrefix(
		func(prefix string) (*Client, error) {
			return NewUDPClient("127.0.0.1", 62003, prefix, 1*time.Second)
		},
		udpServer,
		t,
	)
}

func testClientBulkSend(client *Client, server *GraphiteServer, t *testing.T) {
	var metrics []*Metric
	var plains []string

	for _, data := range metricTestData {
		if data.prefix != "" {
			continue
		}
		metrics = append(metrics, &Metric{Name: data.name, Value: data.value, Timestamp: data.timestamp})
		plains = append(plains, data.plain)
	}

	client.SendMetrics(metrics)
	for _, plain := range plains {
		checkServerMessage(server, plain, t)
	}

	client.Shutdown(1 * time.Second)
}

func TestTCPClientBulkSend(t *testing.T) {
	client, err := NewTCPClient("127.0.0.1", 62003, "", 1*time.Second)
	if err != nil {
		t.Errorf("Failed to create TCP Client")
		return
	}
	testClientBulkSend(client, tcpServer, t)
}

func TestUDPClientBuldSend(t *testing.T) {
	client, err := NewUDPClient("127.0.0.1", 62003, "", 1*time.Second)
	if err != nil {
		t.Errorf("Failed to create UDP Client")
		return
	}
	testClientBulkSend(client, udpServer, t)
}

func testClientReconnect(newClient func() *Client, newServer func() *GraphiteServer, t *testing.T) {
	// procedure
	// * new server and client
	// * send some data
	// * shutdown the server, new another server
	// * send some data
	// * check if data after new server created is received

	// new server
	server := newServer()
	client := newClient()

	// send some data
	for i := 0; i < 10; i++ {
		client.SendSimple("test.reconnect", i, 1493712949)
	}

	// shutdown the server, and new server
	server.Shutdown()
	server = newServer()

	// send some other data
	var dataAfterReconnect = make(map[string]struct{})
	var foundDataAfterReconnect = 0
	for i := 10; i < 20; i++ {
		client.SendSimple("test.reconnect", i, 1493712949)
		plain := fmt.Sprintf("test.reconnect %v 1493712949\n", i)
		dataAfterReconnect[plain] = struct{}{}

		// wait for some time, make sure the lower level network stack has sent the data out,
		// so can network error (e.g. server closed) be noticed.
		// if we don't sleep here, network stack may merge subsequent data into one packet,
		// and subsequent net.Conn.Write() would success without notice that server has already closed connection.
		time.Sleep(time.Microsecond * 10)
	}

	for {
		ret, err := server.GetMessage(1 * time.Second)
		if err != nil {
			// timeout
			break
		}
		if _, ok := dataAfterReconnect[ret]; ok {
			foundDataAfterReconnect++
		}
	}
	if foundDataAfterReconnect <= 5 {
		t.Errorf("Client:reconnect() may not working properly, expected to get at least 5 messages, got %d instead.", foundDataAfterReconnect)
	}
}

func TestTCPClientReconnect(t *testing.T) {
	testClientReconnect(
		func() *Client {
			client, err := NewTCPClient("127.0.0.1", 62004, "", 1*time.Microsecond)
			if err != nil {
				fmt.Println("failed to create tcp client:", err)
			}
			return client
		},
		func() *GraphiteServer {
			return NewGraphiteServer("tcp", "127.0.0.1:62004")
		},
		t,
	)
}

func TestUDPClientReconnect(t *testing.T) {
	testClientReconnect(
		func() *Client {
			client, err := NewUDPClient("127.0.0.1", 62004, "", 1*time.Microsecond)
			if err != nil {
				fmt.Println("failed to create udp client:", err)
			}
			return client
		},
		func() *GraphiteServer {
			return NewGraphiteServer("udp", "127.0.0.1:62004")
		},
		t,
	)
}
