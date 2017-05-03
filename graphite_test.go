package graphite

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
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
var udpServer = NewGraphiteServer("udp", "127.0.0.1:62004")

type GraphiteServer struct {
	network       string
	address       string
	messageBuffer chan string
	listener      *net.TCPListener
	conn          *net.UDPConn
}

func NewGraphiteServer(network string, address string) *GraphiteServer {
	server := &GraphiteServer{
		network:       network,
		address:       address,
		messageBuffer: make(chan string, 100),
	}
	go server.receiveWorker()
	return server
}

func (s *GraphiteServer) GetMessage(timeout time.Duration) (string, error) {
	select {
	case message := <-s.messageBuffer:
		return message, nil
	case <-time.After(timeout):
		return "", errors.New("timeout waiting for data")
	}
}

func (s *GraphiteServer) Shutdown() {
	if s.network == "tcp" && s.listener != nil {
		s.listener.Close()
	}
	if s.network == "udp" && s.conn != nil {
		s.conn.Close()
	}
}

func (s *GraphiteServer) receiveWorker() {
	if s.network == "tcp" {
		tcpAddr, err := net.ResolveTCPAddr(s.network, s.address)
		if err != nil {
			fmt.Println("net.ResolveTCPAddr(() failed with:", err)
			return
		}

		listener, err := net.ListenTCP(s.network, tcpAddr)
		if err != nil {
			fmt.Println("net.ListenTCP() failed with:", err)
			return
		}
		s.listener = listener

		defer listener.Close()

		for {
			tcpConn, err := listener.AcceptTCP()
			if err != nil {
				fmt.Println("listener.AcceptTCP() failed with:", err)
				break
			}
			go func(conn *net.TCPConn) {
				defer conn.Close()
				for {
					buf := make([]byte, 1500)
					n, err := conn.Read(buf)
					if err != nil {
						if err == io.EOF {
							break
						}
						fmt.Println("conn.Read() failed with:", err)
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
		// udp
	}
}

func TestMetricPlain(t *testing.T) {
	for _, data := range metricTestData {
		metric := &Metric{Name: data.name, Value: data.value, Timestamp: data.timestamp}
		plainRet := metric.Plain(cleanPrefix(data.prefix))
		if plainRet != data.plain {
			t.Errorf(
				"Metric:Plain() was incorrect, got: %s, want: %s",
				strings.Replace(plainRet, "\n", "\\n", -1),
				strings.Replace(data.plain, "\n", "\\n", -1),
			)
		}
	}
}

func TestMetricPlainB(t *testing.T) {
	for _, data := range metricTestData {
		metric := &Metric{Name: data.name, Value: data.value, Timestamp: data.timestamp}
		plainRet := metric.PlainB(cleanPrefix(data.prefix))
		plain := []byte(data.plain)
		if bytes.Compare(plainRet, plain) != 0 {
			t.Errorf(
				"Metric:PlainB() was incorrect, got: %s, want: %s",
				bytes.Replace(plainRet, []byte("\n"), []byte("\\n"), -1),
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

func TestTcpClient(t *testing.T) {
	var metric *Metric
	var err error

	client, err := NewTCPClient("127.0.0.1", 62003, "", 1*time.Second)
	if err != nil {
		t.Errorf("Failed to create TCP Client")
		return
	}

	for _, data := range metricTestData {
		if data.prefix != "" {
			continue
		}
		metric = &Metric{Name: data.name, Value: data.value, Timestamp: data.timestamp}
		client.SendMetric(metric)
		plainRet, err := tcpServer.GetMessage(1 * time.Second)
		if err != nil {
			t.Errorf("server.GetMessage() failed with: %s", err)
		} else if plainRet != data.plain {
			t.Errorf(
				"server got incorrect message, got: %s, want: %s",
				strings.Replace(plainRet, "\n", "\\n", -1),
				strings.Replace(data.plain, "\n", "\\n", -1),
			)
		}
	}

	// after Shutdown(), should not receive anything
	client.Shutdown(1 * time.Second)
	client.SendMetric(&Metric{Name: "test.shutdown", Value: 0, Timestamp: 0})
	_, err = tcpServer.GetMessage(1 * time.Second)
	if err == nil {
		t.Errorf("Clien still sends data to server after shutdown")
	}
}

func TestTcpClientWithPrefix(t *testing.T) {
	var metric *Metric

	for _, data := range metricTestData {
		if data.prefix == "" {
			continue
		}

		client, err := NewTCPClient("127.0.0.1", 62003, data.prefix, 1*time.Second)
		if err != nil {
			t.Errorf("Failed to create TCP Client")
			continue
		}
		metric = &Metric{Name: data.name, Value: data.value, Timestamp: data.timestamp}
		client.SendMetric(metric)
		plainRet, err := tcpServer.GetMessage(1 * time.Second)
		if err != nil {
			t.Errorf("server.GetMessage() failed with: %s", err)
		} else if plainRet != data.plain {
			t.Errorf(
				"server got incorrect message, got: %s, want: %s",
				strings.Replace(plainRet, "\n", "\\n", -1),
				strings.Replace(data.plain, "\n", "\\n", -1),
			)
		}
		client.Shutdown(1 * time.Second)
	}
}

func TestTcpClientBulkSend(t *testing.T) {
	var metrics []*Metric
	var plains []string
	var err error

	for _, data := range metricTestData {
		if data.prefix != "" {
			continue
		}
		metrics = append(metrics, &Metric{Name: data.name, Value: data.value, Timestamp: data.timestamp})
		plains = append(plains, data.plain)
	}

	client, err := NewTCPClient("127.0.0.1", 62003, "", 1*time.Second)
	if err != nil {
		t.Errorf("Failed to create TCP Client")
		return
	}

	client.SendMetrics(metrics)
	for _, plain := range plains {
		plainRet, err := tcpServer.GetMessage(1 * time.Second)
		if err != nil {
			t.Errorf("server.GetMessage() failed with: %s", err)
		} else if plainRet != plain {
			t.Errorf(
				"server got incorrect message, got: %s, want: %s",
				strings.Replace(plainRet, "\n", "\\n", -1),
				strings.Replace(plain, "\n", "\\n", -1),
			)
		}
	}

	client.Shutdown(1 * time.Second)
}
