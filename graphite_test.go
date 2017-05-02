package graphite

import (
	"bytes"
	"strings"

	"testing"
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
