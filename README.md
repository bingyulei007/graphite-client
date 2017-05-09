A Graphite A go library for sending metrics to graphite.

# Usage

``` go
import (
        "github.com/openmetric/graphite-client"
)

// Create a client
client := graphite.NewClient("tcp://127.0.0.1:2003", "", 1*time.Second)

// Create a Metric instance, and send this metric
metric := &graphite.Metric{
	Name:      "test.int",
	Value:     1,
	Timestamp: time.Now().Unix(),
}
client.SendMetric(metric)

// Send something without create a Metric instance,
// if timestamp is 0, time.Now() is used automatically.
client.SendSimple("test.int", 2, 0)

// Shutdown will block the caller until all metrics in buffer have been sent, or timeout occurs.
// If timeout occurs before all metrics flushed, metrics left in buffer will be lost.
client.Shutdown(1*time.Second)

// ---------------------------------------------

// Name of all metrics sent by this client will be prefixed with 'myprogram.'
prefixedClient := graphite.NewClient("udp://127.0.0.1:2003", "myprogram.", 1*time.Second)

// Server will get: 'myprogram.test.float 1.2 1493795673'
client.SendSimple("test.float", 1.2, 1493795673)

client.Shutdown()

// ---------------------------------------------

// NopClient just silently drops all data.
if config.DisableGraphite {
	client := graphite.NewNopClient()
}
client.SendSimple("any.metric", 0, 0)
client.Shutdown()
```
