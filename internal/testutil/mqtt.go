// Package testutil provides an in-process MQTT broker and subscriber for tests.
package testutil

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	mqttserver "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

// StartBroker runs a mochi broker on a free localhost port.
func StartBroker(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	srv := mqttserver.New(nil)
	if err := srv.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatal(err)
	}
	// mochi v2.6+: NewTCP(listeners.Config). If the signature differs, check `go doc github.com/mochi-mqtt/server/v2/listeners NewTCP`.
	if err := srv.AddListener(listeners.NewTCP(listeners.Config{ID: "test", Address: addr})); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() {
		// mochi v2.7.9 can deadlock in Close when a client connects at the
		// same moment (Clients.GetByListener re-takes its read lock while
		// Delete waits for the write lock). Don't let that hang the run.
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = srv.Close()
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Log("mqtt test broker did not close within 5 s (mochi shutdown deadlock); leaving it")
		}
	})
	return "tcp://" + addr
}

var subscribers atomic.Int64

type Message struct {
	Topic    string
	Payload  []byte
	Retained bool
}

type Subscriber struct {
	mu   sync.Mutex
	msgs []Message
	c    mqtt.Client
}

func Subscribe(t *testing.T, url, filter string) *Subscriber {
	t.Helper()
	s := &Subscriber{}
	// Unique client ids: a broker disconnects an existing client when another
	// connects with the same id.
	id := fmt.Sprintf("sub-%s-%d", t.Name(), subscribers.Add(1))
	opts := mqtt.NewClientOptions().AddBroker(url).SetClientID(id).SetCleanSession(true)
	s.c = mqtt.NewClient(opts)
	if tok := s.c.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscriber connect: %v", tok.Error())
	}
	tok := s.c.Subscribe(filter, 1, func(_ mqtt.Client, m mqtt.Message) {
		s.mu.Lock()
		s.msgs = append(s.msgs, Message{Topic: m.Topic(), Payload: append([]byte(nil), m.Payload()...), Retained: m.Retained()})
		s.mu.Unlock()
	})
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	t.Cleanup(func() { s.c.Disconnect(100) })
	return s
}

func (s *Subscriber) WaitFor(t *testing.T, timeout time.Duration, match func(Message) bool) Message {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		for _, m := range s.msgs {
			if match(m) {
				s.mu.Unlock()
				return m
			}
		}
		s.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var topics []string
	for _, m := range s.msgs {
		topics = append(topics, m.Topic+"="+string(m.Payload))
	}
	t.Fatalf("no matching MQTT message within %s; got %v", timeout, topics)
	return Message{}
}

// Messages returns every message received so far, in order.
func (s *Subscriber) Messages() []Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Message(nil), s.msgs...)
}

func (s *Subscriber) Count(match func(Message) bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, m := range s.msgs {
		if match(m) {
			n++
		}
	}
	return n
}

func (s *Subscriber) Publish(t *testing.T, topic, payload string, retained bool) {
	t.Helper()
	if tok := s.c.Publish(topic, 1, retained, payload); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("publish: %v", tok.Error())
	}
}

// Topic returns a matcher for messages on topic.
func Topic(topic string) func(Message) bool {
	return func(m Message) bool { return m.Topic == topic }
}

// Kick connects briefly with clientID. The broker then drops the existing
// client with that id (session takeover), so it has to reconnect.
func Kick(t *testing.T, url, clientID string) {
	t.Helper()
	c := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(url).SetClientID(clientID).SetCleanSession(true))
	if tok := c.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("kick connect: %v", tok.Error())
	}
	c.Disconnect(0)
}
