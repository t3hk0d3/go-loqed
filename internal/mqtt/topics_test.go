package mqtt_test

import (
	"testing"

	"github.com/t3hk0d3/go-loqed/internal/mqtt"
)

var topics = mqtt.Topics{Base: "loqed"}

func TestTopics(t *testing.T) {
	cases := map[string]string{
		topics.Status():             "loqed/status",
		topics.Availability("abc"):  "loqed/abc/availability",
		topics.State("abc"):         "loqed/abc/state",
		topics.Event("abc"):         "loqed/abc/event",
		topics.Command("abc"):       "loqed/abc/command",
		topics.CommandStatus("abc"): "loqed/abc/command_status",
		topics.CommandWildcard():    "loqed/+/command",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("got %s want %s", got, want)
		}
	}
}

func TestTopicIDReplacesUnsafeCharacters(t *testing.T) {
	if got := mqtt.TopicID("Yq1g/K4+#x y"); got != "Yq1g_K4__x_y" {
		t.Errorf("TopicID: %s", got)
	}
}
