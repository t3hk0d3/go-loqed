module github.com/t3hk0d3/go-loqed/loqed-mqtt

go 1.27

require (
	github.com/eclipse/paho.mqtt.golang v1.5.1
	github.com/mochi-mqtt/server/v2 v2.7.9
	github.com/t3hk0d3/go-loqed v0.0.0-00010101000000-000000000000
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/rs/xid v1.4.0 // indirect
	golang.org/x/net v0.44.0 // indirect
	golang.org/x/sync v0.17.0 // indirect
)

replace github.com/t3hk0d3/go-loqed => ../
