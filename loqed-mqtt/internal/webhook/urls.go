package webhook

import (
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/gateway"
)

// PrivateURL is the URL the bridge calls for lockID: webhook.private_url
// if set, otherwise http://<this host's address towards the bridge>:<port>.
func PrivateURL(base string, port int, lockID, bridgeIP string) (string, error) {
	path := "/webhook/" + url.PathEscape(lockID)
	if base != "" {
		return strings.TrimRight(base, "/") + path, nil
	}
	ip, err := SourceIP(bridgeIP)
	if err != nil {
		return "", fmt.Errorf("webhook: cannot work out this host's address towards the bridge (set webhook.private_url): %w", err)
	}
	return "http://" + net.JoinHostPort(ip.String(), strconv.Itoa(port)) + path, nil
}

// SourceIP returns the local address the OS would use to reach the bridge.
// Connecting a UDP socket sends no packets.
func SourceIP(bridgeIP string) (net.IP, error) {
	conn, err := net.Dial("udp", gateway.BridgeAddress(bridgeIP))
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	return conn.LocalAddr().(*net.UDPAddr).IP, nil
}

var dockerNets = netip.MustParsePrefix("172.16.0.0/12")

// LikelyContainerAddress reports whether local looks like a Docker bridge
// network address that the LOQED bridge (on the LAN) cannot reach.
func LikelyContainerAddress(local net.IP, bridgeIP string) bool {
	l, ok := netip.AddrFromSlice(local)
	if !ok {
		return false
	}
	host := bridgeIP
	if h, _, err := net.SplitHostPort(bridgeIP); err == nil {
		host = h
	}
	b, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return dockerNets.Contains(l.Unmap()) && !dockerNets.Contains(b.Unmap())
}

// CloudURL is the URL to register for one lock at app.loqed.com.
func CloudURL(publicBase, secret, lockID string) string {
	return strings.TrimRight(publicBase, "/") + "/cloud/" + secret + "/" + url.PathEscape(lockID)
}
