// Package relaytransport implements a libp2p transport that uses a relay server
// for peer discovery and connection. It supports automatic relay reservation,
// peer identification exchange, keep‑alive ping‑pong, and message routing.
package relaytransport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	"github.com/multiformats/go-multiaddr"
)

// PeerRegistry stores and retrieves peer information.
type PeerRegistry interface {
	// RegisterPeer stores a peer's username and address.
	RegisterPeer(peerID, username, address string) error
	// UsernameByPeerID returns the username for a given peer ID.
	UsernameByPeerID(peerID string) (string, bool)
	// GetKnownPeers returns all known peers (peerID → username).
	GetKnownPeers() map[string]string
	// MergeKnownPeers merges a map of peer IDs to usernames.
	MergeKnownPeers(peers map[string]string) error
}

// RelayDiscovery defines the interface for interacting with the relay server.
type RelayDiscovery interface {
	// Register sends a registration request to the relay.
	Register(ctx context.Context, host host.Host, relayID peer.ID, myPeerID string) error
	// Deregister sends a deregistration request.
	Deregister(ctx context.Context, host host.Host, relayID peer.ID, myPeerID string) error
	// GetPeerList requests the list of currently registered peers from the relay.
	GetPeerList(ctx context.Context, host host.Host, relayID peer.ID) ([]string, error)
}

// Config holds the configuration for a RelayTransport.
type Config struct {
	Host           host.Host
	RelayAddr      multiaddr.Multiaddr
	PrivKey        crypto.PrivKey // optional, not used internally
	Username       string
	PeerRegistry   PeerRegistry
	RelayDiscovery RelayDiscovery

	MessageProtocolID  string
	IdentifyProtocolID string

	KeepAliveInterval        time.Duration
	PingTimeout              time.Duration
	ReservationRenewInterval time.Duration
	DiscoveryInterval        time.Duration
}

// RelayTransport is a libp2p transport that uses a relay for peer discovery.
type RelayTransport struct {
	host     host.Host
	username string
	peerID   string

	relayAddr    multiaddr.Multiaddr
	relayInfo    *peer.AddrInfo
	observedAddr multiaddr.Multiaddr
	privKey      crypto.PrivKey

	peerRegistry   PeerRegistry
	relayDiscovery RelayDiscovery

	// Protocol IDs (converted to protocol.ID when used)
	identifyProtocolID string
	messageProtocolID  string

	// Handlers
	peerConnectedHandlers    []func(string)
	peerDisconnectedHandlers []func(string)
	messageReceivedHandlers  []func(string, string, []byte)
	messageHandler           func(string, string, []byte)
	mu                       sync.RWMutex

	// Keep-alive / ping-pong
	keepAliveCancel map[string]context.CancelFunc
	kaMu            sync.Mutex
	pendingPings    map[string]*pendingPing
	pingMu          sync.Mutex

	// Reservation
	reservationExpiry time.Time
	renewalTimer      *time.Timer
	renewalMu         sync.Mutex

	// Intervals
	keepAliveInterval        time.Duration
	pingTimeout              time.Duration
	reservationRenewInterval time.Duration
	discoveryInterval        time.Duration
}

type pendingPing struct {
	ch    chan time.Time
	timer *time.Timer
}

// NewRelayTransport creates a new RelayTransport with the given configuration.
func NewRelayTransport(cfg Config) (*RelayTransport, error) {
	if cfg.Host == nil {
		return nil, errors.New("host is required")
	}
	if cfg.RelayAddr == nil {
		return nil, errors.New("relay address is required")
	}
	if cfg.PeerRegistry == nil {
		return nil, errors.New("peer registry is required")
	}
	if cfg.RelayDiscovery == nil {
		cfg.RelayDiscovery = NewDefaultRelayDiscovery()
	}
	if cfg.MessageProtocolID == "" {
		cfg.MessageProtocolID = "/cashbook/1.0.0"
	}
	if cfg.IdentifyProtocolID == "" {
		cfg.IdentifyProtocolID = "/cashbook/identify/1.0.0"
	}
	if cfg.KeepAliveInterval <= 0 {
		cfg.KeepAliveInterval = 15 * time.Second
	}
	if cfg.PingTimeout <= 0 {
		cfg.PingTimeout = 5 * time.Second
	}
	if cfg.ReservationRenewInterval <= 0 {
		cfg.ReservationRenewInterval = 55 * time.Minute
	}
	if cfg.DiscoveryInterval <= 0 {
		cfg.DiscoveryInterval = 30 * time.Second
	}

	relayInfo, err := peer.AddrInfoFromP2pAddr(cfg.RelayAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid relay address: %w", err)
	}

	t := &RelayTransport{
		host:           cfg.Host,
		username:       cfg.Username,
		peerID:         cfg.Host.ID().String(),
		relayAddr:      cfg.RelayAddr,
		relayInfo:      relayInfo,
		privKey:        cfg.PrivKey,
		peerRegistry:   cfg.PeerRegistry,
		relayDiscovery: cfg.RelayDiscovery,

		identifyProtocolID: cfg.IdentifyProtocolID,
		messageProtocolID:  cfg.MessageProtocolID,

		keepAliveCancel: make(map[string]context.CancelFunc),
		pendingPings:    make(map[string]*pendingPing),

		keepAliveInterval:        cfg.KeepAliveInterval,
		pingTimeout:              cfg.PingTimeout,
		reservationRenewInterval: cfg.ReservationRenewInterval,
		discoveryInterval:        cfg.DiscoveryInterval,
	}

	// Set stream handlers (convert string to protocol.ID)
	t.host.SetStreamHandler(protocol.ID(t.identifyProtocolID), t.handleIdentifyStream)
	t.host.SetStreamHandler(protocol.ID(t.messageProtocolID), t.handleStream)

	return t, nil
}

// Start connects to the relay, reserves a slot, and starts background tasks.
func (t *RelayTransport) Start(ctx context.Context) error {
	// 1. Connect to relay
	if err := t.host.Connect(ctx, *t.relayInfo); err != nil {
		return fmt.Errorf("connect to relay: %w", err)
	}
	fmt.Println("✅ Connected to relay")

	// 2. Get observed address from the connection
	for _, conn := range t.host.Network().ConnsToPeer(t.relayInfo.ID) {
		t.observedAddr = conn.RemoteMultiaddr()
		break
	}
	if t.observedAddr != nil {
		fmt.Printf("📡 Observed address from relay: %s\n", t.observedAddr)
	}

	// 3. Reserve a slot on the relay
	resv, err := client.Reserve(ctx, t.host, *t.relayInfo)
	if err != nil {
		return fmt.Errorf("relay reservation failed: %w", err)
	}
	fmt.Printf("📅 Relay reservation made, expires: %s\n", resv.Expiration.Format("15:04:05"))
	t.setReservationExpiry(resv.Expiration)

	// 4. Add circuit address for self
	circuitAddr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("%s/p2p-circuit", t.relayAddr.String()))
	t.host.Peerstore().AddAddr(t.host.ID(), circuitAddr, peerstore.PermanentAddrTTL)
	fmt.Println("🔌 Added self circuit address")

	// 5. Register with relay
	if err := t.registerWithRelay(ctx); err != nil {
		fmt.Printf("⚠️ Relay registration failed: %v\n", err)
	} else {
		fmt.Println("📝 Relay registration acknowledged")
		go t.discoverOnce(ctx)
	}

	// 6. Start background tasks
	go t.renewReservationLoop(ctx)
	go t.discoverPeersLoop(ctx)

	return nil
}

// Stop deregisters from the relay and closes the host.
func (t *RelayTransport) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := t.deregisterFromRelay(ctx); err != nil {
		fmt.Printf("⚠️ Failed to deregister from relay: %v\n", err)
	}
	return t.host.Close()
}

// SendMessage sends a JSON‑encoded message to a peer using the message protocol.
func (t *RelayTransport) SendMessage(ctx context.Context, toPeerID, msgType string, payload []byte) error {
	pid, err := peer.Decode(toPeerID)
	if err != nil {
		return fmt.Errorf("invalid peer ID: %w", err)
	}
	if !t.IsPeerConnected(toPeerID) {
		return fmt.Errorf("peer %s not connected", toPeerID)
	}
	ctx = network.WithAllowLimitedConn(ctx, "relay send")
	s, err := t.host.NewStream(ctx, pid, protocol.ID(t.messageProtocolID))
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	defer s.Close()

	message := map[string]interface{}{
		"type":    msgType,
		"payload": json.RawMessage(payload),
	}
	if err := json.NewEncoder(s).Encode(message); err != nil {
		return fmt.Errorf("failed to encode message: %w", err)
	}
	return nil
}

// SetMessageHandler sets a single handler for all incoming messages.
func (t *RelayTransport) SetMessageHandler(handler func(fromPeerID string, msgType string, payload []byte)) {
	t.messageHandler = handler
}

// OnPeerConnected adds a handler that is called when a peer connects.
func (t *RelayTransport) OnPeerConnected(handler func(string)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peerConnectedHandlers = append(t.peerConnectedHandlers, handler)
}

// OnPeerDisconnected adds a handler that is called when a peer disconnects.
func (t *RelayTransport) OnPeerDisconnected(handler func(string)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peerDisconnectedHandlers = append(t.peerDisconnectedHandlers, handler)
}

// OnMessageReceived adds a handler that is called when a message is received.
func (t *RelayTransport) OnMessageReceived(handler func(string, string, []byte)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.messageReceivedHandlers = append(t.messageReceivedHandlers, handler)
}

// GetConnectedPeers returns information about currently connected peers.
func (t *RelayTransport) GetConnectedPeers() []PeerInfo {
	conns := t.host.Network().Conns()
	peerMap := make(map[string]PeerInfo)
	relayID := t.relayInfo.ID.String()

	for _, conn := range conns {
		pid := conn.RemotePeer().String()
		if pid == relayID {
			continue // skip relay
		}
		if _, exists := peerMap[pid]; exists {
			continue
		}
		username, _ := t.peerRegistry.UsernameByPeerID(pid)
		peerMap[pid] = PeerInfo{
			ID:       pid,
			Username: username,
			Address:  conn.RemoteMultiaddr().String(),
			Online:   true,
		}
	}
	peers := make([]PeerInfo, 0, len(peerMap))
	for _, p := range peerMap {
		peers = append(peers, p)
	}
	return peers
}

// IsPeerConnected checks if a peer is currently connected.
func (t *RelayTransport) IsPeerConnected(peerID string) bool {
	pid, err := peer.Decode(peerID)
	if err != nil {
		return false
	}
	return len(t.host.Network().ConnsToPeer(pid)) > 0
}

// DisconnectFromPeer closes all connections to the given peer.
func (t *RelayTransport) DisconnectFromPeer(peerID string) error {
	t.stopKeepAlive(peerID)
	pid, err := peer.Decode(peerID)
	if err != nil {
		return fmt.Errorf("invalid peer ID: %w", err)
	}
	return t.host.Network().ClosePeer(pid)
}

// GetPeerID returns the local peer ID.
func (t *RelayTransport) GetPeerID() string {
	return t.peerID
}

// GetUsername returns the local username.
func (t *RelayTransport) GetUsername() string {
	return t.username
}

// -------------------- Internal methods --------------------

func (t *RelayTransport) registerWithRelay(ctx context.Context) error {
	return t.relayDiscovery.Register(ctx, t.host, t.relayInfo.ID, t.peerID)
}

func (t *RelayTransport) deregisterFromRelay(ctx context.Context) error {
	return t.relayDiscovery.Deregister(ctx, t.host, t.relayInfo.ID, t.peerID)
}

func (t *RelayTransport) requestPeerList(ctx context.Context) ([]string, error) {
	return t.relayDiscovery.GetPeerList(ctx, t.host, t.relayInfo.ID)
}

func (t *RelayTransport) renewReservationLoop(ctx context.Context) {
	ticker := time.NewTicker(t.reservationRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resv, err := client.Reserve(ctx, t.host, *t.relayInfo)
			if err != nil {
				fmt.Printf("⚠️ Reservation renewal failed: %v\n", err)
			} else {
				fmt.Printf("📅 Reservation renewed, expires: %s\n", resv.Expiration.Format("15:04:05"))
				t.setReservationExpiry(resv.Expiration)
			}
		}
	}
}

func (t *RelayTransport) discoverPeersLoop(ctx context.Context) {
	ticker := time.NewTicker(t.discoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			peerIDs, err := t.requestPeerList(ctx)
			if err != nil {
				fmt.Printf("⚠️ Relay discovery error: %v\n", err)
				continue
			}
			t.handleDiscoveredPeers(peerIDs)
		}
	}
}

func (t *RelayTransport) discoverOnce(ctx context.Context) {
	peerIDs, err := t.requestPeerList(ctx)
	if err != nil {
		fmt.Printf("⚠️ Immediate discovery error: %v\n", err)
		return
	}
	t.handleDiscoveredPeers(peerIDs)
}

func (t *RelayTransport) handleDiscoveredPeers(peerIDs []string) {
	for _, pidStr := range peerIDs {
		pid, err := peer.Decode(pidStr)
		if err != nil {
			log.Printf("⚠️ Invalid peer ID in discovery list: %s", pidStr)
			continue
		}
		if pid == t.host.ID() {
			continue
		}
		if t.IsPeerConnected(pid.String()) {
			continue
		}
		log.Printf("🔍 Discovered peer via relay: %s\n", pid.String()[:12])

		circuitAddrStr := fmt.Sprintf("%s/p2p-circuit/p2p/%s", t.relayAddr.String(), pid.String())
		circuitAddr, err := multiaddr.NewMultiaddr(circuitAddrStr)
		if err != nil {
			log.Printf("⚠️ Failed to construct circuit address for %s: %v", pid.String()[:12], err)
			continue
		}
		pi := peer.AddrInfo{
			ID:    pid,
			Addrs: []multiaddr.Multiaddr{circuitAddr},
		}

		go func(pi peer.AddrInfo) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := t.connectAndIdentify(ctx, pi); err != nil {
				fmt.Printf("⚠️ Failed to connect to %s: %v\n", pi.ID.String()[:12], err)
			}
		}(pi)
	}
}

func (t *RelayTransport) connectAndIdentify(ctx context.Context, pi peer.AddrInfo) error {
	if err := t.host.Connect(ctx, pi); err != nil {
		return fmt.Errorf("connect failed: %w", err)
	}
	username, err := t.exchangeIdentification(ctx, pi.ID.String())
	if err != nil {
		t.host.Network().ClosePeer(pi.ID)
		return fmt.Errorf("identification failed: %w", err)
	}
	address := ""
	if len(pi.Addrs) > 0 {
		address = pi.Addrs[0].String()
	}
	if err := t.peerRegistry.RegisterPeer(pi.ID.String(), username, address); err != nil {
		log.Printf("⚠️ Failed to register peer %s: %v", pi.ID.String()[:12], err)
	}
	t.startKeepAlive(pi.ID.String())

	t.mu.RLock()
	handlers := t.peerConnectedHandlers
	t.mu.RUnlock()
	for _, h := range handlers {
		h(pi.ID.String())
	}

	fmt.Printf("✅ Identified peer %s as %s\n", pi.ID.String()[:12], username)
	return nil
}

func (t *RelayTransport) exchangeIdentification(ctx context.Context, peerID string) (string, error) {
	ctx = network.WithAllowLimitedConn(ctx, "identify")
	pid, err := peer.Decode(peerID)
	if err != nil {
		return "", fmt.Errorf("invalid peer ID: %w", err)
	}
	s, err := t.host.NewStream(ctx, pid, protocol.ID(t.identifyProtocolID))
	if err != nil {
		return "", fmt.Errorf("open identify stream: %w", err)
	}
	defer s.Close()

	// Send our info
	req := struct {
		Username   string            `json:"username"`
		PeerID     string            `json:"peer_id"`
		KnownPeers map[string]string `json:"known_peers"`
	}{
		Username:   t.username,
		PeerID:     t.peerID,
		KnownPeers: t.peerRegistry.GetKnownPeers(),
	}
	if err := json.NewEncoder(s).Encode(req); err != nil {
		return "", fmt.Errorf("send identify: %w", err)
	}

	// Read response
	s.SetReadDeadline(time.Now().Add(3 * time.Second))
	var resp struct {
		Username   string            `json:"username"`
		PeerID     string            `json:"peer_id"`
		KnownPeers map[string]string `json:"known_peers"`
	}
	if err := json.NewDecoder(s).Decode(&resp); err != nil {
		return "", fmt.Errorf("receive identify: %w", err)
	}
	s.SetReadDeadline(time.Time{})

	if resp.PeerID != peerID {
		return "", fmt.Errorf("peer ID mismatch: expected %s, got %s", peerID, resp.PeerID)
	}
	if resp.Username == "" {
		return "", fmt.Errorf("received empty username")
	}

	if err := t.peerRegistry.MergeKnownPeers(resp.KnownPeers); err != nil {
		log.Printf("⚠️ Failed to merge known peers: %v", err)
	}
	return resp.Username, nil
}

func (t *RelayTransport) handleIdentifyStream(s network.Stream) {
	defer s.Close()
	remotePeerID := s.Conn().RemotePeer().String()
	fmt.Printf("🔐 Identification request from: %s\n", remotePeerID[:12])

	s.SetReadDeadline(time.Now().Add(10 * time.Second))
	var incomingMsg struct {
		Username   string            `json:"username"`
		PeerID     string            `json:"peer_id"`
		KnownPeers map[string]string `json:"known_peers"`
	}
	decoder := json.NewDecoder(s)
	if err := decoder.Decode(&incomingMsg); err != nil {
		fmt.Printf("❌ Failed to read identification: %v\n", err)
		return
	}
	s.SetReadDeadline(time.Time{})

	if incomingMsg.PeerID != remotePeerID {
		fmt.Printf("⚠️ Peer ID mismatch from %s\n", remotePeerID[:12])
		return
	}
	if incomingMsg.Username == "" {
		fmt.Printf("⚠️ Empty username from %s\n", remotePeerID[:12])
		return
	}

	if err := t.peerRegistry.MergeKnownPeers(incomingMsg.KnownPeers); err != nil {
		log.Printf("⚠️ Failed to merge known peers: %v", err)
	}

	response := struct {
		Username   string            `json:"username"`
		PeerID     string            `json:"peer_id"`
		KnownPeers map[string]string `json:"known_peers"`
	}{
		Username:   t.username,
		PeerID:     t.peerID,
		KnownPeers: t.peerRegistry.GetKnownPeers(),
	}
	if err := json.NewEncoder(s).Encode(response); err != nil {
		fmt.Printf("❌ Failed to send identification response: %v\n", err)
		return
	}

	address := s.Conn().RemoteMultiaddr().String()
	if err := t.peerRegistry.RegisterPeer(remotePeerID, incomingMsg.Username, address); err != nil {
		log.Printf("⚠️ Failed to register peer %s: %v", remotePeerID[:12], err)
	}
	t.startKeepAlive(remotePeerID)

	fmt.Printf("✅ Identified and registered peer: %s (%s)\n", incomingMsg.Username, remotePeerID[:12])
}

func (t *RelayTransport) handleStream(s network.Stream) {
	defer s.Close()
	remotePeerID := s.Conn().RemotePeer().String()

	s.SetReadDeadline(time.Now().Add(5 * time.Second))
	var msg map[string]json.RawMessage
	decoder := json.NewDecoder(s)
	if err := decoder.Decode(&msg); err != nil {
		fmt.Printf("❌ Failed to decode message from %s: %v\n", remotePeerID[:12], err)
		return
	}
	s.SetReadDeadline(time.Time{})

	var msgType string
	if err := json.Unmarshal(msg["type"], &msgType); err != nil {
		fmt.Printf("❌ Failed to parse message type from %s: %v\n", remotePeerID[:12], err)
		return
	}
	payload := msg["payload"]

	// Handle ping/pong internally
	switch msgType {
	case "ping":
		var pingData struct{ Nonce string }
		if err := json.Unmarshal(payload, &pingData); err != nil {
			fmt.Printf("❌ Failed to parse ping: %v\n", err)
			return
		}
		// Send pong as a new message (not on the same stream)
		pongPayload, _ := json.Marshal(map[string]string{"nonce": pingData.Nonce})
		// Use background context – the pong is independent of the incoming stream
		if err := t.SendMessage(context.Background(), remotePeerID, "pong", pongPayload); err != nil {
			fmt.Printf("⚠️ Failed to send pong: %v\n", err)
		} /* else {
			log.Printf("📤 Sent pong to %s (nonce %s)", remotePeerID[:12], pingData.Nonce)
		} */
		return

	case "pong":
		var pongData struct{ Nonce string }
		if err := json.Unmarshal(payload, &pongData); err != nil {
			fmt.Printf("❌ Failed to parse pong: %v\n", err)
			return
		}
		// log.Printf("📥 Received pong from %s (nonce %s)", remotePeerID[:12], pongData.Nonce)
		t.handlePong(pongData.Nonce)
		return
	}

	// Forward other messages
	t.mu.RLock()
	handlers := t.messageReceivedHandlers
	t.mu.RUnlock()
	for _, handler := range handlers {
		handler(remotePeerID, msgType, payload)
	}
	if t.messageHandler != nil {
		t.messageHandler(remotePeerID, msgType, payload)
	}
}

func (t *RelayTransport) shouldSendKeepAlive(remotePeerID string) bool {
	return t.peerID < remotePeerID
}

func (t *RelayTransport) startKeepAlive(peerID string) {
	if !t.shouldSendKeepAlive(peerID) {
		return
	}
	t.kaMu.Lock()
	defer t.kaMu.Unlock()
	if cancel, exists := t.keepAliveCancel[peerID]; exists {
		cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.keepAliveCancel[peerID] = cancel

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("🔥 Ping-pong goroutine for %s panicked: %v (restarting)", peerID[:12], r)
				time.Sleep(5 * time.Second)
				t.startKeepAlive(peerID)
			}
		}()

		ticker := time.NewTicker(t.keepAliveInterval)
		defer ticker.Stop()
		var seq int64 = 0
		for {
			select {
			case <-ctx.Done():
				log.Printf("🛑 Ping-pong stopped for %s", peerID[:12])
				return
			case <-ticker.C:
				if !t.IsPeerConnected(peerID) {
					log.Printf("👋 Peer %s disconnected, stopping ping-pong", peerID[:12])
					return
				}
				nonce := fmt.Sprintf("%d-%d", time.Now().UnixNano(), seq)
				seq++

				// log.Printf("Sending ping to %s (nonce %s)", peerID[:12], nonce)

				pongCh := make(chan time.Time, 1)
				t.pingMu.Lock()
				timer := time.AfterFunc(t.pingTimeout, func() {
					t.pingMu.Lock()
					delete(t.pendingPings, nonce)
					t.pingMu.Unlock()
					log.Printf("⚠️ Ping timeout for %s (nonce %s)", peerID[:12], nonce)
				})
				t.pendingPings[nonce] = &pendingPing{ch: pongCh, timer: timer}
				t.pingMu.Unlock()

				payload, _ := json.Marshal(map[string]string{"nonce": nonce})
				err := t.SendMessage(ctx, peerID, "ping", payload)
				if err != nil {
					log.Printf("⚠️ Ping to %s failed: %v", peerID[:12], err)
					t.pingMu.Lock()
					if pp, ok := t.pendingPings[nonce]; ok {
						pp.timer.Stop()
						delete(t.pendingPings, nonce)
					}
					t.pingMu.Unlock()
					continue
				}

				select {
				case <-pongCh:
					// success
				case <-time.After(t.pingTimeout):
					// already handled by timer
				}
			}
		}
	}()
}

func (t *RelayTransport) handlePong(nonce string) {
	t.pingMu.Lock()
	defer t.pingMu.Unlock()
	if pp, ok := t.pendingPings[nonce]; ok {
		delete(t.pendingPings, nonce)
		pp.timer.Stop()
		select {
		case pp.ch <- time.Now():
		default:
		}
		close(pp.ch)
	}
}

func (t *RelayTransport) stopKeepAlive(peerID string) {
	t.kaMu.Lock()
	defer t.kaMu.Unlock()
	if cancel, exists := t.keepAliveCancel[peerID]; exists {
		cancel()
		delete(t.keepAliveCancel, peerID)
	}
}

func (t *RelayTransport) setReservationExpiry(expiry time.Time) {
	t.renewalMu.Lock()
	defer t.renewalMu.Unlock()
	t.reservationExpiry = expiry
	if t.renewalTimer != nil {
		t.renewalTimer.Stop()
	}
	renewTime := expiry.Add(-2 * time.Minute)
	now := time.Now()
	if renewTime.Before(now) {
		go t.renewReservation(context.Background())
		return
	}
	t.renewalTimer = time.AfterFunc(time.Until(renewTime), func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		t.renewReservation(ctx)
	})
}

func (t *RelayTransport) renewReservation(ctx context.Context) {
	log.Println("Attempting to renew relay reservation...")
	resv, err := client.Reserve(ctx, t.host, *t.relayInfo)
	if err != nil {
		log.Printf("❌ Reservation renewal failed: %v", err)
		time.AfterFunc(1*time.Minute, func() {
			t.renewReservation(context.Background())
		})
	} else {
		log.Printf("📅 Reservation renewed, expires: %s", resv.Expiration.Format("15:04:05"))
		t.setReservationExpiry(resv.Expiration)
	}
}

// -------------------- DefaultRelayDiscovery --------------------

// DefaultRelayDiscovery implements the RelayDiscovery interface using the
// original /bpc/discovery/1.0.0 JSON protocol.
type DefaultRelayDiscovery struct {
	ProtocolID string
}

// NewDefaultRelayDiscovery returns a DefaultRelayDiscovery with the default protocol ID.
func NewDefaultRelayDiscovery() *DefaultRelayDiscovery {
	return &DefaultRelayDiscovery{ProtocolID: "/bpc/discovery/1.0.0"}
}

type registerPayload struct {
	PeerID string `json:"peer_id"`
}
type registerMessage struct {
	Type    string          `json:"type"`
	Payload registerPayload `json:"payload"`
}
type discoverMessage struct {
	Type string `json:"type"`
}
type deregisterPayload struct {
	PeerID string `json:"peer_id"`
}
type deregisterMessage struct {
	Type    string            `json:"type"`
	Payload deregisterPayload `json:"payload"`
}
type peersMessage struct {
	Type    string   `json:"type"`
	Payload []string `json:"payload"`
}

func (d *DefaultRelayDiscovery) Register(ctx context.Context, host host.Host, relayID peer.ID, myPeerID string) error {
	ctx = network.WithAllowLimitedConn(ctx, "relay register")
	s, err := host.NewStream(ctx, relayID, protocol.ID(d.ProtocolID))
	if err != nil {
		return err
	}
	defer s.Close()

	msg := registerMessage{
		Type: "Register",
		Payload: registerPayload{
			PeerID: myPeerID,
		},
	}
	data, _ := json.Marshal(msg)
	if _, err := s.Write(data); err != nil {
		return err
	}
	s.CloseWrite()
	// ignore ack
	var ack json.RawMessage
	_ = json.NewDecoder(s).Decode(&ack)
	return nil
}

func (d *DefaultRelayDiscovery) Deregister(ctx context.Context, host host.Host, relayID peer.ID, myPeerID string) error {
	ctx = network.WithAllowLimitedConn(ctx, "relay deregister")
	s, err := host.NewStream(ctx, relayID, protocol.ID(d.ProtocolID))
	if err != nil {
		return err
	}
	defer s.Close()

	msg := deregisterMessage{
		Type: "Deregister",
		Payload: deregisterPayload{
			PeerID: myPeerID,
		},
	}
	data, _ := json.Marshal(msg)
	if _, err := s.Write(data); err != nil {
		return err
	}
	s.CloseWrite()
	return nil
}

func (d *DefaultRelayDiscovery) GetPeerList(ctx context.Context, host host.Host, relayID peer.ID) ([]string, error) {
	ctx = network.WithAllowLimitedConn(ctx, "get peerlist")
	s, err := host.NewStream(ctx, relayID, protocol.ID(d.ProtocolID))
	if err != nil {
		return nil, err
	}
	defer s.Close()

	msg := discoverMessage{Type: "Discover"}
	data, _ := json.Marshal(msg)
	if _, err := s.Write(data); err != nil {
		return nil, err
	}
	s.CloseWrite()

	responseBytes, err := io.ReadAll(s)
	if err != nil {
		return nil, err
	}

	var resp peersMessage
	if err := json.Unmarshal(responseBytes, &resp); err != nil {
		return nil, fmt.Errorf("invalid Peers response: %w", err)
	}
	if resp.Type != "Peers" {
		return nil, fmt.Errorf("expected Peers response, got %s", resp.Type)
	}
	return resp.Payload, nil
}

// -------------------- PeerInfo --------------------

// PeerInfo contains basic information about a connected peer.
type PeerInfo struct {
	ID       string
	Username string
	Address  string
	Online   bool
}
