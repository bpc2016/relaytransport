package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/multiformats/go-multiaddr"

	// Replace with your actual import path after publishing the package
	relaytransport "github.com/bpc2016/relaytransport"
)

// --------------------------------------------------------------------
// In-memory peer registry (simple map)
// --------------------------------------------------------------------
type memoryRegistry struct {
	mu    sync.RWMutex
	peers map[string]string // peerID -> username
}

func newMemoryRegistry() *memoryRegistry {
	return &memoryRegistry{peers: make(map[string]string)}
}

func (r *memoryRegistry) RegisterPeer(peerID, username, address string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers[peerID] = username
	return nil
}

func (r *memoryRegistry) UsernameByPeerID(peerID string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	u, ok := r.peers[peerID]
	return u, ok
}

func (r *memoryRegistry) GetKnownPeers() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c := make(map[string]string, len(r.peers))
	for k, v := range r.peers {
		c[k] = v
	}
	return c
}

func (r *memoryRegistry) MergeKnownPeers(peers map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, v := range peers {
		if _, exists := r.peers[k]; !exists {
			r.peers[k] = v
		}
	}
	return nil
}

// --------------------------------------------------------------------
// Peer runner
// --------------------------------------------------------------------
type peer struct {
	host      host.Host
	transport *relaytransport.RelayTransport
	username  string
	registry  *memoryRegistry
}

func newPeer(ctx context.Context, username string, relayMA multiaddr.Multiaddr) (*peer, error) {
	// 1. Create libp2p host
	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"),
		libp2p.EnableRelay(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create host: %w", err)
	}

	// 2. Create registry and transport
	reg := newMemoryRegistry()
	transport, err := relaytransport.NewRelayTransport(relaytransport.Config{
		Host:           h,
		RelayAddr:      relayMA,
		Username:       username,
		PeerRegistry:   reg,
		RelayDiscovery: relaytransport.NewDefaultRelayDiscovery(), // uses /bpc/discovery/1.0.0
	})
	if err != nil {
		h.Close()
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	// 3. Set message handler (just logs)
	transport.SetMessageHandler(func(fromPeerID, msgType string, payload []byte) {
		log.Printf("[%s] Received %s from %s: %s", username, msgType, fromPeerID, string(payload))
	})

	// 4. Start the transport
	if err := transport.Start(ctx); err != nil {
		h.Close()
		return nil, fmt.Errorf("failed to start transport: %w", err)
	}

	return &peer{
		host:      h,
		transport: transport,
		username:  username,
		registry:  reg,
	}, nil
}

func (p *peer) close() {
	p.transport.Stop()
	p.host.Close()
}

// --------------------------------------------------------------------
// Main
// --------------------------------------------------------------------
func main() {
	// Parse command-line arguments
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <relay_multiaddr> [alice_username] [bob_username]\n", os.Args[0])
	}
	relayAddrStr := os.Args[1]
	aliceUsername := "Alice"
	bobUsername := "Bob"
	if len(os.Args) >= 3 {
		aliceUsername = os.Args[2]
	}
	if len(os.Args) >= 4 {
		bobUsername = os.Args[3]
	}

	relayMA, err := multiaddr.NewMultiaddr(relayAddrStr)
	if err != nil {
		log.Fatalf("Invalid relay address: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create two peers
	alice, err := newPeer(ctx, aliceUsername, relayMA)
	if err != nil {
		log.Fatalf("Failed to create Alice: %v", err)
	}
	defer alice.close()

	bob, err := newPeer(ctx, bobUsername, relayMA)
	if err != nil {
		log.Fatalf("Failed to create Bob: %v", err)
	}
	defer bob.close()

	log.Printf("Alice peer ID: %s", alice.host.ID().String())
	log.Printf("Bob peer ID: %s", bob.host.ID().String())

	// Wait for peers to discover each other (up to 30 seconds)
	log.Println("Waiting for peers to discover each other...")
	discovered := make(chan struct{}, 2)

	// Use connection handlers to know when a peer connects
	alice.transport.OnPeerConnected(func(peerID string) {
		if peerID == bob.host.ID().String() {
			log.Printf("Alice: Bob connected!")
			discovered <- struct{}{}
		}
	})
	bob.transport.OnPeerConnected(func(peerID string) {
		if peerID == alice.host.ID().String() {
			log.Printf("Bob: Alice connected!")
			discovered <- struct{}{}
		}
	})

	// Wait for both to connect (or timeout)
	timeout := time.After(30 * time.Second)
	connectedCount := 0
waitLoop:
	for {
		select {
		case <-discovered:
			connectedCount++
			if connectedCount == 2 {
				break waitLoop
			}
		case <-timeout:
			log.Fatal("Timed out waiting for peers to connect")
		}
	}

	// Send a message from Alice to Bob
	log.Printf("Sending message from %s to %s", aliceUsername, bobUsername)
	err = alice.transport.SendMessage(ctx, bob.host.ID().String(), "greeting", []byte("Hello Bob!"))
	if err != nil {
		log.Printf("Send failed: %v", err)
	} else {
		log.Println("Message sent successfully")
	}

	// Wait a moment for the message to be received (should be almost instant)
	time.Sleep(2 * time.Second)

	log.Println("Test completed successfully. Press Ctrl+C to exit.")
	// Wait for interrupt to cleanly shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh
	log.Println("Shutting down...")
}
