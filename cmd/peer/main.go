package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/multiformats/go-multiaddr"

	relaytransport "github.com/bpc2016/relaytransport" // adjust
)

// --------------------------------------------------------------------
// In‑memory peer registry
// --------------------------------------------------------------------
type memoryRegistry struct {
	mu    sync.RWMutex
	peers map[string]string
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
// Main
// --------------------------------------------------------------------
func main() {
	username := flag.String("name", "", "username (required)")
	flag.Parse()

	if *username == "" {
		log.Fatal("Please provide a username with -name")
	}

	// Hardcoded relay address (replace with your actual relay)
	const relayAddrStr = "/ip4/138.197.8.191/tcp/4001/p2p/12D3KooWH3C5p4dyffXmFz9sZ3zevcRY9sGPCfoAzvmvvGKtebJf"
	relayMA, err := multiaddr.NewMultiaddr(relayAddrStr)
	if err != nil {
		log.Fatal("Invalid relay address:", err)
	}

	// Create libp2p host
	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"),
		libp2p.EnableRelay(),
	)
	if err != nil {
		log.Fatal("Failed to create host:", err)
	}
	defer h.Close()

	// Create registry and transport
	reg := newMemoryRegistry()
	transport, err := relaytransport.NewRelayTransport(relaytransport.Config{
		Host:           h,
		RelayAddr:      relayMA,
		Username:       *username,
		PeerRegistry:   reg,
		RelayDiscovery: relaytransport.NewDefaultRelayDiscovery(),
	})
	if err != nil {
		log.Fatal("Failed to create transport:", err)
	}

	// Set message handler
	transport.SetMessageHandler(func(fromPeerID, msgType string, payload []byte) {
		var msg string
		if err := json.Unmarshal(payload, &msg); err != nil {
			log.Printf("[%s] Received %s from %s: (raw) %s", *username, msgType, fromPeerID, string(payload))
		} else {
			log.Printf("[%s] Received %s from %s: %s", *username, msgType, fromPeerID, msg)
		}
	})

	// Start transport
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := transport.Start(ctx); err != nil {
		log.Fatal("Failed to start transport:", err)
	}

	log.Printf("%s started. Peer ID: %s", *username, h.ID().String())

	// After a delay, if this is "alice", send a message to the first connected peer
	go func() {
		time.Sleep(10 * time.Second)
		peers := transport.GetConnectedPeers()
		for _, p := range peers {
			if p.ID != h.ID().String() && p.Username != "" {
				// Send a JSON‑encoded string as payload
				payload, _ := json.Marshal(fmt.Sprintf("Hello from %s!", *username))
				log.Printf("%s sending message to %s (%s)", *username, p.Username, p.ID[:12])
				if err := transport.SendMessage(ctx, p.ID, "greeting", payload); err != nil {
					log.Printf("Send failed: %v", err)
				} else {
					log.Println("Message sent")
				}
				break
			}
		}
	}()

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	log.Println("Shutting down...")
	transport.Stop()
}
