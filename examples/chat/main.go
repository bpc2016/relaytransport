package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/bpc2016/relaytransport"
	"github.com/libp2p/go-libp2p"
	"github.com/multiformats/go-multiaddr"
)

type registry struct {
	mu    sync.RWMutex
	peers map[string]string
}

func newRegistry() *registry { return &registry{peers: make(map[string]string)} }
func (r *registry) RegisterPeer(peerID, username, address string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers[peerID] = username
	return nil
}
func (r *registry) UsernameByPeerID(peerID string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	u, ok := r.peers[peerID]
	return u, ok
}
func (r *registry) GetKnownPeers() map[string]string        { return nil }
func (r *registry) MergeKnownPeers(map[string]string) error { return nil }

func main() {
	name := flag.String("name", "", "your name")
	group := flag.String("group", "chat", "group name")
	relayFile := flag.String("f", "relay.cfg", "relay address file")
	verbose := flag.Bool("v", false, "verbose")
	flag.Parse()
	if *name == "" {
		log.Fatal("provide -name")
	}

	data, _ := os.ReadFile(*relayFile)
	relayMA, _ := multiaddr.NewMultiaddr(strings.TrimSpace(string(data)))

	h, _ := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"), libp2p.EnableRelay())
	defer h.Close()

	reg := newRegistry()
	transport, _ := relaytransport.NewRelayTransport(relaytransport.Config{
		Host:         h,
		RelayAddr:    relayMA,
		Username:     *name,
		Group:        *group,
		IsServer:     true, // mesh
		PeerRegistry: reg,
		Verbose:      *verbose,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle incoming messages
	transport.SetMessageHandler(func(from, typ string, payload []byte) {
		if typ == "chat" {
			var msg string
			if err := json.Unmarshal(payload, &msg); err == nil {
				if uname, ok := reg.UsernameByPeerID(from); ok {
					log.Printf("[%s] %s", uname, msg)
				} else {
					log.Printf("[%s] %s", from[:12], msg)
				}
			}
		}
	})

	// Signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() { <-sigCh; cancel() }()

	if err := transport.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer transport.Stop()

	log.Printf("%s ready. Type messages and press Enter. Use '/quit' to exit.", *name)

	// Input loop
	scanner := bufio.NewScanner(os.Stdin)
	go func() {
		for scanner.Scan() {
			text := scanner.Text()
			if text == "/quit" {
				cancel()
				return
			}
			// Send to all connected peers
			payload, _ := json.Marshal(text)
			for _, p := range transport.GetConnectedPeers() {
				if err := transport.SendMessage(ctx, p.ID, "chat", payload); err != nil {
					log.Printf("Failed to send to %s: %v", p.ID[:12], err)
				}
			}
		}
	}()

	<-ctx.Done()
	log.Println("Exiting")
}
