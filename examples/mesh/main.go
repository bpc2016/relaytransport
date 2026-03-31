package main

import (
	"context"
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
func (r *registry) GetKnownPeers() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	copy := make(map[string]string)
	for k, v := range r.peers {
		copy[k] = v
	}
	return copy
}
func (r *registry) MergeKnownPeers(peers map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, v := range peers {
		if _, ok := r.peers[k]; !ok {
			r.peers[k] = v
		}
	}
	return nil
}

func main() {
	name := flag.String("name", "", "node name")
	group := flag.String("group", "mesh", "group name (no star:)")
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
		IsServer:     true, // mesh: all nodes are servers
		PeerRegistry: reg,
		Verbose:      *verbose,
	})

	transport.OnPeerConnected(func(peerID string) {
		if u, ok := reg.UsernameByPeerID(peerID); ok {
			log.Printf("✅ Connected to %s (%s)", u, peerID[:12])
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() { <-sigCh; cancel() }()

	if err := transport.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer transport.Stop()

	log.Printf("%s is ready!", *name)
	<-ctx.Done()
}
