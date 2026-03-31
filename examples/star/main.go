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
	"time"

	"github.com/bpc2016/relaytransport"
	"github.com/libp2p/go-libp2p"
	"github.com/multiformats/go-multiaddr"
)

type registry struct {
	mu       sync.RWMutex
	username map[string]string
	isServer map[string]bool
}

func newRegistry() *registry {
	return &registry{
		username: make(map[string]string),
		isServer: make(map[string]bool),
	}
}
func (r *registry) RegisterPeer(peerID, username, address string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.username[peerID] = username
	return nil
}
func (r *registry) SetPeerRole(peerID string, isServer bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.isServer[peerID] = isServer
}
func (r *registry) UsernameByPeerID(peerID string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	u, ok := r.username[peerID]
	return u, ok
}
func (r *registry) IsServer(peerID string) (bool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.isServer[peerID]
	return s, ok
}
func (r *registry) GetKnownPeers() map[string]string        { return nil }
func (r *registry) MergeKnownPeers(map[string]string) error { return nil }

func main() {
	role := flag.String("role", "", "server or client")
	group := flag.String("group", "myapp", "group name (star prefix added automatically)")
	relayFile := flag.String("f", "relay.cfg", "relay address file")
	verbose := flag.Bool("v", false, "verbose")
	flag.Parse()
	if *role != "server" && *role != "client" {
		log.Fatal("role must be 'server' or 'client'")
	}
	isServer := *role == "server"

	data, _ := os.ReadFile(*relayFile)
	relayMA, _ := multiaddr.NewMultiaddr(strings.TrimSpace(string(data)))

	h, _ := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"), libp2p.EnableRelay())
	defer h.Close()

	reg := newRegistry()
	transport, _ := relaytransport.NewRelayTransport(relaytransport.Config{
		Host:         h,
		RelayAddr:    relayMA,
		Username:     *role,
		Group:        "star:" + *group, // add star prefix
		IsServer:     isServer,
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

	log.Printf("%s started", *role)
	if !isServer {
		// client waits for a server
		log.Println("Waiting for server...")
		for i := 0; i < 30; i++ {
			for _, p := range transport.GetConnectedPeers() {
				if svr, ok := reg.IsServer(p.ID); ok && svr {
					log.Printf("Found server %s", p.ID[:12])
					goto done
				}
			}
			time.Sleep(1 * time.Second)
		}
		log.Fatal("No server found")
	done:
	}
	<-ctx.Done()
}
