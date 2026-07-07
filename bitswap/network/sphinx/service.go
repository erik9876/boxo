package sphinx

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
)

const DefaultKeyTTL = time.Hour

type ServiceConfig struct {
	Job JobConfig
	// Proxy.Discoverer may stay nil: discovery then runs against the
	// ContentRouting given to NewService. Non-nil = test seam
	Proxy ProxyConfig
	// zero means DefaultKeyTTL
	KeyTTL time.Duration
	// PeerRouting, when set, precedes every packet send with a FindPeer
	// for the next hop, known or not, across all roles. Uniform lookup
	// traffic: no hop reveals through its behavior whether it had to
	// resolve its successor. Nil dials straight from the peerstore
	PeerRouting PeerFinder
}

// Service is one full Sphinx node on a libp2p host: key layer, packet
// core and both job roles behind one constructor with a defined shutdown
// order. Every node runs relay, proxy and initiator; the relay handler is
// also how an initiator receives its own SURB replies. No own goroutines
type Service struct {
	km    *KeyManager
	pool  *KeyStore
	surbs *SURBStore
	kx    *KeyExchange
	tr    *Transport
	proxy *Proxy
	jobs  *JobManager

	closeOnce sync.Once
}

// NewService brings the stack up on h. identity must be h's own Ed25519
// key; rt may be nil only when cfg.Proxy.Discoverer is set. After
// construction the node relays and proxies; the initiator role needs a
// filled pool first (PoolStatus)
func NewService(h host.Host, identity crypto.PrivKey, rt routing.ContentRouting, cfg ServiceConfig) (*Service, error) {
	if h == nil || identity == nil {
		return nil, errors.New("sphinx service needs a host and an identity key")
	}
	pid, err := peer.IDFromPrivateKey(identity)
	if err != nil {
		return nil, fmt.Errorf("deriving peer id from identity key: %w", err)
	}
	if pid != h.ID() {
		return nil, fmt.Errorf("identity key derives peer %s, host is %s", pid, h.ID())
	}
	if cfg.KeyTTL == 0 {
		cfg.KeyTTL = DefaultKeyTTL
	}
	if cfg.Proxy.Discoverer == nil {
		if rt == nil {
			return nil, errors.New("sphinx service needs a content routing or an explicit proxy discoverer")
		}
		cfg.Proxy.Discoverer = ContentRoutingDiscoverer{Routing: rt}
	}

	km, err := NewKeyManager(identity, cfg.KeyTTL)
	if err != nil {
		return nil, err
	}
	surbs := NewSURBStore()
	pool := NewKeyStore()

	// mux first; the roles land in it later because they send through the
	// transport
	mux := &DeliveryMux{}
	relay, err := NewRelay(km, surbs, mux)
	if err != nil {
		return nil, err
	}
	tr := NewTransport(h, relay, cfg.PeerRouting)

	proxy, err := NewProxy(tr, cfg.Proxy)
	if err != nil {
		_ = tr.Close()
		return nil, err
	}
	jobs, err := NewJobManager(tr, km, pool, surbs, cfg.Job)
	if err != nil {
		_ = tr.Close()
		return nil, err
	}
	mux.SetProxy(proxy)
	mux.SetJobs(jobs)

	// key exchange last: from here on peers may send packets, and the
	// relay handler above is already serving
	kx := NewKeyExchange(h, km, pool)

	return &Service{
		km:    km,
		pool:  pool,
		surbs: surbs,
		kx:    kx,
		tr:    tr,
		proxy: proxy,
		jobs:  jobs,
	}, nil
}

// Close is idempotent and always returns nil. Order matters: key exchange
// first (stop advertising), then job manager (settle pending jobs while
// the transport can still deliver a late reply), then transport. In-flight
// work is not awaited; late proxy reply sends fail as benign drops
func (s *Service) Close() error {
	s.closeOnce.Do(func() {
		_ = s.kx.Close()
		s.jobs.Close()
		_ = s.tr.Close()
	})
	return nil
}

// Jobs is the seam the ProviderFinder adapter plugs into
func (s *Service) Jobs() *JobManager { return s.jobs }

func (s *Service) JobMetrics() JobMetricsSnapshot { return s.jobs.Metrics() }

func (s *Service) ProxyMetrics() ProxyMetricsSnapshot { return s.proxy.Metrics() }

func (s *Service) TransportMetrics() TransportMetricsSnapshot { return s.tr.Metrics() }

// PoolStatus reports the live pool against one job's demand; StartJob can
// only succeed while live >= need
func (s *Service) PoolStatus() (live, need int) {
	s.pool.EvictExpired()
	return s.pool.Len(), s.jobs.sampleSize()
}
