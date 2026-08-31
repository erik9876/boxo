module github.com/ipfs/boxo/testplans/hiddenpath

go 1.26.2

// This go.mod deliberately carries no boxo replace, so the docker:go
// builder can inject one per composition (see README.md).
//
// The sdk-go replace is the known race fix by hannahhoward, a panic on a
// closed channel in the sync service client; the websocket replace keeps
// the dead nhooyr.io vanity import resolvable.
replace (
	github.com/testground/sdk-go => github.com/hannahhoward/sdk-go v0.3.1-0.20220106065751-1280c9501986
	nhooyr.io/websocket => github.com/coder/websocket v1.8.7
)

require (
	github.com/ipfs/boxo v0.41.0
	github.com/ipfs/go-cid v0.6.1
	github.com/ipfs/go-datastore v0.9.1
	github.com/libp2p/go-libp2p v0.48.0
	github.com/libp2p/go-libp2p-kad-dht v0.40.0
	github.com/multiformats/go-multihash v0.2.3
	github.com/testground/sdk-go v0.3.0
)

require (
	codeberg.org/vula/highctidh v1.0.2025051200 // indirect
	filippo.io/bigmod v0.1.1-0.20260103110540-f8a47775ebe5 // indirect
	filippo.io/keygen v0.0.0-20260114151900-8e2790ea4c5b // indirect
	filippo.io/mlkem768 v0.0.0-20260214141301-2e7bebc7d88d // indirect
	github.com/BurntSushi/toml v1.6.0 // indirect
	github.com/avast/retry-go v2.6.0+incompatible // indirect
	github.com/benbjohnson/clock v1.3.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bytedance/sonic v1.12.2 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cskr/pubsub v1.0.2 // indirect
	github.com/davidlazar/go-crypto v0.0.0-20200604182044-b73af7476f6c // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.4.1 // indirect
	github.com/dunglas/httpsfv v1.1.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/filecoin-project/go-clock v0.1.0 // indirect
	github.com/flynn/noise v1.1.0 // indirect
	github.com/fxamacker/cbor/v2 v2.9.1 // indirect
	github.com/gammazero/chanqueue v1.1.2 // indirect
	github.com/gammazero/deque v1.2.1 // indirect
	github.com/gin-gonic/gin v1.10.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-playground/validator/v10 v10.22.0 // indirect
	github.com/goccy/go-json v0.10.3 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/gopacket v1.1.19 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.1.0 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/huin/goupnp v1.3.0 // indirect
	github.com/influxdata/influxdb1-client v0.0.0-20200515024757-02f0bf5dbca3 // indirect
	github.com/ipfs/bbloom v0.1.0 // indirect
	github.com/ipfs/go-block-format v0.2.3 // indirect
	github.com/ipfs/go-cidutil v0.1.1 // indirect
	github.com/ipfs/go-dsqueue v0.2.0 // indirect
	github.com/ipfs/go-ipfs-pq v0.0.4 // indirect
	github.com/ipfs/go-ipld-format v0.6.3 // indirect
	github.com/ipfs/go-log/v2 v2.9.2 // indirect
	github.com/ipfs/go-metrics-interface v0.3.0 // indirect
	github.com/ipfs/go-peertaskqueue v0.8.3 // indirect
	github.com/ipld/go-ipld-prime v0.24.0 // indirect
	github.com/jackpal/go-nat-pmp v1.0.2 // indirect
	github.com/jbenet/go-temp-err-catcher v0.1.0 // indirect
	github.com/katzenpost/chacha20 v0.0.1 // indirect
	github.com/katzenpost/circl v1.3.8-0.20260413165442-e2d217fd59f5 // indirect
	github.com/katzenpost/hpqc v0.0.84 // indirect
	github.com/katzenpost/katzenpost v0.0.88 // indirect
	github.com/katzenpost/sntrup4591761 v0.0.0-20231024131303-8755eb1986b8 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/koron/go-ssdp v0.0.6 // indirect
	github.com/libp2p/go-buffer-pool v0.1.0 // indirect
	github.com/libp2p/go-cidranger v1.1.0 // indirect
	github.com/libp2p/go-flow-metrics v0.3.0 // indirect
	github.com/libp2p/go-libp2p-asn-util v0.4.1 // indirect
	github.com/libp2p/go-libp2p-kbucket v0.8.0 // indirect
	github.com/libp2p/go-libp2p-record v0.3.1 // indirect
	github.com/libp2p/go-libp2p-routing-helpers v0.7.5 // indirect
	github.com/libp2p/go-msgio v0.3.0 // indirect
	github.com/libp2p/go-netroute v0.4.0 // indirect
	github.com/libp2p/go-reuseport v0.4.0 // indirect
	github.com/libp2p/go-yamux/v5 v5.0.1 // indirect
	github.com/marten-seemann/tcp v0.0.0-20210406111302-dfbc87cc63fd // indirect
	github.com/mattn/go-isatty v0.0.22 // indirect
	github.com/mattn/go-pointer v0.0.1 // indirect
	github.com/mikioh/tcpinfo v0.0.0-20190314235526-30a79bb1804b // indirect
	github.com/mikioh/tcpopt v0.0.0-20190314235656-172688c1accc // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/mr-tron/base58 v1.3.0 // indirect
	github.com/multiformats/go-base32 v0.1.0 // indirect
	github.com/multiformats/go-base36 v0.2.0 // indirect
	github.com/multiformats/go-multiaddr v0.16.1 // indirect
	github.com/multiformats/go-multiaddr-dns v0.5.0 // indirect
	github.com/multiformats/go-multiaddr-fmt v0.1.0 // indirect
	github.com/multiformats/go-multibase v0.3.0 // indirect
	github.com/multiformats/go-multicodec v0.10.0 // indirect
	github.com/multiformats/go-multistream v0.6.1 // indirect
	github.com/multiformats/go-varint v0.1.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/pelletier/go-toml/v2 v2.2.3 // indirect
	github.com/pion/datachannel v1.5.10 // indirect
	github.com/pion/dtls/v3 v3.1.2 // indirect
	github.com/pion/ice/v4 v4.0.10 // indirect
	github.com/pion/interceptor v0.1.40 // indirect
	github.com/pion/logging v0.2.4 // indirect
	github.com/pion/mdns/v2 v2.0.7 // indirect
	github.com/pion/randutil v0.1.0 // indirect
	github.com/pion/rtcp v1.2.16 // indirect
	github.com/pion/rtp v1.8.19 // indirect
	github.com/pion/sctp v1.8.39 // indirect
	github.com/pion/sdp/v3 v3.0.18 // indirect
	github.com/pion/srtp/v3 v3.0.6 // indirect
	github.com/pion/stun/v3 v3.1.1 // indirect
	github.com/pion/transport/v3 v3.0.7 // indirect
	github.com/pion/transport/v4 v4.0.1 // indirect
	github.com/pion/turn/v4 v4.0.2 // indirect
	github.com/pion/webrtc/v4 v4.1.2 // indirect
	github.com/polydawn/refmt v0.90.0 // indirect
	github.com/prometheus/client_golang v1.23.2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.67.5 // indirect
	github.com/prometheus/procfs v0.20.1 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.59.0 // indirect
	github.com/quic-go/webtransport-go v0.10.0 // indirect
	github.com/raulk/clock v1.1.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0 // indirect
	github.com/shurlinet/go-hqc v0.1.1 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/testground/sync-service v0.1.0 // indirect
	github.com/testground/testground v0.5.3 // indirect
	github.com/whyrusleeping/go-keyspace v0.0.0-20160322163242-5b898ac5add1 // indirect
	github.com/wlynxg/anet v0.0.5 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	gitlab.com/yawning/aez.git v0.0.0-20211027044916-e49e68abd344 // indirect
	gitlab.com/yawning/bsaes.git v0.0.0-20190805113838-0a714cd429ec // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.uber.org/dig v1.19.0 // indirect
	go.uber.org/fx v1.24.0 // indirect
	go.uber.org/mock v0.5.2 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	golang.org/x/arch v0.10.0 // indirect
	golang.org/x/crypto v0.51.0 // indirect
	golang.org/x/exp v0.0.0-20260603202125-055de637280b // indirect
	golang.org/x/mod v0.36.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/telemetry v0.0.0-20260508192327-42602be52be6 // indirect
	golang.org/x/text v0.37.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	golang.org/x/tools v0.45.0 // indirect
	gonum.org/v1/gonum v0.17.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	lukechampine.com/blake3 v1.4.1 // indirect
	nhooyr.io/websocket v1.8.6 // indirect
)
