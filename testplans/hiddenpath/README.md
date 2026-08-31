# Test of the Sphinx provider discovery

Testground testplan for the evaluation of `bitswap/network/sphinx`.
The test starts 50 bitswap nodes: one initiator, one provider, the rest
form the relay pool.
All nodes connect in a full mesh and bootstrap the DHT; in Sphinx runs
the key exchange fills every relay pool first.
"f" pool nodes are then killed abruptly and the provider announces
"jobs" CIDs; both sets come from the run seed, so runs at the same seed
kill the same nodes and query the same targets.
The initiator then times one discovery job per CID: in vanilla mode it
looks the providers up itself, in Sphinx mode it delegates to proxies
over Sphinx paths and receives the reply through a SURB.
Every instance writes a `result.json`, with the per-job latencies on the
initiator.

## Running the test

To run the test [install](https://docs.testground.ai/getting-started)
Testground and import the testplan:

```bash
testground plan import --from [path-to-testplan] --name hiddenpath
```

`docker:go` builds in a container and sees only pushed code, so `go.mod`
carries no boxo replace; each composition injects one at a pinned fork
version. Record that version's hashes first, otherwise the in-container
build fails with "missing go.sum entry":

```bash
GOWORK=off go mod edit -replace=github.com/ipfs/boxo=github.com/erik9876/boxo@[version]
GOWORK=off GOPROXY=https://proxy.golang.org go mod download github.com/ipfs/boxo
GOWORK=off go mod edit -dropreplace=github.com/ipfs/boxo
```

Generate the compositions with that pin and run one. The pin used for the
thesis measurements is recorded at the top of `gen.py`:

```bash
compositions/gen.py --boxo-pin [version]
testground run composition -f compositions/gen/[cell]-s[seed].toml --collect --wait
```
