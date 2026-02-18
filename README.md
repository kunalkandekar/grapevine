GRAPEVINE
=========

Overlay Multicast

# Introduction

A service / library for peer-to-peer message dissemtination across a
self-organizing overlay network tree. Message delivery reliability is NOT guaranteed and is
expected to be implemented at a higher layer.

You could think of it as a gossip protocol-based dissemination that is much more efficient
(because tree) but less robust (much fewer connections also because tree, so lost messages
during partitions are very possible.)

NOTE: This was actually written in 2015, only getting around to releasing it now, so expect
some anachronisms. Also I'm literally touching this after a decade+ so I expect lots of
broken things and bitrot. For now this is just an initial commit to share the code.

This project was a spin-off of a larger distributed systems project and so is somewhat
ad-hoc designed with somewhat niche requirements, and in its current state may not be ready
for widespread use.


## Naming

I know, I know, there are, like, a 100 other P2P dissemination projects and related research
papers called "Grapevine." This was called castaway in earlier revisions, but I decided
to rename it to something relevant starting with "G" because it's in Go, and I couldn't come
up with anything else at the time. Sue me.


# Salient Points

* Nodes organize themselves into a overlay distribution tree rooted at the
  controller node.
* Controller is used to bootstrap the overlay, as all followers join the overlay by
  connecting to it first


# Caveats

* Assumes low churn, i.e. long lasting sessions, peers don't drop and rejoin
  frequently.
* Unordered but (mostly) reliable message delivery.
* No log recovery - Nodes that drop off and rejoin will not recover any messges
  transmitted in the meantime. (Not needed for current use case.)
* Centralized - controller is a single point of failure. But also lightweight, so
  robustness could be achieved easily.
* Root failover can be mildly disruptive - detecting a node is down would, in the
  worse case, take as long as the time configured for the heartbeat to detect this.
  However we expect any node downtime to be an infrequent occurrence for our use
  case.


# Usage
-----
Build:

```bash
go build -o grapevine ./grapevine
```

Start controller node:

```bash
./grapevine -n <node-name> -i 127.0.0.1 -p 4000
```

Start follower nodes:

```bash
./grapevine -n <node-name> -i 127.0.0.1 -p 4001 -j 127.0.0.1:4000

./grapevine -n <node-name> -i 127.0.0.1 -p 4002 -j 127.0.0.1:4000

...

etc.
```

Start sample console app: Starts a REPL where commands can be issued to simulate the
network locally using in-memory nodes, and trigger various events on it.

```bash
./grapevine -app test

cmd: r ../test/s0.run # Runs instructions in the "run" file sequentially.
```

# Testing
-----
Run all tests:

```bash
go test ./...
```

Run only smoke tests:

```bash
go test ./grapevine -run 'TestDoGetAndReadResponse|TestResetDebugFlag' -v
```

Run the run-file integration test (exercises console `r <script>` path):

```bash
go test ./grapevine -run TestConsoleRunScriptIntegration -v
```

Skip integration test (short mode):

```bash
go test ./... -short
```
