# Node Operator Quickstart — Serve Compact Block Filters

Run a DigiByte full node? You can make it directly usable by mobile and other light
(SPV) wallets in about 30 seconds by serving **compact block filters (BIP157/158)**.
The more nodes that do, the healthier and more decentralized DigiByte's light-wallet
infrastructure becomes.

Works on DigiByte Core **8.26** and **9.26**.

**TL;DR:** add `blockfilterindex=basic` + `peerblockfilters=1` to `digibyte.conf`, restart,
verify with `digibyte-cli getindexinfo`.

## Enable it

Add two lines to your `digibyte.conf`:

```
blockfilterindex=basic
peerblockfilters=1
```

Then restart your node:

```
# systemd:
systemctl restart digibyted

# or manually:
digibyte-cli stop        # wait a few seconds for a clean shutdown
digibyted -daemon
```

The block-filter index builds once (a few minutes to ~an hour, depending on hardware
and disk). After that your node advertises `NODE_COMPACT_FILTERS` and serves filters to
light clients automatically.

## Verify

```
digibyte-cli getindexinfo
# → "basic block filter index": { "synced": true, ... }

digibyte-cli getnetworkinfo
# → "localservicesnames" includes "COMPACT_FILTERS"
```

Once both check out, seeders pick your node up automatically (a `getcfheaders`
round-trip confirms it) and light wallets can sync from it.

## `digibyte.conf` location

| OS | Path |
|---|---|
| Linux | `~/.digibyte/digibyte.conf` |
| macOS | `~/Library/Application Support/DigiByte/digibyte.conf` |
| Windows | `%APPDATA%\DigiByte\digibyte.conf` |

## Cost & privacy

- **Cost:** a one-time disk + CPU hit to build the index, then a small footprint that
  grows only with new blocks, plus modest bandwidth serving filters.
- **Privacy:** compact filters are *more* private than the old BIP37 bloom filters — the
  wallet downloads filters and decides locally what to request, instead of handing your
  node a filter of its own addresses.

## Legacy (BIP37 bloom) — optional

Older SPV clients still use bloom filters. To serve them too, also add
`peerbloomfilters=1`. Bloom is off by default and is gradually being retired in favor of
compact filters, so treat it as optional.

---

*Why this matters: the pool of filter-serving DigiByte nodes is small today, so light
wallets lean on just a handful of peers. Every node that turns filters on makes that
network more resilient and less centralized.*
