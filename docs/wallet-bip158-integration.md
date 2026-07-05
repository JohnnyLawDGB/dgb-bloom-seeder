# Android Wallet — Adopting BIP 158 Compact Filters

A practical, code-level guide for upgrading the [DigiByte Android Wallet](https://github.com/JohnnyLawDGB/digibytewallet-android) to take advantage of the new compact-filter peer seeder. Companion to [`wallet-integration.md`](./wallet-integration.md), which covers the API surface in detail.

This doc focuses on:

- What's now available that wasn't before
- Two viable rollout paths (one minimal, one full)
- Concrete Kotlin patterns for the existing `SyncService.kt` codebase
- A test checklist and a phased deployment plan

---

## What's new

Until now the wallet has been hitting `https://api.digiscope.me/api/peers/bloom`, getting a list of BIP 37 bloom-filter peers, and injecting them into the SPV peer manager. **Bloom support has since been retired seeder-side.** `/api/peers/bloom` still resolves and never errors — but it's now a deprecated alias that serves the same compact-filter (BIP 158) peer list as every other endpoint:

| URL | Returns |
|---|---|
| `https://api.digiscope.me/api/peers` | Block-filter (BIP 158) peers. **Recommended.** |
| `https://api.digiscope.me/api/peers/filter` | Same list, explicit path (equivalent to `?capability=filter`). |
| `https://api.digiscope.me/api/peers/bloom` | **Deprecated** — soft-aliases to the same filter peer list; no bloom peers are served anymore. |
| `https://api.digiscope.me/api/peers/all` | **Deprecated** — soft-aliases to the same filter peer list; no longer a combined filter+bloom set. |
| `https://api.digiscope.me/api/peers/stats` | Filter-validated counts. No `peers_bloom_*` keys. |

Each peer object carries:

- `peer_capability` — always `"filter"` now. Kept in the schema for compatibility, but there's no `"bloom"` value to route on anymore.
- `capabilities` — array of service-flag names (`["NETWORK", "BLOOM", "WITNESS", "COMPACT_FILTERS", "NETWORK_LIMITED"]`); an honest readout of the peer's advertised bits, independent of what the seeder validated.
- `services_hex` — `"0x44d"` etc.; debugging convenience.
- `bloom_validated_at` — legacy field, frozen; the seeder no longer performs bloom validation.
- `filter_validated_at` — unix timestamp of last successful BIP 158 validation; `null` if never validated.

v3.5.38 wallets see these extra fields as unknown JSON members and ignore them — no JSON-parsing break. They will, however, now receive filter peers from `/api/peers/bloom` that they can't sync against over BIP 37, since the seeder no longer has any bloom peers to give them.

---

## Rollout paths

Pick one. Path A is the smallest code change and ships immediately; Path B is the "real" integration.

### Path A — Minimum change (ship today)

**Behavior change:** none — wallet still uses BIP 37 bloom filters internally. Just switch the URL to get a better-ranked peer list (the seeder's new composite scoring is already live behind both `/api/peers/bloom` and `/api/peers`).

**Code change:** one URL constant in `SyncService.kt`. The wallet keeps consuming the JSON the same way it does today.

Pros: zero risk to existing sync paths; immediate uptake of the new ranking algorithm.
Cons: doesn't take advantage of BIP 158 privacy/efficiency gains.

### Path B — Full BIP 158 adoption

**Behavior change:** wallet learns to sync via compact block filters (BIP 158) for any peer flagged `peer_capability=filter`, while keeping a BIP 37 path available for peers flagged `peer_capability=bloom`. In practice every peer sourced from the seeder is `filter` now — the `bloom` branch only matters for peers the wallet gets from elsewhere (hardcoded peer, DNS seeds).

**Code change:** new client-side BIP 158 stack (or a library wrapper), capability-based peer routing in the peer manager, and a small amount of wallet-state plumbing to deduplicate transactions between the two protocols during the transition.

Pros: better privacy (server can't see what addresses the wallet is searching for), less bandwidth per block during sync, no false positives.
Cons: real engineering work. BIP 158 client-side decoding is non-trivial (Golomb-Coded Sets); wallet needs to download the GCS-encoded filter per block and run candidate-address checks locally.

The rest of this doc focuses on Path A first (it's basically a one-day task), then sketches Path B (a multi-sprint effort).

---

## Path A — One-URL change

Find the existing URL constant in `SyncService.kt` (or wherever the wallet defines its seeder endpoint). It looks something like:

```kotlin
private const val SEEDER_URL = "https://api.digiscope.me/api/peers/bloom"
```

Change it to:

```kotlin
private const val SEEDER_URL = "https://api.digiscope.me/api/peers"
```

Now responses are block-filter (BIP 158) peers. The wallet's existing JSON parser keeps working because the top-level `capability` field and the per-peer fields are additive.

Note: bloom support has been retired seeder-side. The old `/api/peers/bloom` URL still resolves and never errors, but it's now a **deprecated alias** that serves this same filter peer list — there's no bloom-only behavior left to opt into. If the wallet's connection code only speaks BIP 37, it can no longer get usable peers from this seeder at all; that's the motivating reason to move to Path B (or at least keep the wallet on its hardcoded/DNS-seed fallback for BIP 37 sync).

### Detecting which capability the response represents

Read the top-level `capability` field on the response if useful for logging. It's always `"filter"` now:

```kotlin
data class PeerListResponse(
    val peers: List<Peer>,
    val count: Int,
    val capability: String,         // always "filter"
    val crawl_age_seconds: Int,
)
```

### Done. Tests:

- Existing sync against v3.5.38 backend should still work (run it).
- Cold launch + first sync should still complete in the same time as before (peers from the new endpoint are at least as good as the old).
- Force a sync with airplane mode → wifi to exercise the fallback chain.

---

## Path B — Full BIP 158 adoption

Three pieces to build:

### Piece 1: HTTP layer

Same as Path A — point at `https://api.digiscope.me/api/peers` (or `/api/peers/filter`, equivalent). The HTTP client doesn't need to branch on capability; every `Peer` the seeder returns is filter-validated. Add `peer_capability` to the parsed model for schema compatibility even though it's now a constant:

```kotlin
data class Peer(
    val ip: String,
    val port: Int,
    val services: Long,
    val services_hex: String,
    val capabilities: List<String>,
    val user_agent: String,
    val peer_capability: String,       // always "filter"
    val bloom_validated_at: Long?,
    val filter_validated_at: Long?,
    val uptime_score: Double,
    val composite_score: Double,
    val attempts_7d: Int,
    val successes_7d: Int,
    val tenure_days: Double,
    val last_seen: Long,
    val first_seen: Long,
    val protocol_version: Int,
)
```

### Piece 2: Capability-based peer routing

In the SPV peer manager (whatever class wraps the `BlockchainService` / `BitcoinJSPV` peer pool), the injection point becomes:

```kotlin
fun injectSeederPeers(response: PeerListResponse) {
    for (peer in response.peers) {
        when (peer.peer_capability) {
            "filter" -> filterStack.addPriorityPeer(peer.ip, peer.port)
            "bloom"  -> bloomStack.addPriorityPeer(peer.ip, peer.port)
            else     -> Log.w("SyncService", "unknown peer_capability: ${peer.peer_capability}")
        }
    }
}
```

Since the seeder now only ever returns `peer_capability = "filter"`, `injectSeederPeers` will only ever populate `filterStack` in practice — the `bloom` branch is dead code for peers sourced from the seeder. Keep it only if `bloomStack` still needs to be fed from elsewhere (hardcoded peer, DNS seeds) for wallets still running a BIP 37 fallback path:

- `bloomStack` — existing BIP 37 code path; no longer fed by the seeder, but may still serve wallet-internal fallback peers.
- `filterStack` — BIP 158 code path; the only stack the seeder feeds now.

Given that, the "which stack to prefer" decision from earlier plans (fall through to `bloomStack` if `filterStack` is empty) no longer has a seeder-side counterpart — the seeder doesn't do that fallthrough anymore either. Treat `bloomStack` purely as a last-resort, wallet-local path.

### Piece 3: BIP 158 client implementation

This is the hard part. The wallet needs to:

1. **Fetch block headers as it does today.** No change to header sync.
2. **Request compact filters for each block** via `getcfilters`. Peer responds with a `cfilter` message containing a Golomb-Coded Set encoded filter for that block.
3. **Decode the GCS filter locally.** This is a bit-level decode (rice-coded delta-encoded sorted hashes). Reference: BIP 158, sections "Building the Filter" and "Querying the Filter".
4. **Check the filter against the wallet's watched addresses.** For each address script, compute its `siphash24(P, k1, k2)` and test whether the result is in the filter. False positives are possible (that's the point of a filter — privacy via overlap with other queries) at the rate of `1/M` per element, where `M = 784931` for the basic filter type. So expect ~1 false positive per 784k addresses per block.
5. **For blocks where the filter says "yes, your addresses might be in here", request the full block** via `getdata`. Process it normally for transaction extraction.

Libraries that can do steps 2–4 for Android/JVM:

- **bitcoinj** — has `BlockchainService` and `Filter` classes; does not currently implement BIP 158 client (mostly server-side support). Some forks have implemented it; check `bitcoinj-cash` and other community forks. May need to vendor a fork or contribute back.
- **NBitcoin (.NET)** — has full BIP 158 client. Useful as a reference implementation if hand-rolling for Kotlin.
- **Custom Kotlin implementation** — straightforward if the existing wallet team has comfort with bit-level encoding. ~300 lines of Kotlin for the GCS decoder + filter-match path. The protocol messages (`getcfilters`, `cfilter`, `getcfheaders`, `cfheaders`) are 50–100 lines on the wire-format side.

**For an MVP**, the wallet can skip step 5 (full block fetch) entirely and just use the filters for "is this address still active?" checks during background polling. That gives a privacy/efficiency win without a full sync overhaul. Adding step 5 unlocks full BIP 158 syncing.

### Wallet UX considerations

- **Sync speed claim:** filter-based sync downloads ~4 KB per block on average vs. ~1 MB per full block — 250x smaller. Sync time for a fresh wallet should drop accordingly, though the wallet still has to fetch full blocks for any block whose filter matches (~1 in ~785k addresses-per-block as noted).
- **Privacy claim:** with BIP 37 bloom filters, every peer the wallet connects to learns which addresses the wallet cares about. With BIP 158, peers send the filter without knowing what the wallet is looking for; the wallet does the matching locally. Don't oversell "anonymous" — the wallet's IP still leaks via the TCP connection — but "the server doesn't know what you searched for" is accurate.
- **Battery / data usage:** the smaller per-block payload offsets the higher number of round-trips (one filter per block). Net is a wash on data; the win is privacy.

---

## Testing checklist

Before shipping any change:

- [ ] **Smoke:** Wallet cold-launches against current production, syncs to tip successfully, sends and receives. (Baseline.)
- [ ] **JSON parsing:** New fields don't break the existing deserializer. Run the existing `SyncServiceTest` if there is one.
- [ ] **Zero-peer response:** Force the seeder to return zero filter peers above threshold (operator changes config); wallet should not crash or show a sync error — it should fall back per the client's own fallback chain (cached peers → hardcoded peer → DNS seeds). There is no server-side bloom fallthrough to rely on anymore.
- [ ] **Capability field (Path B only):** Confirm `peer_capability` is `"filter"` on every peer from the seeder and routes to `filterStack`; confirm `bloomStack` is only ever populated from wallet-local fallback sources, not the seeder response.
- [ ] **Stale cache:** Wallet has a 24-hour-old cached peer list, server is unreachable; wallet uses cached list and continues to function.
- [ ] **API unreachable:** All network paths fail; wallet falls back to hardcoded `digiscope.me:12024` and DNS seeds. Last-resort path works.
- [ ] **Rate limiting:** Backoff is at least 30s on 5xx; wallet doesn't hammer the endpoint.
- [ ] **Privacy (Path B):** Confirm via TCP capture that addresses are never sent in the clear to a filter peer. The wallet should only send `getcfilters` requests; address-side matching happens locally.

---

## Phased deployment plan

A reasonable wallet-team rollout once the engineering work is done:

1. **Internal alpha** (v3.6.0-alpha) — Path A only. Internal team and a handful of testers point at the new endpoint, confirm sync still works, confirm logs show the new fields. Two-week soak.
2. **Public beta** (v3.6.0-beta) — Path A available to public testflight. Confirm no crash rate increase, no sync regressions. Two-week soak.
3. **Path B alpha** (v3.7.0-alpha) — BIP 158 stack enabled behind a feature flag, default off. Internal testing only. Validate sync speed and accuracy.
4. **Path B beta** (v3.7.0-beta) — feature flag default-on for opt-in users. Privacy improvement marketed.
5. **Path B GA** (v3.7.0) — feature flag default-on for everyone. v3.5.x wallets keep using BIP 37 forever, but note they can no longer source bloom peers from this seeder (bloom has been retired seeder-side) — they fall back to their hardcoded peer / DNS seeds. New wallets use BIP 158 by default; users can opt back to BIP 37 (against non-seeder peers) if anything goes sideways.

Coordinate with the seeder operator (this repo) for any backend changes needed to support the rollout (e.g., additional endpoints, different threshold values).

---

## Questions

For seeder/API behavior questions: file in this repo or DM the seeder operator.
For BIP 158 wire-format questions: BIP 158 spec at https://github.com/bitcoin/bips/blob/master/bip-0158.mediawiki — DigiByte uses the same protocol unchanged.
For wallet-side architecture questions: see the digibytewallet-android repo and ping its maintainers.
