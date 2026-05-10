# Android Wallet — Adopting BIP 158 Compact Filters

A practical, code-level guide for upgrading the [DigiByte Android Wallet](https://github.com/JohnnyLawDGB/digibytewallet-android) to take advantage of the new capability-aware peer seeder. Companion to [`wallet-integration.md`](./wallet-integration.md), which covers the API surface in detail.

This doc focuses on:

- What's now available that wasn't before
- Two viable rollout paths (one minimal, one full)
- Concrete Kotlin patterns for the existing `SyncService.kt` codebase
- A test checklist and a phased deployment plan

---

## What's new

Until now the wallet has been hitting `https://api.digiscope.me/api/peers/bloom`, getting a list of BIP 37 bloom-filter peers, and injecting them into the SPV peer manager. The new seeder still serves that endpoint — v3.5.38 wallets in the wild keep working unchanged — but it also serves:

| URL | Returns |
|---|---|
| `https://api.digiscope.me/api/peers` | Block-filter peers, falling through to bloom if no filter peers are above threshold |
| `https://api.digiscope.me/api/peers/filter` | Block-filter peers only (BIP 158) |
| `https://api.digiscope.me/api/peers/all` | Filter peers first, then bloom peers; one combined ranked list |
| `https://api.digiscope.me/api/peers/stats` | Per-capability counts |

Each peer object now carries:

- `peer_capability` — `"filter"` or `"bloom"`. **This is the key field for the new wallet behavior** — tells the wallet which protocol stack this peer expects.
- `capabilities` — array of service-flag names (`["NETWORK", "BLOOM", "WITNESS", "COMPACT_FILTERS", "NETWORK_LIMITED"]`).
- `services_hex` — `"0x44d"` etc.; debugging convenience.
- `bloom_validated_at` / `filter_validated_at` — unix timestamps of last successful validation per capability; `null` if never validated.

v3.5.38 wallets see these extra fields as unknown JSON members and ignore them; no protocol break.

---

## Rollout paths

Pick one. Path A is the smallest code change and ships immediately; Path B is the "real" integration.

### Path A — Minimum change (ship today)

**Behavior change:** none — wallet still uses BIP 37 bloom filters internally. Just switch the URL to get a better-ranked peer list (the seeder's new composite scoring is already live behind both `/api/peers/bloom` and `/api/peers`).

**Code change:** one URL constant in `SyncService.kt`. The wallet keeps consuming the JSON the same way it does today.

Pros: zero risk to existing sync paths; immediate uptake of the new ranking algorithm.
Cons: doesn't take advantage of BIP 158 privacy/efficiency gains.

### Path B — Full BIP 158 adoption

**Behavior change:** wallet learns to sync via compact block filters (BIP 158) for any peer flagged `peer_capability=filter`, while keeping the existing BIP 37 path for `peer_capability=bloom`. Initial sync of a fresh wallet uses filter peers when available, bloom when not.

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

Now responses will include block-filter peers when available, falling through to bloom peers when not. The wallet's existing JSON parser keeps working because the new top-level field (`capability`) and the new per-peer fields are additive.

If you want to be explicit about wanting bloom-only behavior — for example because the wallet's connection code only speaks BIP 37 — keep the existing `/api/peers/bloom` URL. It's not deprecated; it'll continue serving bloom peers indefinitely. The seeder maintainer treats it as a permanent alias.

### Detecting which capability the response represents

Optional but useful — read the top-level `capability` field on the response and log it. This lets the wallet detect when the fallthrough fires (filter peers exhausted, falling back to bloom):

```kotlin
data class PeerListResponse(
    val peers: List<Peer>,
    val count: Int,
    val capability: String,         // "filter", "bloom", or "filter+bloom"
    val crawl_age_seconds: Int,
)
```

Log it on each refresh; it'll be `"filter"` once Adam's and digiscope's nodes are both serving filters, `"bloom"` until then.

### Done. Tests:

- Existing sync against v3.5.38 backend should still work (run it).
- Cold launch + first sync should still complete in the same time as before (peers from the new endpoint are at least as good as the old).
- Force a sync with airplane mode → wifi to exercise the fallback chain.

---

## Path B — Full BIP 158 adoption

Three pieces to build:

### Piece 1: HTTP layer

Same as Path A — point at `https://api.digiscope.me/api/peers` (or `?capability=filter|bloom` for the combined ranked list). The HTTP client doesn't care which capability the peer is; it just hands the wallet a list of `Peer` objects with the new fields. Add `peer_capability` to the parsed model:

```kotlin
data class Peer(
    val ip: String,
    val port: Int,
    val services: Long,
    val services_hex: String,
    val capabilities: List<String>,
    val user_agent: String,
    val peer_capability: String,       // "filter" or "bloom"
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

The wallet runs two stacks simultaneously during the transition:

- `bloomStack` — existing BIP 37 code path; serves backwards-compat sync for peers without filter support.
- `filterStack` — new BIP 158 code path; preferred for new syncs.

The wallet decides which stack to use for its actual block-data requests based on which has the best-ranked available peer. A reasonable starting rule: if `filterStack` has at least one connected peer above threshold, use it; otherwise fall through to `bloomStack`. Same fallthrough logic the seeder does server-side.

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
- [ ] **Fallthrough:** Force the seeder to return zero filter peers (operator changes config); wallet should fall through to bloom peers via the default endpoint, not crash, not show a sync error.
- [ ] **Capability routing (Path B only):** Inject a list of mixed-capability peers; verify each peer ends up on the right stack via logging.
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
5. **Path B GA** (v3.7.0) — feature flag default-on for everyone. v3.5.x wallets keep using BIP 37 forever; new wallets use BIP 158 by default; users can opt back to BIP 37 if anything goes sideways.

Coordinate with the seeder operator (this repo) for any backend changes needed to support the rollout (e.g., additional endpoints, different threshold values).

---

## Questions

For seeder/API behavior questions: file in this repo or DM the seeder operator.
For BIP 158 wire-format questions: BIP 158 spec at https://github.com/bitcoin/bips/blob/master/bip-0158.mediawiki — DigiByte uses the same protocol unchanged.
For wallet-side architecture questions: see the digibytewallet-android repo and ping its maintainers.
