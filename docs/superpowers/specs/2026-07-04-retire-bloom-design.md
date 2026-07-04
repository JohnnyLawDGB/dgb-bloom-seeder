# Retire BIP37 Bloom — Compact-Filter-Only Seeder

**Date:** 2026-07-04
**Status:** Approved (design)

## Motivation

The DigiByte light-wallet stack is consolidating on compact block filters (BIP 157/158). Bloom (BIP 37) is legacy: off by default in Core, being retired upstream, and — critically — every compact-filter-capable node (8.26+) already runs Dandelion++ (default-on since 7.17.2), so `filter` already implies the privacy property. The wallet (v3.9.0) syncs via BIP 158 with no BIP 37 code. We are in beta, so no meaningful field population depends on bloom.

This change makes the seeder a single-capability service: it discovers, validates, ranks, and serves **compact-filter peers only**. Bloom and the redundant `dandelion` capability are removed.

## Decisions

- **Legacy public surface → soft-alias to filter.** `/api/peers/bloom`, `/api/peers/all`, and `?capability=bloom|dandelion|combined|<anything>` all return filter peers with `"capability":"filter"`. Nothing 404s or 400s. Rationale: zero breakage for stragglers/monitoring; internally only `filter` exists.
- **Drop bloom from `/stats` entirely.** No bloom counts, no NODE_BLOOM observability. Trades a migration-progress metric for maximally clean code.
- **DB schema untouched.** `bloom_validated_at` column + `idx_peers_bloom` index stay dormant (stop being written/read). No prod migration; fresh DBs stay identical to production. Existing bloom-only rows age out via the 24h `prune`.
- **No renames.** `bloom_seeder.db`, the repo name, and the seeder's own user-agent stay as-is.
- **No nginx change.** The API soft-alias makes the existing `/api/peers/bloom` and `/api/peers/all` locations transparently serve filter peers.

## Behavior contract (end state)

`GET /peers` (and every alias/param): up to `api_max_results` filter-validated peers above the inclusion threshold, ranked by composite score DESC. `"capability"` is always `"filter"`. Each peer carries `peer_capability:"filter"`, `services_hex`, and a `capabilities` array (still lists `BLOOM` if the node advertises the bit — honest display).

`GET /stats`: `peers_total`, `peers_filter_validated`, `peers_filter_above_threshold`, `all_peers_known`, `attempts_7d_total`, `last_crawl`, `uptime_seconds`. **Removed:** `peers_bloom_validated`, `peers_bloom_above_threshold`. `peers_total` converges to the filter count as old bloom-only rows age out.

## Per-file changes

**seeder/api.py** — delete `_DANDELION_MIN_VERSION`, `_ua_version`, `_is_dandelion_capable`. `handle_peers`: remove capability validation/branching; always `peers = fetch("filter")`, `peer_capability="filter"`, `response_capability="filter"`; delete the default-fallthrough / bloom / combined / dandelion branches. Keep `SERVICE_FLAG_NAMES` including `(0x004,"BLOOM")` for the display array.

**seeder/crawler.py** — imports: drop `NODE_BLOOM`, `build_filterload`. `handshake_peer`: remove the NODE_BLOOM filterload probe + `bloom_verified`; return only `filter_verified`. `crawl_cycle`: `priority = known_filter | static_set`; remove `known_bloom`, `bloom_found`, bloom attempt-logging, bloom downgrade-clear, bloom upsert, and `bloom_found` from the returned stats.

**seeder/storage.py** — remove `upsert_bloom_peer`. Make `get_ranked_peers`, `get_above_threshold_count`, `get_validated_peer_set`, `clear_validation`, `record_attempt` filter-only (drop the `capability` parameter + bloom branch; operate on `filter_validated_at`). `record_attempt` still writes `capability='filter'` into the retained `peer_attempts.capability` column. `get_stats`: remove the two bloom queries + keys. Schema block and the old `bloom_peers` migration: leave as-is (dormant / guarded).

**seeder/protocol.py** — delete `build_filterload` (dead — nothing constructs bloom filterloads once the crawler stops probing). **Keep `NODE_BLOOM`**: it names a real service bit (0x04) that peers still advertise and that the API still displays in the `capabilities` array, and protocol tests use it.

**seeder.py** — initial-crawl log line → filter-only (drop `bloom_found`).

**docs** — `operator-quickstart.md`: cut the "Legacy (BIP37 bloom) — optional" section; add an explicit "do not enable `peerbloomfilters` (retired)" note. `README.md`: single-capability framing; `/peers` + `/stats` field updates; note legacy aliases transparently serve filter. `wallet-integration.md`, `wallet-bip158-integration.md`: mark bloom endpoints deprecated → use `/api/peers` or `?capability=filter`.

## Testing (TDD)

- **test_api.py** — default → filter (no bloom fallthrough); `?capability=filter` → filter; legacy `bloom` / `dandelion` / `bloom|filter` / garbage → filter with `"capability":"filter"`; `/stats` has no `peers_bloom_*` keys. Remove bloom/combined/dandelion assertions.
- **test_crawler.py** — remove `bloom_verified` handshake + bloom attempt/downgrade/upsert tests; keep/adjust the filter path.
- **test_storage.py** — remove `upsert_bloom_peer` + bloom-branch tests; update filter-only signatures; `get_stats` shape test.
- **test_protocol.py** — remove `build_filterload` / `NODE_BLOOM` tests.

## Out of scope (adjacent, tracked separately)

- Removing `peerbloomfilters=1` from operator/own nodes (services `0x44d`→`0x449`; filter validation unaffected).
- Wallet repo: switch peer query to `?capability=filter` / rely on the new filter-only default.

## Deploy & rollback

Feature branch `feat/retire-bloom`. Backup prod DB (`cp bloom_seeder.db bloom_seeder.db.bak-PRE-RETIRE-BLOOM-<date>`). Deploy the branch to prod, `pm2 restart bloom-seeder`, verify: `/peers`, `/api/peers/bloom` + `/api/peers/all` (both return filter peers), `/stats` (no bloom keys), and that the ~13 filter peers still validate over a crawl cycle. Then merge to master; delete branch local + origin.

Rollback: `git checkout <prior-sha>` + restore DB backup + `pm2 restart`. Schema is unchanged, so the DB backup is belt-and-suspenders.
