# Allowing the digiscope.me bloom seeder to validate Adam's node

This is a one-line `digibyte.conf` change plus a `digibyted` restart. Reason and verification steps below.

## What's happening

The bloom seeder at `https://api.digiscope.me/api/peers` runs on `134.199.198.90`. It tries to crawl Adam's node at `129.212.182.152:12024` every 30 minutes, but the handshake fails with `Connection reset by peer` right after the seeder sends its `version` message.

Diagnosis ruled out:

- ❌ Network/firewall — TCP connect succeeds from digiscope to Adam.
- ❌ Ban (manual or banscore) — `listbanned` is empty on Adam's side.
- ❌ Node not synced — `getblockchaininfo` shows IBD complete; `getindexinfo` shows basic block filter index synced.
- ❌ Config missing flags — `peerbloomfilters=1`, `blockfilterindex=1`, `peerblockfilters=1` all set.

What's left:

- ✅ **Per-IP duplicate-connection rejection.** Adam's node already has an active outbound connection TO digiscope.me's full node (running on the same IP, port 12024). DigiByte Core (a Bitcoin Core derivative) by default rejects a second connection from an IP it already has a peer at — eclipse-attack defense. The seeder's connection is that second connection; the daemon accepts TCP, reads our version (which is why the reset happens *after* version, not before), notices the duplicate IP, and disconnects.

## The fix — one line

Add to `~/.digibyte/digibyte.conf` (on Adam's node):

```ini
whitelist=134.199.198.90/32
```

This whitelists digiscope.me's IP. Whitelisted peers get the default permissions set: `noban`, `bloomfilter`, plus an implicit bypass of the per-IP duplicate-connection rule. Other peers' behavior is unchanged.

If you want to be more explicit about permissions:

```ini
whitelist=noban,bloomfilter,download@134.199.198.90/32
```

(`download` allows getdata requests; `bloomfilter` allows `filterload` regardless of `peerbloomfilters` setting; `noban` exempts from banscore. All of these are already effectively true for this IP today, so the short form `whitelist=134.199.198.90/32` is fine.)

## Restart the daemon

The `whitelist` option is loaded at startup — no live reload. So:

```bash
digibyte-cli stop
# Wait a few seconds for clean shutdown
digibyted -daemon
```

Or if running under systemd:

```bash
systemctl restart digibyted
```

Initial block-load on restart is ~1–2 minutes; the RPC will report `error -28 Loading blocks...` until done.

## Verify the fix worked

After the next bloom seeder crawl (≤30 minutes from restart):

```bash
curl -s https://api.digiscope.me/api/peers/filter | jq '.peers[] | .ip'
```

If Adam's node shows up alongside `174.131.163.123`, the fix worked.

For a faster confirmation, you can also `tail -f ~/.digibyte/debug.log` with `debug=net` enabled and watch for incoming connections from `134.199.198.90`. They should now reach `version` exchange successfully and not be dropped immediately after.

## Or — alternative if you'd rather not whitelist

The behavior we're fighting is per-IP rejection. Two other options:

1. **Raise `maxconnections`.** Currently `maxconnections=40` on Adam's node; at 39/40 saturated. Bumping to `maxconnections=125` (DigiByte default) gives the daemon more slack and may permit the second connection without explicit whitelisting. Costs more memory/sockets but no security tradeoff.

2. **Have the seeder skip already-peered IPs.** Possible seeder-side change: if our local `digibyted` already has IP X as a peer, skip X in the crawl. But that would require the seeder to read peer info from a DigiByte RPC, which it deliberately doesn't do (the seeder is a pure P2P crawler, intentionally decoupled from any specific full node). Mentioning for completeness; not recommended.

## Closing the loop

Once Adam's node accepts the seeder's connection, its uptime score will accumulate the same way `174.131.163.123` did: starts at the Bayesian prior (~0.5) and climbs as successful crawls land. After ~37 hours of clean uptime, the score crosses 0.9. The first crawl after the restart will be at most ~30 minutes away — so by the time you're reading this paragraph, it may already be live.

Reply with the result and I'll confirm from the seeder side.
