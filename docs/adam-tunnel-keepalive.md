# Reverse-tunnel keepalive on the relay sshd (applied 2026-07-03)

CGNAT'd filter nodes reach the seeder through reverse SSH tunnels to a relay:

```
polloloco box:  ssh -R 0.0.0.0:13024:localhost:12024  root@digiscope.me         (134.199.198.90)
johnnylaw box:  ssh -R 0.0.0.0:13025:localhost:22024  root@129.212.182.152      (adam's VPS)
```

The seeder crawls them as `134.199.198.90:13024` and `129.212.182.152:13025`.

## The problem

When the home link blips or a node restarts (e.g. the 9.26.4 upgrade), the tunnel
drops. On reconnect the loop logged:

```
Error: remote port forwarding failed for listen port 13025
connect_to localhost port 22024: failed.
```

**Cause:** relay `sshd` was not reaping the dead tunnel session fast enough, so the
old session kept holding the forward port and the reconnecting `ssh -R` couldn't
rebind until it timed out.

- `digiscope.me`: `ClientAliveInterval 0` (never probes) → dead session lingered
  until TCP timeout.
- Adam's VPS: `ClientAliveInterval 120` in the main `sshd_config` → ~360s
  (6 min) reap window.

Not a node/config problem — both nodes advertise `0x44d`
(`NODE_BLOOM` + `NODE_COMPACT_FILTERS`) and `GatewayPorts clientspecified` was
already set on both relays (else the `1302x` ports couldn't bind `0.0.0.0`).

## The fix (applied to both relays)

A drop-in appended to `/etc/ssh/sshd_config.d/99-dgb-tunnel.conf`:

```ini
ClientAliveInterval 15
ClientAliveCountMax 3
```

`15 x 3 = 45s` reap window — just under the tunnel client's ~55s reconnect cycle
(`ServerAliveInterval 15 x ServerAliveCountMax 3 = 45s` give-up + `sleep 10`), so
the port is free by the time the client re-dials. Result: after a blip the tunnel
rebinds in ~10–15s instead of minutes.

On Adam's VPS the drop-in overrides the main-file `ClientAliveInterval 120`,
because the `Include /etc/ssh/sshd_config.d/*.conf` at line 12 is parsed before
line 125 and sshd uses the first obtained value. Confirmed with `sshd -T`.

### Procedure used (safe: drop-in, validate, reload — never restart)

```bash
CONF=/etc/ssh/sshd_config.d/99-dgb-tunnel.conf
cp -a "$CONF" "${CONF}.bak.$(date +%Y%m%d-%H%M%S)"
cat >> "$CONF" <<'EOF'
ClientAliveInterval 15
ClientAliveCountMax 3
EOF
sshd -t && systemctl reload ssh          # reload only if sshd -t passes
sshd -T | grep clientaliveinterval       # must read: clientaliveinterval 15
```

### Backups (for rollback)

- `digiscope.me`  : `/etc/ssh/sshd_config.d/99-dgb-tunnel.conf.bak.20260703-012619`
- adam's VPS      : `/etc/ssh/sshd_config.d/99-dgb-tunnel.conf.bak.20260703-013202`

Rollback = restore the backup (or delete the two appended lines) and
`systemctl reload ssh`. Nothing else on either box is touched.

## Access note

Both relays are reachable from the panopticon host with the `~/.ssh/digiscope_deploy`
key as `root` (`digiscope.me` and `129.212.182.152`). The forwarding-only
`~/.ssh/dgb_tunnel` key is for the tunnels themselves and grants no shell.
