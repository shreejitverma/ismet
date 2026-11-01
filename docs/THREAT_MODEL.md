# Threat Model

ISMET runs in the customer's process, with the customer's credentials.
It is a library, not a service, so it has no server of its own to attack.
The assets worth protecting are venue credentials, the integrity of the order path, and the integrity of the audit trail.

## Assumptions

- The host and the Python environment are trusted.
  Code with the same OS privileges can read credentials from memory and edit the journal file.
- TLS from the operating system's trust store is sound.
- Venues are not malicious, but they are unreliable: they drop, repeat, reorder, and contradict.

## Threats and controls

| Threat | Control | Evidence | Remaining risk |
|---|---|---|---|
| Credentials leak through logs or errors | `SecretStr` wrapping; hooks receive the path, never the final URL | `tests/unit/test_config.py`, `tests/unit/test_http.py` | The `__cause__` of a transport error holds httpx's request, including a `QueryAuth` URL. Never serialize exception causes to shared logs. Prefer header auth. |
| Credentials sent in clear over FIX | `FixProvider.from_settings` refuses credentials without `tls = true`, with no settings override | `tests/fix/test_provider.py` | A `FixProvider` built directly chooses its own `ssl` setting, which is how a stunnel or VPN connection is configured. |
| Duplicate orders from retries | Non-idempotent requests are sent once; `AmbiguousOutcome` | `tests/unit/test_correctness_regressions.py` | A third-party adapter can violate the capability contract. Trading conformance is the defense. FIX requires one exception: answering a counterparty's resend request retransmits a stored order under its original sequence number with PossDupFlag=Y, which a conforming venue does not execute twice. This happens only within `stale_resend_after`, 60 seconds by default, and older messages are gap-filled. A counterparty that ignores PossDupFlag could execute it twice. |
| Order sent without risk checks | `OrderManager` is the supported path, and risk fails closed | `tests/execution/test_risk.py` | Python cannot stop code calling `provider.place_order` directly. This is an API boundary, not a security boundary. |
| Accidental live trading | Providers default to `LIVE`, which requires `allow_live=True` | `tests/execution/test_manager.py` | A misconfigured adapter that declares `PAPER` while pointing at production. |
| Audit trail edited | Append-only triggers, plus a SHA-256 hash chain checked at startup | `tests/execution/test_journal.py` | Tail truncation fails `verify` through the `<journal>.head` anchor, unless the anchor is deleted too. Rewriting the whole chain and its anchor passes, as does deleting everything, which also forgets used client order ids. Copy `head_hash` to separate storage and pass it to `start(expected_head_hash=...)` to detect those. The chain gives tamper evidence, not tamper prevention. |
| Kill switch bypassed or undone | State is durable and shared across processes. A stale read cannot release it, and a release from another process must name an operator and sit on an intact chain | `tests/execution/test_manager.py` | Someone with write access can write a valid-looking release with a recomputed chain. If the journal is down when the switch is engaged and the process then stops, the engagement is lost: it is retried while running, and `stop()` logs this at critical level. |
| Malicious provider plugin | Entry points load only when named in configuration or in `ismet providers <name>`, and `ismet providers` lists names without importing | `src/ismet/providers/registry.py`, `tests/cli/test_cli.py` | A loaded plugin runs with full process privileges. Install plugins as you would any dependency. |
| Supply chain compromise | Actions pinned to commits, trusted publishing, build attestation, SBOM, `pip-audit`, CodeQL, Dependabot, and a release gated on all of CI and on the tag naming the version. The build job holds no OIDC token: only the attest and publish jobs do, and they install nothing | `.github/workflows/`, `tests/unit/test_workflows.py` | Attestation proves where a wheel was built, not that its build tools were honest: `build`, `hatchling`, and their dependencies are pinned by version and not by hash, and CI installs `pip install .[dev]` from version ranges rather than `uv.lock`, which it only checks for freshness. A compromised tool in the build job could change the artifact that then gets attested. The `v0` tag of `anchore/sbom-action` is a moving major version, pinned here to one commit. Dependency version ranges remain open for library users, who should pin through a lock file. |
| Hostile or corrupt market data | Bounded decimal magnitude, strict models, bounded queues | `tests/unit/test_correctness_regressions.py` | A plausible but wrong price passes validation. `QualityMonitor` flags outliers, and the price collar limits the damage. |
| Resource exhaustion | Bounded stream queues and book buffers, with drops counted | `tests/unit/test_ws.py`, `tests/marketdata/test_book.py` | Choosing the `BLOCK` backpressure policy lets a slow consumer stall a feed. |

## Credential guidance

- Use API keys scoped to trading only.
  Disable withdrawal permission on every exchange key.
- Bind keys to static IPs where the venue offers it.
- Keep keys in the environment or a secret manager rather than in the config file.
- Rotate keys on a schedule, and immediately on any suspicion.

## Not yet implemented

- Built-in Vault, cloud KMS, or OS keychain integration.
  Fetch from those stores and pass the value through settings.
- Rotation of credentials without a restart.
- External anchoring of the journal head.
