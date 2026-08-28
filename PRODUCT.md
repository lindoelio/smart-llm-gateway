# PRODUCT.md

## 1. Executive Summary & Vision

**Smart LLM Gateway** is a high-performance, developer-centric AI gateway designed to standardize, secure, and abstract multi-provider AI infrastructure. Its first public interface is an OpenAI-compatible API (by default at `localhost:8080`), with planned inbound support for Anthropic Messages-compatible, Google Gemini-compatible, and Alibaba Cloud Model Studio/DashScope APIs. Test harnesses, IDE extensions, applications, and autonomous agents can use stable client-facing protocols and model names while routing remains independent.

The gateway separates the model name requested by a client from the provider and upstream model that ultimately serve the request. A stable logical model such as `deepseek-v4-flash-0731` may be fulfilled by DeepInfra, OpenRouter, DeepSeek, or another compatible provider without requiring any client change. Ordered provider routes, model fallbacks, circuit breakers, cost tracking, and quota-aware switching keep work running with minimal interruption.

Configuration can be stored in either SQLite or PostgreSQL. SQLite supports a self-contained local installation, while PostgreSQL supports shared deployments across local machines, VMs, and other hosts. Neon is a recommended PostgreSQL hosting option, not a product dependency.

The architectural direction is a pragmatic ports-and-adapters design. Inbound protocol adapters, outbound provider/protocol adapters, storage adapters, and provider-native caching capabilities surround a protocol-neutral routing core. This prevents client protocol, provider, and database choices from becoming coupled.

The project will be distributed as open-source software at `github.com/lindoelio/smart-llm-gateway`, including an installer for the gateway CLI.

---

## 2. Core Value Propositions

* **Single Configuration Endpoint**: One gateway key and one URL for all clients, frameworks, and agents.
* **Multi-Protocol Edge**: Start with OpenAI compatibility and add Anthropic, Gemini, and DashScope-compatible interfaces without rewriting routing rules or provider integrations.
* **Stable Model Identity**: Client-facing model names remain fixed while upstream providers and physical model identifiers can change transparently.
* **Model-Agnostic Grades**: Optional capability grades such as `small`, `standard`, and `frontier` allow consumers to request a performance class instead of a specific model.
* **Multi-Provider Resilience**: Ordered provider routes, provider-level circuit breakers, and cross-model fallbacks reduce disruption caused by outages, throttling, or degraded endpoints.
* **Authoritative Quota Awareness**: When a provider exposes plan allowances, usage, remaining balance, or reset times, its adapter synchronizes those facts so the router can move traffic proactively without user-managed counters.
* **Quota-Aware Circuit Breaking**: When authoritative quota data is unavailable, provider-specific error classification opens the appropriate circuit and immediately attempts a configured fallback.
* **Cost and Consumption Visibility**: Every request records provider-reported billable units and charges when available. Missing provider billing data remains `unknown`; the gateway does not invent an estimated charge.
* **Dynamic Presets and Scheduling**: Entire routing topologies can be switched manually or through scheduled windows without changing clients.
* **Portable Storage**: SQLite provides a zero-infrastructure local mode; PostgreSQL provides a shared remote mode, including deployment on Neon.
* **Low-Latency Routing**: Concurrent in-memory caches keep authentication, configuration, and routing overhead out of the inference critical path.
* **Provider-Native Cache Optimization**: Use prompt or context caching exposed by upstream providers to reduce repeated input processing, cost, and latency while preserving provider-reported cache accounting.
* **Agentic-First and Open-Source**: A headless CLI and reproducible installer make the gateway easy to operate manually or through coding agents.

---

## 3. System Architecture & Technical Specifications

```text
  OpenAI clients   Anthropic clients   Gemini clients   DashScope clients
         \                |                 |                 /
          +---------------+-----------------+----------------+
                                  |
                                  v
                    +-----------------------------+
                    | Inbound Protocol Adapters   |
                    +-----------------------------+
                                  |
                                  v
       +----------------------------------------------------------+
       | Canonical Inference Core                                 |
       | auth -> preset/schedule -> route health -> fallback       |
       +----------------------------------------------------------+
                 |                         |
                 | storage port            | inference/cache capabilities
                 v                         v
       +-------------------+      +-------------------------------+
       | SQLite/PostgreSQL |      | Provider + Protocol Adapters  |
       +-------------------+      +-------------------------------+
                                              |
                         +--------------------+--------------------+
                         v                    v                    v
                    OpenRouter            Anthropic        Google/Alibaba/Others
```

### Technical Specs

* **Language/Runtime**: Rust on the stable toolchain with an explicit minimum supported Rust version (MSRV), asynchronous networking, and single-binary distribution.
* **Workspace Structure**: A virtual Cargo Workspace organizes the system into private, non-published crates for the domain, ports, application services, adapters, runtime, and executable composition root. Workspace crates share dependency, lint, build-profile, and lockfile policy.
* **Input Protocols**:
  * **Initial**: OpenAI-compatible HTTP API, initially including `/v1/chat/completions`.
  * **Planned**: Anthropic Messages-compatible, Google Gemini `generateContent`-compatible, and Alibaba Cloud Model Studio/DashScope-native interfaces.
* **Architecture Style**: Pragmatic ports and adapters. Traits and crate boundaries are introduced at real variability boundaries—client protocols, provider transports, persistence, secret resolution, and provider-native caching—not around every internal function.
* **Canonical Inference Model**: Protocol adapters translate requests, streaming events, usage, and errors into a shared core representation. The representation contains common typed concepts plus namespaced extensions or lossless passthrough data so provider-specific features are not forced into a lowest-common-denominator schema.
* **Capability Negotiation**: Each provider route declares supported modalities, tools, streaming behavior, structured output, reasoning controls, and cache modes. Unsupported explicit features are reported clearly rather than silently dropped.
* **Storage Backends**:
  * **SQLite** for a self-contained local gateway using a single database file.
  * **PostgreSQL** for shared deployments. Neon is supported as a standard PostgreSQL host with SSL.
* **Database Portability**: Schema migrations and repository adapters must be tested against both SQLite and PostgreSQL. Product behavior must not depend on Neon-specific features.
* **Control-Plane Caching**: Concurrent local memory caches hold validated gateway keys, active configuration, and translated routing maps for up to 30 seconds. Provider-account and provider-route runtime state must propagate faster than the general configuration TTL.
* **LLM Cache Optimization**: Provider-native prompt/context caching is a separate inference capability. The gateway translates cache intent where safe, tracks provider-reported cache reads/writes or cached tokens, and never assumes a cache entry is portable across providers, accounts, routes, or models.
* **Last-Known-Good State**: A validated routing snapshot is retained locally so a temporary remote PostgreSQL outage does not immediately stop inference traffic.
* **Secret Handling**: Provider credentials are referenced by configuration but supplied through the gateway execution environment or another secret store; they are never returned to clients.
* **Provider Control-Plane Sync**: Provider adapters may use quota, subscription, usage, billing, balance, and response-header APIs. Synchronization runs in the background, at startup, and on demand when authoritative data is stale or close to a routing threshold. Providers without such APIs use error-driven circuit breaking instead of inferred quota counters.

---

## 4. Key Features & Domain Entities

### A. Performance Grades and Logical Models

Clients may request either a grade or a stable logical model name.

Example grades:

* `small`: Fast, high-throughput inline autocomplete.
* `standard`: Default code explanation and multi-file reasoning.
* `frontier`: Advanced multi-step planning and deep architectural redesigns.
* `small_with_vision`: Rapid visual asset prototyping.
* `standard_with_vision_audio`: Deep multimodal engineering and auditing.

A preset maps each grade to a logical model. A logical model has a stable public name, such as `deepseek-v4-flash-0731`, which does not reveal or lock the client to the provider that serves it.

Resolution follows this sequence:

```text
requested grade (optional)
  -> logical model
  -> ordered provider routes
  -> upstream provider model
  -> fallback logical model and its provider routes, when required
```

The response and usage record retain both the client-requested identity and the actual provider/model used for traceability.

### B. Provider Routes, Fallbacks, and Circuit Breakers

Each logical model can have multiple ordered provider routes. For example, `deepseek-v4-flash-0731` may try DeepSeek directly, then DeepInfra, then OpenRouter. Each route can use the upstream identifier required by that provider.

* **Provider fallback**: Routes are evaluated by priority and current eligibility.
* **Model fallback**: If all eligible routes for a logical model fail or are unavailable, the router may continue through an ordered list of fallback logical models.
* **Minimal runtime state**: Mutable availability is stored only at provider-account and provider-route levels. Logical-model availability is derived from its routes and is not persisted separately.
* **Circuit breaker**: Route health transitions through `closed`, `open`, and `half-open` states. Breaker behavior depends on the normalized failure class instead of treating every `429` or provider error equally.
* **Quota and billing breaker**: A machine-identified quota, credit, or spend-limit exhaustion opens immediately without repeated failures. It remains open until an authoritative reset/recovery signal or an allowed probe confirms recovery.
* **Rate-limit breaker**: Temporary throughput or concurrency throttling respects `Retry-After` or a provider-specific cooldown, then permits a half-open probe.
* **Availability breaker**: Timeouts, connection failures, overload, and eligible `5xx` responses use configurable failure thresholds and short recovery probes.
* **Safe retry boundary**: A request may be replayed to another route only before response headers or the first streamed output token are sent to the client. Once output has begun, the gateway must not replay the request silently because this could duplicate output and charges.
* **Routing evidence**: Failure category, fallback path, circuit-breaker transition, and final route are recorded without exposing provider credentials.

### C. Dynamic Presets System

A **Preset** is an atomic collection of grade mappings, logical-model routes, fallback chains, and applicable policies. Activating a preset changes routing for new requests without modifying connected clients or restarting the gateway.

Example presets:

* **`default`**: Balanced provider priority and model quality.
* **`cost_saver`**: Prefers lower-cost routes and efficient fallback models.
* **`high_throughput`**: Prefers providers with greater concurrency and lower latency.
* **`benchmark`**: Pins routes for reproducible model comparisons.

### D. Time-Based Scheduling Engine

Schedules select presets for configured operational windows and time zones. Higher priority resolves overlapping schedules. A manual preset activation overrides schedules until that override is explicitly cleared.

Examples:

* An off-peak window activates a lower-price provider topology.
* A continuous-integration window activates a high-throughput topology.

Preset selection is performed at request start. In-flight requests continue on the route already selected.

### E. Gateway Key Governance

A middleware layer validates client requests against configured gateway keys and masks all upstream provider credentials. Gateway keys can be stored locally in SQLite or shared through PostgreSQL. Stored secrets must be protected at rest or represented as hashes when the original value does not need to be recovered.

### F. Usage and Provider Cost Tracking

Every upstream attempt receives a request/attempt identifier and records:

* requested grade or logical model;
* resolved logical model, provider route, and upstream model;
* provider-specific billed units, including request count, input, cached-input, reasoning, and output tokens when applicable;
* provider-reported charge and currency, when available;
* the source and status of the amount: `provider_reported`, `reconciled`, or `unknown`;
* timestamps, latency, success/failure status, and fallback/circuit-breaker metadata.

Providers that expose billing exports or usage APIs may be reconciled asynchronously after the request. If the billed amount cannot be obtained from the provider, it remains unknown and is never derived from a local pricing estimate.

If the primary database is temporarily unavailable, usage events are written to a durable local spool and synchronized later so remote-database outages do not silently discard cost data.

### G. Authoritative Quota Monitoring and Error-Driven Failover

Users should not have to reproduce provider quotas manually inside the gateway. Each provider adapter declares whether the provider exposes authoritative plan, quota, usage, credit, or reset data to the supplied credentials.

The MVP keeps only two mutable availability states:

| Level | States | Effect |
|---|---|---|
| Provider account | `unknown`, `available`, `blocked` | Applies to every route that shares the provider credential or account. |
| Provider route | `closed`, `open`, `half_open` | Applies only to one upstream provider/model route. |

A logical model has no independent runtime status. It is available whenever at least one of its configured routes is eligible. A route is eligible when it is enabled, its provider account is not blocked, and its route circuit is not open.

New provider accounts start as `unknown` and eligible. The gateway does not need to poll every model continuously or prove that credit exists before the first request.

When authoritative data is available, the gateway synchronizes it and may move traffic proactively when **5% or less remains**. This covers different provider business models without assuming token billing:

* periodic request allowances, such as a weekly request pool;
* input, output, or total token allowances;
* prepaid monetary credit and provider-reported spend;
* provider rate limits, treated as temporary capacity rather than subscription exhaustion.

When authoritative data is not available, the gateway does not estimate remaining quota, reconstruct provider balances, or use local pricing tables to make quota decisions. It keeps the route eligible until the provider returns a machine-classifiable failure, then opens the appropriate circuit and attempts a fallback within the same client request whenever the safe retry boundary has not been crossed.

#### Normalized Provider Error Contract

There is no assumption that HTTP status alone identifies the cause. In particular, `429` can represent a transient request rate limit, token throughput limit, concurrency limit, exhausted subscription quota, depleted credit, or enforced spend limit.

Every provider adapter translates structured response fields, provider error codes, status, headers, and—only as a provider-specific last resort—stable documented message patterns into an internal error contract:

* `category`: `quota_exhausted`, `credit_exhausted`, `spend_limit_exceeded`, `rate_limited`, `concurrency_limited`, `authentication_failed`, `model_unavailable`, `provider_unavailable`, or `unknown`;
* `scope`: provider account, route, logical model, upstream model, or credential;
* `retry_class`: `after_reset`, `after_delay`, `after_configuration_change`, `immediate_fallback`, or `not_retryable`;
* `retry_at`: authoritative reset or retry time when the provider supplies one;
* `provider_code`, HTTP status, request ID, and sanitized diagnostic metadata.

Classification precedence is: structured provider code, standardized or documented headers, provider-specific structured fields, and finally documented message matching. Unknown `429` responses use a short conservative rate-limit circuit; they must not be labeled as quota exhaustion without evidence.

Routing behavior:

* Confirmed quota, credit, or spend exhaustion opens the affected circuit on the first failure and skips repeated retries against the same constraint.
* The normalized error scope determines where state is written: account-level exhaustion blocks all routes sharing the account, while model- or route-specific failures open only the affected route circuit.
* Temporary rate or concurrency limits use `Retry-After`, reset headers, or a provider-specific cooldown and then transition to half-open.
* The router first tries another provider route for the same logical model, then a configured fallback logical model.
* The failed upstream attempt is hidden from the client when fallback succeeds before response headers or the first streamed output token are forwarded.
* Requests already streaming are never replayed silently.
* At `retry_at`, one request is admitted through the half-open route. Success closes the circuit; another classified failure renews the block. This real request also acts as the recovery probe, avoiding synthetic inference traffic.
* If all eligible routes are unavailable, the gateway returns one normalized OpenAI-compatible error that includes a gateway request ID but does not leak upstream credentials or sensitive diagnostics.
* Authoritative proactive monitoring and reactive circuit breaking coexist. No routing decision is based on an estimated quota or estimated balance.

### H. Protocol and Cache Extensibility

Protocol and provider are independent composition axes. An OpenAI-compatible client may be routed to a provider reached through Anthropic, Gemini, DashScope, or OpenAI-compatible upstream semantics when the request can be translated without losing required behavior. Likewise, multiple providers may share the same upstream protocol adapter.

This composition avoids implementing every client-protocol/provider combination separately:

```text
client protocol adapter -> canonical request/events -> router
  -> provider connector + upstream protocol adapter -> provider
```

The canonical model covers common messages, typed content parts, tool definitions and calls, streaming events, finish reasons, usage, and normalized errors. Provider-specific fields remain available through namespaced extensions and route capabilities. Translation must preserve semantics or explicitly reject the unsupported feature.

LLM caching is divided into two distinct product concepts:

* **Provider-native prompt/prefix caching**: OpenAI and Anthropic expose prompt caching semantics for reusable prompt prefixes.
* **Provider-native context caching**: Google Gemini can create and reuse explicit cached-content resources.
* **Gateway response caching**: Reusing a previous generated answer is a different, riskier feature because LLM output can be nondeterministic and sensitive. It is not part of the initial caching scope and, if added, must be explicit and opt-in.

The gateway expresses a protocol-neutral cache intent and lets the selected route adapter decide whether it supports implicit caching, explicit breakpoints, or explicit cached-content resources. Cache identifiers and accounting are scoped to the actual provider account, route, and model. A fallback route never assumes that another route's cache can be reused.

Cache optimization must not require storing raw prompts in the gateway database by default. Usage records capture only provider-reported cache metrics and billed cost. The gateway never claims a cache hit or saving without provider evidence.

### I. Headless CLI

The CLI exposes deterministic commands suitable for humans, scripts, and autonomous agents. The final syntax may evolve, but the supported operations include:

* `gateway init --database <sqlite|postgres>`: Initialize a local or shared installation.
* `preset create|activate|clear-override`: Manage preset lifecycle and manual selection.
* `schedule set`: Associate a preset with a time window, time zone, and priority.
* `grade map`: Map a grade to a logical model inside a preset.
* `model create`: Register a stable client-facing logical model.
* `route add`: Add and prioritize an upstream provider route for a logical model.
* `fallback add`: Configure an ordered logical-model fallback.
* `provider sync`: Refresh provider plan, quota, usage, and balance information.
* `quota status`: Show authoritative quota sources, freshness, remaining headroom, reset times, and providers that support reactive detection only.
* `quota policy`: Override switching threshold or stale/no-fallback behavior without manually duplicating the provider allowance.
* `usage summary`: Show provider-reported request/token consumption, billed cost, and quota headroom when authoritative data exists.
* `route status`: Show provider-account state, route circuit state, and derived logical-model availability.
* `cache status`: Show route cache capability, provider-scoped cache resources, expiry, and provider-reported cache usage.

Commands must support machine-readable output for automation.

---

## 5. Portable Database Model

The migration system maintains equivalent schemas for SQLite and PostgreSQL. PostgreSQL-specific identity, timestamp, and constraint syntax must not leak into the SQLite migration set, and vice versa.

The logical schema contains the following entities:

| Entity | Responsibility |
|---|---|
| `gateway_keys` | Client authentication keys and descriptions. |
| `presets` | Atomic routing configurations and manual-override state. |
| `logical_models` | Stable client-facing model names. |
| `preset_grade_mappings` | Grade-to-logical-model mapping per preset. |
| `provider_accounts` | Provider identity and credential reference, never the returned secret. |
| `provider_routes` | Ordered provider/upstream-model targets, upstream protocol, declared capabilities, and circuit-breaker configuration. |
| `model_fallbacks` | Ordered cross-model fallback relationships per preset. |
| `schedules` | Time-window preset activation rules with time zone and priority. |
| `usage_attempts` | Per-attempt tokens, cost, outcome, route, and reconciliation status. |
| `provider_quota_snapshots` | Authoritative provider-synchronized plans, constraints, balances, consumption, reset times, and freshness. |
| `quota_routing_policies` | Switching threshold and stale/no-fallback behavior; does not duplicate provider facts. |
| `provider_error_mappings` | Versioned provider codes and fields mapped to the normalized error contract. |
| `provider_account_state` | `unknown`, `available`, or `blocked` state, reason, evidence, observation time, and retry time per provider account. |
| `provider_route_state` | `closed`, `open`, or `half_open` circuit state, failure count, last error, and next probe time per route. |
| `provider_cache_entries` | Optional provider-scoped cache resource identifiers, model/account scope, expiry, and lifecycle state without raw prompt content by default. |
| `cache_policies` | Protocol-neutral cache intent and per-preset or per-model enablement; never a claim that every route supports the same cache semantics. |

Core integrity rules:

* Logical model names are globally unique and stable.
* A grade is mapped at most once inside a preset.
* Provider route priority is unique for a given preset and logical model.
* Model fallback priority is unique for a given preset and source model.
* Usage attempts are immutable after completion except for explicit billing reconciliation fields.
* Monetary values use fixed-precision decimals and always include a currency.
* Quota snapshots retain their provider source, observation time, freshness, and reset semantics and contain authoritative provider values only.
* Error mappings are versioned and tested against sanitized provider fixtures because provider contracts can change independently.
* Logical-model availability is always derived and is never stored as a third mutable source of truth.
* Provider-native cache entries are scoped to one provider account, route, and model and cannot be reused implicitly across fallback boundaries.

---

## 6. Open-Source Distribution and Deployment

The canonical repository is `github.com/lindoelio/smart-llm-gateway`.

The project distribution must include:

* a virtual Cargo Workspace whose internal crates set `publish = false` and compose into the public gateway executable;
* versioned binaries for supported operating systems and architectures;
* a CLI installer with non-interactive support;
* checksums or signatures for downloadable artifacts;
* an upgrade path that preserves configuration and database migrations;
* documented SQLite quick start and PostgreSQL shared-deployment guides;
* PostgreSQL examples for generic hosts and Neon, with Neon described as an optional hosting choice;
* sample environment files that contain placeholders only and never real credentials;
* an explicit open-source license and contribution guidelines before the first public release.

---

## 7. Success and Performance Criteria

1. **Stable Client Contract**: Changing a logical model's provider or upstream identifier requires no client configuration change.
2. **Zero In-Flight Disruption**: Preset, schedule, quota, and circuit-breaker changes affect new requests without terminating requests already streaming.
3. **Provider Resilience**: When a route fails before output begins, the gateway can continue through configured provider and model fallbacks while recording the complete path.
4. **Quota Continuity**: Authoritative quota data enables proactive switching at 5% remaining; otherwise, a classifiable provider exhaustion error opens the circuit on its first occurrence and triggers an eligible fallback without manual bookkeeping.
5. **Consumption Traceability**: Every upstream attempt records provider-reported billable units and cost status. Unavailable billing data remains unknown rather than estimated.
6. **Storage Portability**: The same externally visible behavior and migration coverage pass against both SQLite and PostgreSQL.
7. **Graceful Database Degradation**: A remote PostgreSQL outage uses a last-known-good routing snapshot and durable local usage spool. A new installation with no valid snapshot may use an explicitly configured static emergency route.
8. **Internal Overhead**: Under cache-hit conditions, authorization, preset selection, schedule evaluation, quota eligibility, and route selection target less than **1.5 ms** of gateway-added latency, excluding durable usage persistence.
9. **Safe Concurrency**: Circuit-breaker transitions and provider error evidence remain correct under concurrent requests and multiple gateway instances sharing PostgreSQL.
10. **Installability**: A new user can install the CLI, initialize SQLite, configure one provider route, and complete a first request using the documented quick start.
11. **Protocol Extensibility**: Adding a new inbound protocol or upstream provider protocol does not require changing routing, quota, scheduling, or persistence domain logic.
12. **Cache Correctness**: Cache intent is either preserved, translated, or explicitly reported as unsupported. Cache hits and savings are recorded only from provider-reported evidence, and fallback never assumes cross-provider cache portability.
