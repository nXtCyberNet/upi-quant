
# Real-Time Mule & Collusive Fraud Intelligence Engine

> **A graph-powered, real-time UPI fraud detection system built for India's Unified Payments Interface ecosystem.**
>
> Processes **500 transactions per second** with **< 200 ms per-transaction scoring latency**, combining Neo4j graph analytics, Redis Streams ingestion, behavioural profiling, Indian IPv4 ASN intelligence (MMDB-backed), device fingerprinting, and six GDS algorithms into a single weighted fusion risk score with full explainability.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [System Data Flow](#2-system-data-flow)
3. [Neo4j Graph Schema](#3-neo4j-graph-schema)
4. [Risk Fusion Engine](#4-risk-fusion-engine)
5. [Feature Extractor №1 — Behavioural Intelligence](#5-feature-extractor-1--behavioural-intelligence)
6. [Feature Extractor №2 — Graph Intelligence](#6-feature-extractor-2--graph-intelligence)
7. [Feature Extractor №3 — Device Risk](#7-feature-extractor-3--device-risk)
8. [Feature Extractor №4 — Dead Account Detection](#8-feature-extractor-4--dead-account-detection)
9. [Feature Extractor №5 — Velocity & Pass-Through](#9-feature-extractor-5--velocity--pass-through)
10. [Indian IPv4 ASN Intelligence (8-Step Pipeline)](#10-indian-ipv4-asn-intelligence-8-step-pipeline)
11. [Collusive Fraud Detection (Batch)](#11-collusive-fraud-detection-batch)
12. [Mule Account Classification](#12-mule-account-classification)
13. [Graph Data Science Algorithms (Batch)](#13-graph-data-science-algorithms-batch)
14. [Explainability Engine](#14-explainability-engine)
15. [Anomaly Detection Primitives](#15-anomaly-detection-primitives)
16. [Evaluation Metrics](#16-evaluation-metrics)
17. [API Reference](#17-api-reference)
18. [WebSocket Real-Time Alerts](#18-websocket-real-time-alerts)
19. [Configuration Reference](#19-configuration-reference)
20. [Deployment Guide](#20-deployment-guide)
21. [Project Structure](#21-project-structure)
22. [Privacy & Compliance (DPDP Act)](#22-privacy--compliance-dpdp-act)

---

## 1. Architecture Overview

\`\`\`
                         ┌──────────────────────────────────────────┐
                         │           UPI Gateway / API              │
                         │         POST /api/transaction            │
                         └──────────┬───────────────────────────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │    Redis Streams      │
                         │  (transactions queue) │
                         └─────┬────────────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
              ▼                ▼                ▼
       ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
       │   Worker 0  │ │   Worker 1  │ │   Worker N  │   (4 async workers)
       │  ┌────────┐ │ │             │ │             │
       │  │ Ingest │ │ │             │ │             │
       │  │(Neo4j) │ │ │    ...      │ │    ...      │
       │  ├────────┤ │ │             │ │             │
       │  │MMDB ASN│ │ │             │ │             │
       │  │Resolve │ │ │             │ │             │
       │  ├────────┤ │ │             │ │             │
       │  │  Risk  │ │ │             │ │             │
       │  │ Engine │ │ │             │ │             │
       │  └────────┘ │ │             │ │             │
       └─────────────┘ └─────────────┘ └─────────────┘
              │                │                │
              ▼                ▼                ▼
       ┌───────────────────────────────────────────────┐
       │                 Neo4j (GDS)                    │
       │  :User  :Device  :IP  :Transaction  :Cluster  │
       └───────────────────────────────────────────────┘
              │
              ▼
       ┌──────────────────────────────────────┐
       │   Background Graph Analyzer (5s)     │
       │  • Louvain  • Betweenness            │
       │  • PageRank • Clustering Coeff       │
       │  • WCC      • Collusive Detection    │
       │  • User Stats Aggregation            │
       │  • Device Count Refresh              │
       │  • Dormant Account Flagging          │
       └──────────────────────────────────────┘
              │
              ▼
       ┌──────────────────────────────────────┐
       │   WebSocket ws://host/ws/alerts      │
       │   (real-time push to dashboard)      │
       └──────────────────────────────────────┘
\`\`\`

### Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **API** | FastAPI + Uvicorn | REST endpoints + WebSocket |
| **Queue** | Redis 7 Streams | Transaction ingestion pipeline (consumer groups) |
| **Graph DB** | Neo4j 5.15 + GDS Plugin | Transaction graph, feature reads, batch algorithms |
| **ASN Intelligence** | MaxMind MMDB (local) | Indian IPv4 ASN classification (offline, no API calls) |
| **Computation** | NumPy, SciPy | Statistical features (z-score, Mahalanobis, IQR) |
| **Models** | Pydantic v2 | Request/response validation, settings management |
| **Containerisation** | Docker Compose | Neo4j + Redis orchestration |

### Performance Targets

| Metric | Target | Mechanism |
|--------|--------|-----------|
| **Throughput** | 500 TPS | 4 async workers × batch Redis reads × lock-free Neo4j ingest |
| **Scoring Latency** | < 200 ms | 5 extractors run concurrently via `asyncio.gather()` |
| **Deadlock Rate** | ≈ 0 | MATCH-based ingest (not MERGE), exponential backoff retry |
| **Batch Cycle** | Every 5 s | GDS algorithms + aggregation run outside hot path |

---

## 2. System Data Flow

### Hot Path (per-transaction, < 200 ms)

\`\`\`
Transaction → Redis XADD
           → Worker XREADGROUP
           → Step 1: Ingest into Neo4j (MATCH-based, lock-free)
                     ↳ Fallback to MERGE-based if user not pre-seeded
           → Step 1b: MMDB ASN Resolve → INGEST_IP (asn, org, country, class)
           → Step 2: Risk Engine (5 concurrent extractors):
                     ├── BehavioralFeatureExtractor.compute()
                     ├── DeadAccountDetector.compute()
                     ├── DeviceRiskExtractor.compute()
                     ├── GraphIntelligenceExtractor.compute()
                     └── VelocityExtractor.compute()
           → Step 3: Weighted Fusion → RiskResponse
           → Step 4: Write-back risk to Neo4j (fire-and-forget)
           → Step 5: WebSocket alert if risk ≥ 40
           → Step 6: ACK message in Redis stream
\`\`\`

### Cold Path (batch, every 5 s)

\`\`\`
GraphAnalyzer._loop()
  → BATCH_UPDATE_USER_STATS (recalculate avg, std, counts)
  → BATCH_UPDATE_DEVICE_STATS (recalculate account_count per device)
  → QUERY_FLAG_DORMANT_ACCOUNTS (flag users inactive > 30 days)
  → GDS_DROP_PROJECTION + GDS_CREATE_PROJECTION
  → GDS_LOUVAIN (community detection)
  → GDS_BETWEENNESS (centrality)
  → GDS_PAGERANK (importance)
  → GDS_LOCAL_CLUSTERING (clustering coefficient)
  → CollusiveFraudDetector.refresh() (all 6 pattern queries)
\`\`\`

### Deadlock Mitigation Strategy

\`\`\`python
_MAX_RETRIES = 3
_BASE_BACKOFF_SEC = 0.02  # 20 ms

for attempt in range(_MAX_RETRIES):
    try:
        await neo4j.write_async(INGEST_TRANSACTION, params)
        return
    except TransientException:
        backoff = 0.02 * (2 ** attempt) + random(0, 0.01)
        await asyncio.sleep(backoff)
\`\`\`

| Attempt | Backoff Range |
|---------|---------------|
| 1 | 20–30 ms |
| 2 | 40–50 ms |
| 3 | 80–90 ms |

If all retries fail → `INGEST_TRANSACTION_SAFE` (MERGE-based fallback).

---

## 3. Neo4j Graph Schema

### Node Labels

| Label | Key Property | Description |
|-------|-------------|-------------|
| `:User` | `user_id` (UNIQUE) | UPI account holder |
| `:Device` | `device_hash` (UNIQUE) | Device fingerprint |
| `:IP` | `ip_address` (UNIQUE) | IP address node with ASN metadata |
| `:Transaction` | `tx_id` (UNIQUE) | Individual UPI transaction |
| `:Cluster` | `cluster_id` (UNIQUE) | Fraud community cluster |

### Relationship Types

| Relationship | Pattern | Properties |
|-------------|---------|------------|
| `:SENT` | `(User)-[:SENT]->(Transaction)` | — |
| `:RECEIVED_BY` | `(Transaction)-[:RECEIVED_BY]->(User)` | — |
| `:USES_DEVICE` | `(User)-[:USES_DEVICE]->(Device)` | — |
| `:ACCESSED_FROM` | `(User)-[:ACCESSED_FROM]->(IP)` | — |
| `:TRANSFERRED_TO` | `(User)-[:TRANSFERRED_TO]->(User)` | `total_amount`, `tx_count`, `last_tx` |
| `:MEMBER_OF` | `(User)-[:MEMBER_OF]->(Cluster)` | — |

### User Node Properties

| Property | Type | Source | Description |
|----------|------|--------|-------------|
| `user_id` | String | Seed/Ingest | Unique identifier |
| `avg_tx_amount` | Float | Batch | Mean transaction amount |
| `std_tx_amount` | Float | Batch | Std deviation of tx amounts |
| `tx_count` | Int | Batch | Total transactions sent |
| `total_outflow` | Float | Batch | Sum of all sent amounts |
| `last_active` | DateTime | Hot path | Timestamp of last transaction |
| `is_dormant` | Boolean | Batch | True if inactive > 30 days |
| `risk_score` | Float | Hot path | Latest fused risk score |
| `last_lat` / `last_lon` | Float | Seed | Last known geo-coordinates |
| `community_id` | Int | GDS Louvain | Louvain community ID |
| `betweenness` | Float | GDS | Betweenness centrality |
| `pagerank` | Float | GDS | PageRank score |
| `clustering_coeff` | Float | GDS | Local clustering coefficient |
| `component_id` | Int | GDS WCC | Weakly Connected Component ID |

### IP Node Properties

| Property | Type | Description |
|----------|------|-------------|
| `ip_address` | String | IPv4 address |
| `asn` | Int | Autonomous System Number (from MMDB) |
| `asn_type` | String | Classification: `MOBILE_ISP`, `BROADBAND`, `ENTERPRISE`, `INDIAN_CLOUD`, `HOSTING`, `FOREIGN`, `UNKNOWN` |
| `asn_org` | String | Organisation name from MMDB |
| `asn_country` | String | ISO 3166-1 alpha-2 country code |
| `geo_lat` / `geo_lon` | Float | Geo-coordinates |
| `is_vpn` | Boolean | VPN detection flag |

### Indexes

| Index | On | Purpose |
|-------|----|---------|
| `idx_user_risk` | `(User).risk_score` | Fast high-risk user lookups |
| `idx_user_dormant` | `(User).is_dormant` | Dormant account flagging |
| `idx_user_active` | `(User).last_active` | Temporal queries |
| `idx_tx_ts` | `(Transaction).timestamp` | Time-windowed queries |
| `idx_tx_risk` | `(Transaction).risk_score` | Flagged transaction lookups |
| `idx_device_score` | `(Device).device_score` | Device risk propagation |
| `idx_cluster_risk` | `(Cluster).risk_level` | Dashboard cluster queries |
| `idx_ip_asn` | `(IP).asn` | ASN density lookups |

---

## 4. Risk Fusion Engine

### Master Formula

\`\`\`
R = w_g · S_graph + w_b · S_behavioral + w_d · S_device + w_a · S_dead_account + w_v · S_velocity
\`\`\`

Where each sub-score S_i ∈ [0, 100] and the final R = min(R, 100).

### Weight Configuration

| Weight | Symbol | Default | Rationale |
|--------|--------|---------|-----------|
| Graph Intelligence | w_g | **0.30** | Network structure is the strongest mule indicator |
| Behavioural | w_b | **0.25** | Amount spikes, geo anomalies, ASN intelligence |
| Device Risk | w_d | **0.20** | Shared devices, emulators, OS anomalies |
| Dead Account | w_a | **0.15** | Dormant activation / first-strike patterns |
| Velocity | w_v | **0.10** | Transaction burst / pass-through timing |

### Risk Level Classification

\`\`\`
Level = HIGH     if R ≥ 70
        MEDIUM   if R ≥ 40
        LOW      otherwise
\`\`\`

### Scoring Pipeline (Concurrent)

All five extractors execute as concurrent `asyncio.Task` objects:

\`\`\`python
behav, dead, device, graph, vel = await asyncio.gather(
    behavioral.compute(sender_id, amount, timestamp, lat, lon, channel, ip, sim),
    dead_account.compute(sender_id, amount),
    device_risk.compute(device_hash),
    graph_intel.compute(sender_id),
    velocity.compute(sender_id, amount),
)
\`\`\`

Post-scoring pipeline:
1. **Flag Collection** — merge flags from all 5 extractors
2. **Collusive Flags** — O(1) lookup from cached batch detector
3. **Mule Evaluation** — heuristic aggregation across all features
4. **Flag Deduplication** — `dict.fromkeys()` preserves order
5. **Explainability** — `_build_reason()` generates human-readable string
6. **Write-back** — persist `risk_score` and `status` to Neo4j
7. **Alert** — WebSocket broadcast if `risk ≥ 40`

---

## 5. Feature Extractor №1 — Behavioural Intelligence

**File:** `app/features/behavioral.py`
**Input:** `sender_id`, `amount`, `timestamp`, `lat/lon`, `channel`, `ip_address`, `sim_verified`
**Output:** Sub-score S_behavioral ∈ [0, 100] + feature dict + flags

### 5.1 Amount Z-Score (UPI-Only, 3σ Rule)

**Filter mask:** Only computed when `channel == UPI`. Non-UPI transactions → `amount_zscore = 0`.

**Primary path** (≥ 2 historical transactions):

\`\`\`
z = (A_t - Ā_25) / σ_25
\`\`\`

Where Ā_25 and σ_25 are the rolling mean and standard deviation over the last 25 transactions (`BEHAVIORAL_HISTORY_COUNT`).

**Thin-history fallback** (1 tx or 0 tx but profile exists):

\`\`\`
z = (A_t - μ_profile) / σ_profile
\`\`\`

Where σ_profile = max(σ_stored, 0.5 · μ_profile) to avoid division by zero.

**3σ Spike detection:**

\`\`\`
Spike = 1  if  A_t > Ā + 3σ
\`\`\`

### 5.2 Dormant-Burst Cross-Signal

\`\`\`
DormantBurst = 1  if  is_dormant AND μ_profile > 0 AND A_t > μ_profile
\`\`\`

### 5.3 ASN Intelligence (MMDB-Based)

Delegated to `compute_asn_risk()` (see [§10](#10-indian-ipv4-asn-intelligence-8-step-pipeline)). Returns `asn_risk_scaled ∈ [0, 20]`.

### 5.4 SIM Verification Awareness

\`\`\`
SIM_Risk = 10   if sim_verified = False
           0    otherwise
\`\`\`

### 5.5 Temporal Features

**Time since last transaction:**

\`\`\`
Δt = t_current - t_last
\`\`\`

**Velocity score** (transactions in last 60 s window):

\`\`\`
V = min(|{tx : t_current - t_tx ≤ 60s}| / B_burst, 1.0)
\`\`\`

Where B_burst = 10 (BURST_TX_THRESHOLD).

### 5.6 Night-Time Anomaly

\`\`\`
Night = 1  if  hour ≥ 23 OR hour ≤ 5
\`\`\`

### 5.7 Geo-Distance & Impossible Travel

**Haversine formula:**

\`\`\`
d = 2R · arctan2(√a, √(1-a))

a = sin²(Δφ/2) + cos(φ₁) · cos(φ₂) · sin²(Δλ/2)
\`\`\`

R = 6371 km (Earth radius).

**Impossible travel detection** (realistic Indian upper bound):

\`\`\`
ImpossibleTravel = 1  if  d / (Δt / 3600) > 250 km/h
\`\`\`

> **250 km/h** is the maximum realistic travel speed in India (Vande Bharat Express peak speed), chosen instead of the standard 900 km/h to catch domestic fraud attempts where attackers assume flight-speed tolerance.

### 5.8 Mahalanobis Distance

For users with ≥ 5 historical transactions, a multivariate outlier score:

\`\`\`
D_M(x) = √((x - μ)ᵀ Σ⁻¹ (x - μ))
\`\`\`

Where:
- x = [amount, Δt] (current transaction)
- μ = column-wise mean of historical feature matrix
- Σ⁻¹ = Moore-Penrose pseudoinverse of covariance matrix

### 5.9 Behavioural Risk Fusion

\`\`\`
S_behavioral = min(
    min(|z|·10, 30)           # z-score (UPI only)
  + V·20                       # velocity
  + 𝟙[IT]·20                  # impossible travel
  + 𝟙[night]·5                # night flag
  + min(D_M·2, 15)            # Mahalanobis
  + 𝟙[spike]·10               # 3σ outlier
  + 𝟙[DB]·15                  # dormant burst
  + R_ASN·20                   # ASN risk (0–20)
  + 𝟙[¬SIM]·10                # SIM risk
  , 100
)
\`\`\`

| Component | Max Points | Condition |
|-----------|-----------|-----------|
| Amount z-score | 30 | UPI only, `|z| × 10` capped |
| Velocity | 20 | `recent_count / burst_threshold × 20` |
| Impossible travel | 20 | Speed > 250 km/h |
| Mahalanobis | 15 | `D_M × 2` capped |
| Dormant burst | 15 | Dormant + amount > profile mean |
| 3σ spike | 10 | `A_t > μ + 3σ` |
| SIM not verified | 10 | `sim_verified == False` |
| ASN intelligence | 20 | Full MMDB 8-step pipeline |
| Night flag | 5 | 23:00–05:00 |
| **Maximum** | **145** (capped to 100) | |

---

## 6. Feature Extractor №2 — Graph Intelligence

**File:** `app/features/graph_intelligence.py`
**Input:** `user_id`
**Output:** Sub-score S_graph ∈ [0, 100] + feature dict + flags

### 6.1 Pre-Computed GDS Properties (Read from Node)

| Property | Source Algorithm | Updated |
|----------|----------------|---------|
| `community_id` | GDS Louvain | Batch (5s) |
| `betweenness` | GDS Betweenness | Batch (5s) |
| `pagerank` | GDS PageRank | Batch (5s) |
| `clustering_coeff` | GDS Local Clustering | Batch (5s) |

### 6.2 Community Risk

If `community_id` is set, query `QUERY_COMMUNITY_STATS`:

\`\`\`
CommunityRisk = min(avg_risk_cluster, 100)   if members ≥ 3 AND avg_risk > 50
                40                             if high_risk_count ≥ 2
                0                              otherwise
\`\`\`

### 6.3 Centrality Score (Betweenness)

\`\`\`
CentralityScore = min(b · 200, 30)
\`\`\`

Where b is the raw betweenness centrality. For a typical 500-node graph, b peaks around 0.1, so 0.1 × 200 = 20.

### 6.4 PageRank Score

\`\`\`
PageRankScore = min(PR · 500, 15)
\`\`\`

Higher PageRank → node is "important" in transfer network → potential money router.

### 6.5 Structural Anomaly Detection

| Pattern | Detection Rule | Points |
|---------|---------------|--------|
| **Fan-Out** (Distributor) | `out_degree ≥ 5 AND in_degree ≤ 2` | +15 |
| **Fan-In** (Collector) | `in_degree ≥ 5 AND out_degree ≤ 2` | +15 |
| **Tight Ring** | `clustering_coeff > 0.5 AND total_degree > 4` | +10 |

### 6.6 Neighbour Risk Contagion

\`\`\`
Contagion = min(avg_neighbour_risk × 0.3, 15)
\`\`\`

### 6.7 Graph Risk Fusion

\`\`\`
S_graph = min(
    0.30 · CommunityRisk
  + CentralityScore
  + PageRankScore
  + StructuralScore
  + Contagion
  , 100
)
\`\`\`

---

## 7. Feature Extractor №3 — Device Risk

**File:** `app/features/device_risk.py`
**Input:** `device_hash`
**Output:** Sub-score S_device ∈ [0, 100] + feature dict + flags

### 7.1 Multi-Account Penalty

\`\`\`
MultiAccount = 40   if N_accounts ≥ 5
               25   if N_accounts ≥ 3
               10   if N_accounts ≥ 2
               0    otherwise
\`\`\`

### 7.2 Emulator Penalty

\`\`\`
Emulator = 25   if device is emulator
           0    otherwise
\`\`\`

### 7.3 Risk Propagation

From Neo4j `QUERY_DEVICE_RISK_PROPAGATION`:

\`\`\`
Propagation = min(R_device_risk / 100, 1) × 25
\`\`\`

The device risk score itself is a tiered Cypher calculation:

| Condition | Score |
|-----------|-------|
| `user_count ≥ 5` | 100 |
| `user_count ≥ 3` | 70 |
| `max_user_risk > 80` | 60 |
| Default | `avg_user_risk × 0.5` |

### 7.4 High-Risk Neighbour Bonus

\`\`\`
HighRiskBonus = 10   if max(R_users) > 80
                0    otherwise
\`\`\`

### 7.5 OS Anomaly Detection (UPI Constraint)

UPI apps run **only on Android and iOS**. Any other OS is anomalous:

\`\`\`
OSAnomaly = 15   if OS ∉ {Android*, iOS*}
            0    otherwise
\`\`\`

### 7.6 Device Risk Fusion

\`\`\`
S_device = min(MultiAccount + Emulator + Propagation + HighRiskBonus + OSAnomaly, 100)
\`\`\`

| Component | Max Points |
|-----------|-----------|
| Multi-Account | 40 |
| Emulator | 25 |
| Propagation | 25 |
| High-Risk Bonus | 10 |
| OS Anomaly | 15 |
| **Maximum** | **115** (capped to 100) |

---

## 8. Feature Extractor №4 — Dead Account Detection

**File:** `app/features/dead_account.py`
**Input:** `user_id`, `tx_amount`
**Output:** Sub-score S_dead ∈ [0, 100] + feature dict + flags

### Detection Philosophy

A "dead" or "dormant" account has been inactive for > 30 days. When such accounts suddenly activate with large transfers, it is a classic mule-activation pattern used in layered fraud schemes.

### 8.1 First-Strike Path (Single Neo4j Round-Trip)

The optimised `QUERY_DORMANT_WAKEUP` captures everything in one query:

\`\`\`cypher
WITH u,
     duration.between(u.last_active, datetime()).days AS days_slept
OPTIONAL MATCH (u)-[:SENT]->(tx:Transaction)
  WHERE tx.timestamp > datetime() - duration({hours: 1})
...
CASE WHEN days_slept > $dormant_days AND recent_tx_count > 0
     THEN true ELSE false END AS is_first_strike
\`\`\`

### 8.2 Inactivity Score

\`\`\`
Inactivity = min(days_slept / 30, 1) × 30
\`\`\`

### 8.3 Spike Score

\`\`\`
Spike = min((A_t / Ā_profile) / 10, 1) × 30   if Ā_profile > 0
        25                                       if A_t > 5000 (no history)
        0                                        otherwise
\`\`\`

### 8.4 First-Strike Bonus

\`\`\`
FirstStrike = min(20 + 10, 25)   if first_strike AND volume_spike
              20                  if first_strike only
              0                   otherwise
\`\`\`

### 8.5 Low Activity Bonus

\`\`\`
LowActivity = 10   if N_tx ≤ 3
              0    otherwise
\`\`\`

### 8.6 Dead Account Risk Fusion (First-Strike)

\`\`\`
S_dead = min(Inactivity + Spike + FirstStrike + LowActivity, 100)   if dormant/first_strike
         Spike × 0.3                                                  otherwise
\`\`\`

### 8.7 Legacy Path (Fallback — Two Queries)

If `QUERY_DORMANT_WAKEUP` returns nothing, the system falls back to:

1. `QUERY_DORMANT_STATUS` — user profile read
2. `QUERY_RECENT_INFLOW_OUTFLOW` — windowed flow analysis

Additional feature in legacy path:

**Pass-Through Ratio:**

\`\`\`
PT = outflow_window / inflow_window
PassThroughScore = min(PT / 0.80, 1) × 30
\`\`\`

---

## 9. Feature Extractor №5 — Velocity & Pass-Through

**File:** `app/features/velocity.py`
**Input:** `user_id`, `tx_amount`
**Output:** Sub-score S_velocity ∈ [0, 100] + feature dict + flags

### 9.1 Burst Detection

\`\`\`
Burst = 30   if activity ≥ 10 (full burst)
        15   if activity ≥ 5  (half burst)
        0    otherwise
\`\`\`

Where `activity = send_count + receive_count` in the 60-second window.

### 9.2 Pass-Through Score

\`\`\`
r = total_sent_window / total_received_window

PassThrough = min(r / 1.5, 1) × 35   if r > 0.80
              10                       if r > 0.50
              0                        otherwise
\`\`\`

### 9.3 Velocity Component

\`\`\`
TxPerMin = activity (within 60s window)
VelocityComponent = min(TxPerMin / 10, 1) × 20
\`\`\`

### 9.4 Single Transaction Ratio

\`\`\`
SingleTxRatio = 15   if A_t / total_sent > 0.80
                0    otherwise
\`\`\`

### 9.5 Velocity Risk Fusion

\`\`\`
S_velocity = min(Burst + PassThrough + Velocity + SingleTxRatio, 100)
\`\`\`

| Component | Max Points |
|-----------|-----------|
| Burst | 30 |
| Pass-Through | 35 |
| Velocity | 20 |
| Single Tx Ratio | 15 |
| **Maximum** | **100** |

---

## 10. Indian IPv4 ASN Intelligence (8-Step Pipeline)

**File:** `app/features/asn_intelligence.py`
**Data Source:** Local MMDB file (`asn_ipv4_small.mmdb`) — no external API calls
**Input:** `ip_address`, `sender_id`, `neo4j`
**Output:** `asn_risk ∈ [0, 1]`, scaled to `asn_risk_scaled ∈ [0, 20]` for behavioural fusion

### Step 1 — IPv4 Constraint

\`\`\`
Valid(IP) = 1  if  IP ∈ IPv4  AND  ¬Private  AND  ¬Loopback  AND  ¬Reserved  AND  ¬LinkLocal
\`\`\`

If invalid: ASN = NULL, ASN_risk = 0

Rejects: IPv6, `10.x.x.x`, `192.168.x.x`, `172.16–31.x.x`, `127.x.x.x`, `169.254.x.x`, `0.0.0.0/8`, `100.64.0.0/10`.

### Step 2 — ASN Extraction (MMDB)

\`\`\`python
reader = maxminddb.open_database("asn_ipv4_small.mmdb")
data = reader.get(ip_address)
# data = {"asn": 55836, "organization": {"name": "Jio", "country": "IN", ...}}
\`\`\`

\`\`\`
IP → (ASN_number, Org_name, Country)
\`\`\`

If lookup fails: ASN_number = 0.

### Step 3 — Indian ASN Filtering

\`\`\`
ForeignFlag = 0   if Country(ASN) = "IN"
              1   otherwise
\`\`\`

### Step 4 — ASN Classification (India-Only)

For Indian ASNs (Country = IN), classification uses a **two-tier strategy**:

**Tier 1: Curated ASN Maps** (O(1) `frozenset` lookup):

| Class | Example ASNs | Example ISPs |
|-------|-------------|--------------|
| `MOBILE_ISP` | 55836, 45609, 55644, 45271, 45820 | Jio, Airtel, Vi, BSNL, MTNL |
| `BROADBAND` | 17762, 17488, 133982, 132335 | ACT Fibernet, Hathway, Spectra, Alliance |
| `ENTERPRISE` | 4755, 17439, 10201, 18209 | Tata Comms, Sify, PowerGrid, Tata Tele |
| `INDIAN_CLOUD` | 135929, 133275, 132116 | Yotta, CtrlS, Netmagic |
| `HOSTING` | 133296, 45769, 135580 | Web Werks, Lightstorm, Cyfuture |

**Tier 2: Keyword Fallback** on organisation name (30+ patterns):

\`\`\`python
_ORG_KEYWORDS = [
    ("jio", "MOBILE_ISP"), ("airtel", "MOBILE_ISP"),
    ("hathway", "BROADBAND"), ("tata communications", "ENTERPRISE"),
    ("yotta", "INDIAN_CLOUD"), ("web werks", "HOSTING"),
    ...
]
\`\`\`

For foreign ASNs (Country ≠ IN): automatically classified as `FOREIGN`.

### Base Risk Scores

\`\`\`
ASN_base = 0.0   Mobile ISP
           0.1   Broadband
           0.3   Enterprise
           0.6   Indian Cloud
           0.7   Hosting
           0.5   Unknown (Indian)
           0.8   Foreign
\`\`\`

**Rationale:** UPI transactions originate from mobile phones on Indian networks. A legitimate user on Jio/Airtel (Mobile ISP) has base risk 0. A transaction from an AWS Mumbai IP (classified as FOREIGN since AWS is US-registered) scores 0.8.

### Step 5 — ASN Density Feature

\`\`\`
ASN_density = ln(1 + N_accounts_in_ASN)
\`\`\`

Where N is the count of distinct `:User` nodes that have `[:ACCESSED_FROM]` an `:IP` node with the same ASN number.

**Cypher query:**

\`\`\`cypher
MATCH (u:User)-[:ACCESSED_FROM]->(i:IP)
WHERE i.asn = $asn_number
RETURN count(DISTINCT u) AS account_count
\`\`\`

**Normalisation:**

\`\`\`
ASN_density_norm = min(ln(1 + N) / ln(1001), 1) ≈ ln(1+N) / 6.909
\`\`\`

### Step 6 — ASN Drift Feature

Let ASN_mode = the historically most frequent ASN for the user (from `QUERY_USER_ASN_HISTORY`):

\`\`\`
ASN_drift = 1   if ASN_t ≠ ASN_mode
            0   otherwise
\`\`\`

**Cypher query:**

\`\`\`cypher
MATCH (u:User {user_id: $user_id})-[:ACCESSED_FROM]->(i:IP)
WHERE i.asn IS NOT NULL AND i.asn > 0
RETURN i.asn AS asn, count(i) AS usage_count
ORDER BY usage_count DESC
\`\`\`

### Step 7 — ASN Switching Entropy

Let p_i = P(ASN_i) for the user's historical ASN distribution:

\`\`\`
H_ASN = -Σ p_i · ln(p_i)
\`\`\`

Higher entropy → the user hops across many different ASNs → suspicious infrastructure switching.

**Normalisation:**

\`\`\`
H_norm = min(H / 2.5, 1)
\`\`\`

Where 2.5 ≈ ln(12), a practical upper bound for the number of distinct ASNs a single user would use.

### Step 8 — Final ASN Risk Score

\`\`\`
┌───────────────────────────────────────────────────────────────────────────────────┐
│ ASN_risk = 0.4·ASN_base + 0.3·ASN_density_norm + 0.2·ASN_drift                  │
│          + 0.2·ForeignFlag + 0.1·H_norm                                          │
│                                                                                   │
│ ASN_risk = clamp(ASN_risk, 0, 1)                                                 │
│ ASN_risk_scaled = ASN_risk × 20                                                  │
└───────────────────────────────────────────────────────────────────────────────────┘
\`\`\`

This contributes up to **20 points** to the 0–100 behavioural sub-score.

### Worked Examples

| Scenario | IP | ASN | Country | Class | Base | Foreign | Density | Drift | Entropy | **Risk** |
|----------|-----|-----|---------|-------|------|---------|---------|-------|---------|----------|
| Normal Jio user | `49.36.x.x` | 55836 | IN | MOBILE_ISP | 0.0 | 0 | 0.2 | 0 | 0.1 | **0.07** |
| User on Hathway broadband | `59.88.x.x` | 17488 | IN | BROADBAND | 0.1 | 0 | 0.15 | 0 | 0.05 | **0.09** |
| Attacker on AWS Mumbai | `3.6.x.x` | 16509 | US | FOREIGN | 0.8 | 1 | 0.3 | 1 | 0.4 | **0.75** |
| Fraudster on Indian VPS | `103.x.x.x` | 135929 | IN | INDIAN_CLOUD | 0.6 | 0 | 0.05 | 1 | 0.2 | **0.48** |

---

## 11. Collusive Fraud Detection (Batch)

**File:** `app/detection/collusive_fraud.py`
**Execution:** Every `GRAPH_ANALYTICS_INTERVAL_SEC` (5 s)
**Results:** Cached in memory for O(1) per-transaction lookup

### Detection Patterns

| Pattern | Query | Parameters | Description |
|---------|-------|-----------|-------------|
| **Fraud Islands** | `DETECT_FRAUD_ISLANDS` | `min_avg_risk: 40` | Louvain clusters with ≥ 3 members and avg risk > 40 |
| **Money Routers** | `DETECT_MONEY_ROUTERS` | `min_betweenness: 0.01` | High betweenness-centrality nodes |
| **Circular Flows** | `DETECT_CIRCULAR_FLOWS` | — | A→B→C→A triangles within last 7 days |
| **Rapid Chains** | `DETECT_RAPID_CHAINS` | — | 2–4 hop layered transfers, each < 300 s apart |
| **Star Hubs** | `DETECT_STAR_HUBS` | `min_in_degree: 5, min_out_degree: 5` | Fan-in (Collector) / Fan-out (Distributor) / Relay hubs |
| **Relay Mules** | `DETECT_RELAY_MULE` | `min_flow_ratio: 0.75` | outflow_10min / inflow_10min > 0.75 with ≥ 2 in and ≥ 2 out |

### Circular Flow Detection (Cypher)

\`\`\`cypher
MATCH path = (a:User)-[:TRANSFERRED_TO]->(b:User)
                     -[:TRANSFERRED_TO]->(c:User)
                     -[:TRANSFERRED_TO]->(a)
WHERE a <> b AND b <> c AND a <> c
  AND r1.last_tx > datetime() - duration({days: 7})
\`\`\`

### Rapid Chain Detection

\`\`\`cypher
MATCH path = (start:User)-[:TRANSFERRED_TO*2..4]->(finish:User)
WHERE ALL(i IN range(0, size(timestamps)-2)
      WHERE duration.between(timestamps[i], timestamps[i+1]).seconds < 300)
\`\`\`

### Per-Transaction Lookup (O(1))

\`\`\`python
def get_user_flags(self, user_id: str) -> List[str]:
    # O(1) set/dict lookups:
    clusters = self._user_clusters.get(user_id, set())   # dict lookup
    is_relay = user_id in self._relay_mule_ids            # set lookup
\`\`\`

---

## 12. Mule Account Classification

**File:** `app/detection/mule_detection.py`
**Execution:** Per-transaction, after all 5 extractors run
**Output:** `{is_mule: bool, confidence: float [0-1], reasons: [str]}`

### Scoring Accumulator

| Signal | Score | Condition |
|--------|-------|-----------|
| First-Strike dormant | +0.30 | `is_first_strike == True` |
| Dormant activation | +0.25 | `is_dormant AND dead_risk > 40` |
| High pass-through | +0.20 | `outflow_inflow_ratio > 0.75` |
| Shared device | +0.15 | `account_count ≥ 3` |
| Emulator | +0.10 | `is_emulator == True` |
| High-risk cluster | +0.15 | `community_risk > 50` |
| Relay pattern | +0.10 | `tx_per_min > 5 AND ratio > 0.6` |
| Impossible travel | +0.10 | `impossible_travel == True` |
| Amount spike | +0.05 | `spike_flag == True` |

### Classification Rule

\`\`\`
IsMule = (score ≥ 0.5) OR (R_fused ≥ 65)
\`\`\`

---

## 13. Graph Data Science Algorithms (Batch)

**File:** `app/core/graph_analyzer.py`
**Execution:** Every 5 seconds via `asyncio.Task`
**GDS Projection:** `'fraud-graph'` over `:User` nodes and `:TRANSFERRED_TO` relationships

### Algorithm Pipeline

| Order | Algorithm | GDS Procedure | Write Property | Purpose |
|-------|-----------|--------------|---------------|---------|
| 1 | **Louvain** | `gds.louvain.write` | `community_id` | Community/cluster detection |
| 2 | **Betweenness** | `gds.betweenness.write` | `betweenness` | Identify money routers |
| 3 | **PageRank** | `gds.pageRank.write` | `pagerank` | Rank node importance in transfer network |
| 4 | **Local Clustering** | `gds.localClusteringCoefficient.write` | `clustering_coeff` | Detect tightly-knit fraud rings |
| 5 | **WCC** | `gds.wcc.write` | `component_id` | Weakly Connected Components |

### Louvain Community Detection

Modularity-based community detection. The algorithm:
1. Each node starts as its own community
2. Nodes are moved to the community that maximises modularity gain
3. Communities are collapsed into super-nodes
4. Repeat until modularity stabilises

\`\`\`
Q = (1/2m) Σ_ij [A_ij - (k_i·k_j)/(2m)] δ(c_i, c_j)
\`\`\`

**Configuration:** `maxIterations: 10`, `tolerance: 0.0001` (GDS defaults)

### Betweenness Centrality

Measures how often a node lies on shortest paths between other nodes:

\`\`\`
g(v) = Σ_{s≠v≠t} σ_st(v) / σ_st
\`\`\`

High betweenness → the node acts as a "bridge" or "router" in the transfer network.

### PageRank

\`\`\`
PR(v) = (1-d)/N + d · Σ_{u∈B_v} PR(u)/L(u)
\`\`\`

**Configuration:** `dampingFactor: 0.85`, `maxIterations: 20`

### Local Clustering Coefficient

\`\`\`
C_i = 2·|{e_jk}| / (k_i · (k_i - 1))
\`\`\`

Where k_i is the degree of node i and |{e_jk}| counts edges between its neighbours.

High clustering + high degree = tightly-knit fraud ring.

### Background Aggregation (Decoupled from Hot Path)

Before GDS runs, batch queries update materialised properties:

| Query | Purpose | Window |
|-------|---------|--------|
| `BATCH_UPDATE_USER_STATS` | Recalculate `avg_tx_amount`, `std_tx_amount`, `tx_count` | Last `3 × interval` seconds |
| `BATCH_UPDATE_DEVICE_STATS` | Refresh `account_count` per device | All time |
| `QUERY_FLAG_DORMANT_ACCOUNTS` | Set `is_dormant = true` for inactive users | > 30 days |

---

## 14. Explainability Engine

**File:** `app/core/risk_engine.py` → `_build_reason()`
**Output:** Human-readable string explaining why a transaction was flagged

### Reason Generation Rules

| Condition | Reason String |
|-----------|---------------|
| `dead.is_dormant OR dead.is_first_strike` | "Account activated after {N} days of inactivity" |
| `dead.pass_through_ratio > 0.80` | "Pass-through ratio {X}% exceeds threshold" |
| `graph.community_risk > 50` | "Community #{id} has {X}% fraud density" |
| `graph.betweenness > 0.01` | "High betweenness centrality (money router)" |
| `device.account_count ≥ 5` | "Shared device with {N} other accounts" |
| `device.is_emulator` | "Transaction from emulated device" |
| `behav.impossible_travel` | "Impossible travel detected between consecutive transactions" |
| `behav.amount_zscore > 3` | "Amount z-score {X}x above user baseline" |
| `behav.is_night` | "Unusual night-time transaction" |
| `behav.asn_risk ≥ 0.5` | "High ASN risk: {CLASS} network (country: {CC})" |
| `behav.foreign_flag` | "Foreign IP origin: {country}" |
| `behav.asn_drift` | "ASN drift: unusual network for this user" |
| `behav.sim_not_verified` | "SIM not verified for this transaction" |
| `vel.tx_per_min > 5` | "Velocity: {X} tx/min in last window" |
| `vel.outflow_inflow_ratio > 0.80` | "Rapid fund relay pattern" |
| No triggers but R ≥ 70 | "Multiple minor indicators combined above threshold" |
| No triggers and R < 70 | "No significant risk indicators" |

**Format:** Reasons are joined with ". " and terminated with a period.

**Example output:**
> "Account activated after 45 days of inactivity. Amount z-score 4.2x above user baseline. High ASN risk: HOSTING network (country: IN). SIM not verified for this transaction."

---

## 15. Anomaly Detection Primitives

**File:** `app/detection/anomaly_detection.py`

Lightweight statistical helpers used across extractors:

### Z-Score

\`\`\`
z = (x - x̄) / σ
\`\`\`

Returns 0 if `len(values) < 2` or `σ = 0`.

### IQR Outlier Detection

\`\`\`
Outlier = 1  if  x < Q₁ - k·IQR  OR  x > Q₃ + k·IQR
\`\`\`

Default k = 1.5. Requires ≥ 4 data points.

### Rolling Statistics

\`\`\`
(x̄_w, σ_w) = stats(values[:w])
\`\`\`

Default window w = 25.

### Time Velocity

\`\`\`
V(t_ref, W) = |{t_i : t_ref - t_i ≤ W}|
\`\`\`

### Burst Detection

\`\`\`
Burst = 1  if  V(t_ref, W) ≥ θ
\`\`\`

Default θ = 10, W = 60 s.

---

## 16. Evaluation Metrics

**File:** `app/utils/metrics.py`

### Confusion Matrix

\`\`\`
                Predicted Fraud    Predicted Legit
Actual Fraud         TP                  FN
Actual Legit         FP                  TN
\`\`\`

### Precision

\`\`\`
P = TP / (TP + FP)
\`\`\`

### Recall (Sensitivity)

\`\`\`
R = TP / (TP + FN)
\`\`\`

### F1-Score

\`\`\`
F₁ = 2PR / (P + R)
\`\`\`

### False Positive Rate

\`\`\`
FPR = FP / (FP + TN)
\`\`\`

### Latency Statistics

| Metric | Calculation |
|--------|------------|
| Mean Latency | L̄ = (1/N) Σ L_i |
| P95 Latency | `np.percentile(latencies, 95)` |
| P99 Latency | `np.percentile(latencies, 99)` |
| Throughput | TPS = N / T_total |

---

## 17. API Reference

**Base URL:** `http://localhost:8000/api`

### Endpoints

| Method | Path | Description | Response Model |
|--------|------|-------------|----------------|
| `GET` | `/health` | Health check (Neo4j + workers status) | JSON |
| `POST` | `/transaction` | Synchronous score a single transaction | `RiskResponse` |
| `GET` | `/dashboard/stats` | Aggregate dashboard statistics | `DashboardStats` |
| `GET` | `/viz/fraud-network` | Graph data for Cytoscape.js | `{nodes, edges}` |
| `GET` | `/viz/device-sharing` | Device sharing clusters | `{clusters}` |
| `GET` | `/detection/collusive` | Collusive detection summary | JSON |
| `GET` | `/analytics/status` | Last batch analytics run stats | JSON |
| `GET` | `/db/counts` | Node/relationship counts | JSON |

### POST /api/transaction

**Request Body (`TransactionInput`):**

\`\`\`json
{
  "tx_id": "uuid (auto-generated if omitted)",
  "sender_id": "U0001",
  "receiver_id": "U0042",
  "amount": 15000.00,
  "timestamp": "2026-02-12T14:30:00",
  "device_hash": "DEV0001",
  "device_os": "Android 14",
  "device_model": "Model-7",
  "device_is_emulator": false,
  "ip_address": "49.36.128.42",
  "ip_asn": null,
  "ip_asn_type": "UNKNOWN",
  "sim_verified": true,
  "sender_lat": 19.076,
  "sender_lon": 72.8777,
  "channel": "UPI",
  "upi_id_sender": "user1@upi",
  "upi_id_receiver": "user42@upi"
}
\`\`\`

**Response Body (`RiskResponse`):**

\`\`\`json
{
  "tx_id": "a1b2c3d4-...",
  "risk_score": 72.5,
  "risk_level": "HIGH",
  "breakdown": {
    "graph": 45.0,
    "behavioral": 62.0,
    "device": 35.0,
    "dead_account": 80.0,
    "velocity": 28.0
  },
  "cluster_id": "42",
  "flags": [
    "First-Strike: Dormant 45d → active",
    "Amount spike: 4.2σ above baseline",
    "ASN Risk (HOSTING): score=0.68",
    "MULE SUSPECTED (confidence=65%)"
  ],
  "reason": "Account activated after 45 days of inactivity. Amount z-score 4.2x above user baseline. High ASN risk: HOSTING network (country: IN).",
  "processing_time_ms": 87.3,
  "timestamp": "2026-02-12T14:30:00"
}
\`\`\`

### GET /api/viz/fraud-network

**Query Parameters:**

| Param | Default | Description |
|-------|---------|-------------|
| `min_risk` | 30 | Minimum risk score for included nodes |
| `cluster_ids` | — | Comma-separated cluster IDs to include |

Returns up to 200 nodes ordered by risk score (descending).

---

## 18. WebSocket Real-Time Alerts

**URL:** `ws://localhost:8000/ws/alerts`

### Protocol

1. Client connects → server calls `ws.accept()`
2. Server pushes `RiskResponse` JSON whenever a transaction scores ≥ 40 (MEDIUM threshold)
3. Client can send keep-alive messages (ignored by server)
4. Dead connections are automatically pruned on next broadcast

### Alert Payload

Same structure as `RiskResponse` from the REST API.

---

## 19. Configuration Reference

**File:** `app/config.py` — all settings are overridable via environment variables or `.env` file.

### Application

| Variable | Default | Description |
|----------|---------|-------------|
| `APP_NAME` | "Real-Time Mule & Collusive Fraud Intelligence Engine" | Service name |
| `APP_VERSION` | "1.0.0" | Semantic version |
| `DEBUG` | `true` | Debug mode |
| `HOST` | `0.0.0.0` | Bind address |
| `PORT` | `8000` | Bind port |

### Neo4j

| Variable | Default | Description |
|----------|---------|-------------|
| `NEO4J_URI` | `bolt://localhost:7687` | Bolt endpoint |
| `NEO4J_USER` | `neo4j` | Auth username |
| `NEO4J_PASSWORD` | `password123` | Auth password |
| `NEO4J_DATABASE` | `neo4j` | Target database |
| `NEO4J_MAX_POOL_SIZE` | `50` | Connection pool size |

### Redis

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_DB` | `0` | Redis database number |
| `REDIS_STREAM_KEY` | `transactions` | Stream name |
| `REDIS_CONSUMER_GROUP` | `fraud_workers` | Consumer group name |
| `REDIS_ALERTS_CHANNEL` | `fraud_alerts` | Pub/sub channel for alerts |

### Worker Pool

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_COUNT` | `4` | Number of async worker tasks |
| `WORKER_BATCH_SIZE` | `10` | Messages per XREADGROUP call |

### Risk Fusion Weights

| Variable | Default | Sum |
|----------|---------|-----|
| `WEIGHT_GRAPH` | `0.30` | |
| `WEIGHT_BEHAVIORAL` | `0.25` | |
| `WEIGHT_DEVICE` | `0.20` | |
| `WEIGHT_DEAD_ACCOUNT` | `0.15` | |
| `WEIGHT_VELOCITY` | `0.10` | **1.00** |

### Risk Thresholds

| Variable | Default | Description |
|----------|---------|-------------|
| `HIGH_RISK_THRESHOLD` | `70.0` | Score ≥ this → HIGH risk level |
| `MEDIUM_RISK_THRESHOLD` | `40.0` | Score ≥ this → MEDIUM risk level |

### Feature Parameters

| Variable | Default | Description |
|----------|---------|-------------|
| `MMDB_PATH` | `asn_ipv4_small.mmdb/asn_ipv4_small.mmdb` | Path to MaxMind MMDB file |
| `DORMANT_DAYS_THRESHOLD` | `30` | Days of inactivity → dormant flag |
| `DEVICE_ACCOUNT_THRESHOLD` | `5` | Accounts per device → shared device flag |
| `VELOCITY_WINDOW_SEC` | `60` | Sliding window for velocity features |
| `BEHAVIORAL_HISTORY_COUNT` | `25` | Rolling history depth for z-score |
| `PASS_THROUGH_RATIO_THRESHOLD` | `0.80` | Outflow/inflow ratio → relay flag |
| `BURST_TX_THRESHOLD` | `10` | Transactions in window → burst flag |
| `IMPOSSIBLE_TRAVEL_KMH` | `250.0` | Max realistic travel speed (India) |
| `NIGHT_START_HOUR` | `23` | Night window start (24h) |
| `NIGHT_END_HOUR` | `5` | Night window end (24h) |

### Simulation

| Variable | Default | Description |
|----------|---------|-------------|
| `SIMULATION_TPS` | `500` | Target transactions per second |
| `SIMULATION_TOTAL_TX` | `10000` | Total transactions to generate |
| `SIMULATION_USER_COUNT` | `500` | Synthetic user count |
| `SIMULATION_MULE_RATIO` | `0.08` | Fraction of users designated as mules |
| `SIMULATION_DEVICE_COUNT` | `300` | Synthetic device count |

---

## 20. Deployment Guide

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- ~4 GB RAM (Neo4j heap + Redis + Python workers)

### Quick Start

\`\`\`bash
# 1. Start infrastructure
cd fraud-detection-system/backend
docker compose up -d
# Wait for Neo4j to become healthy (~30s)

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Setup Neo4j schema
python ../scripts/setup_neo4j.py

# 4. Seed synthetic data (500 users, 300 devices, ~4000 transactions)
python ../scripts/seed_data.py

# 5. Start the backend
uvicorn app.main:app --host 0.0.0.0 --port 8000

# 6. (Optional) Run 500-TPS simulation
python ../scripts/run_simulation.py --tx 10000 --tps 500
\`\`\`

### Docker Compose Services

| Service | Image | Ports | Purpose |
|---------|-------|-------|---------|
| `neo4j` | `neo4j:5.15-community` | 7474 (browser), 7687 (bolt) | Graph database + GDS plugin |
| `redis` | `redis:7-alpine` | 6379 | Transaction stream + pub/sub alerts |

**Neo4j Memory Configuration:**

| Setting | Value |
|---------|-------|
| Heap initial | 512 MB |
| Heap max | 2 GB |
| Page cache | 512 MB |

### MMDB File

The ASN intelligence module requires the MMDB file at the configured `MMDB_PATH`:

\`\`\`
upi/
├── asn_ipv4_small.mmdb/
│   └── asn_ipv4_small.mmdb   ← Indian IPv4 ASN database
└── fraud-detection-system/
    └── ...
\`\`\`

The module auto-searches multiple candidate paths relative to the working directory and the source file. If the MMDB file is not found, ASN intelligence is disabled (graceful degradation — `asn_risk = 0`).

---

## 21. Project Structure

\`\`\`
fraud-detection-system/
├── README.md
├── backend/
│   ├── docker-compose.yml          # Neo4j + Redis containers
│   ├── requirements.txt            # Python dependencies (14 packages)
│   └── app/
│       ├── __init__.py
│       ├── config.py               # Pydantic Settings (all constants)
│       ├── main.py                 # FastAPI lifespan (startup/shutdown)
│       ├── neo4j_manager.py        # Sync + Async Neo4j driver wrapper
│       │
│       ├── api/
│       │   ├── routes.py           # REST endpoints (8 routes)
│       │   └── websocket.py        # WebSocket alert broadcasting
│       │
│       ├── core/
│       │   ├── graph_analyzer.py   # Batch GDS loop (5s interval)
│       │   ├── risk_engine.py      # Fusion engine + explainability
│       │   └── worker_pool.py      # 4 async Redis stream consumers
│       │
│       ├── detection/
│       │   ├── anomaly_detection.py  # Statistical primitives (z, IQR, Mahalanobis)
│       │   ├── collusive_fraud.py    # 6 pattern detectors (batch + cached)
│       │   └── mule_detection.py     # Heuristic mule classifier
│       │
│       ├── features/
│       │   ├── asn_intelligence.py   # MMDB-backed Indian IPv4 ASN (8-step)
│       │   ├── behavioral.py         # Behavioural anomaly extractor
│       │   ├── dead_account.py       # Dormant account activation detector
│       │   ├── device_risk.py        # Device fingerprint risk scorer
│       │   ├── graph_intelligence.py # Per-user graph feature reader
│       │   └── velocity.py           # Time-windowed velocity scorer
│       │
│       ├── models/
│       │   ├── risk_score.py       # RiskResponse, RiskBreakdown, DashboardStats
│       │   └── transaction.py      # TransactionInput, UserProfile, IPASNType
│       │
│       ├── streaming/
│       │   ├── redis_stream.py     # Redis producer/consumer helpers
│       │   └── transaction_simulator.py  # 500-TPS fraud scenario generator
│       │
│       └── utils/
│           ├── cypher_queries.py   # All Cypher queries (60+ queries)
│           └── metrics.py          # Precision/Recall/F1/FPR calculator
│
├── scripts/
│   ├── run_simulation.py           # CLI: run the 500-TPS simulator
│   ├── seed_data.py                # CLI: populate Neo4j with synthetic data
│   └── setup_neo4j.py              # CLI: create constraints + indexes
│
└── frontend/
    └── src/
        └── components/
            └── FraudClusterGraph.tsx  # (Cytoscape.js visualisation stub)
\`\`\`

---

## 22. Privacy & Compliance (DPDP Act)

### Design Principles

| Principle | Implementation |
|-----------|---------------|
| **No balance storage** | User wealth (`balance`, `total_inflow`) is never stored. Spike detection uses behavioural baselines (3σ rule) instead. |
| **No Sensitive Personal Data** | No Aadhaar, PAN, biometric, or health data stored. Only: `user_id`, `upi_id`, transaction metadata. |
| **Behavioural anchors only** | `avg_tx_amount`, `std_tx_amount`, `tx_count` — statistical aggregates, not raw transaction logs. |
| **Local ASN resolution** | MMDB lookup is entirely offline. No external API calls. No IP-to-person mapping. |
| **Data minimisation** | Transaction nodes store only: `tx_id`, `amount`, `timestamp`, `channel`, `status`, `risk_score`. |

### Compliance Notes

- **DPDP Act (2023):** The system processes only transactional metadata and derived statistical properties. No "Sensitive Personal Data" as defined under Section 2(1)(z) of the Information Technology Act.
- **RBI Guidelines:** Risk scoring operates post-authentication. Balance checks happen at the NPCI switch layer, not in the fraud engine.
- **Data Retention:** Transaction nodes can be pruned via the `MAINT_CLEAR_ALL` query or time-windowed Cypher deletes.

---

## Summary of All Mathematical Formulas

### Risk Fusion

\`\`\`
R = 0.30·S_graph + 0.25·S_behavioral + 0.20·S_device + 0.15·S_dead_account + 0.10·S_velocity
\`\`\`

### Behavioural Sub-Score

\`\`\`
S_b = min( min(|z|·10, 30) + V·20 + 𝟙[IT]·20 + 𝟙[N]·5 + min(D_M·2, 15)
         + 𝟙[Sp]·10 + 𝟙[DB]·15 + R_ASN·20 + 𝟙[¬SIM]·10, 100 )
\`\`\`

### ASN Risk (8-Step)

\`\`\`
R_ASN = clamp(0.4·B + 0.3·D̂ + 0.2·δ + 0.2·F + 0.1·Ĥ, 0, 1)
\`\`\`

Where B = base score, D̂ = ln(1+N)/ln(1001), δ = drift, F = foreign flag, Ĥ = H/2.5.

### Haversine Distance

\`\`\`
d = 2R · arctan2(√(sin²(Δφ/2) + cos(φ₁)·cos(φ₂)·sin²(Δλ/2)), √(1-a))
\`\`\`

### Mahalanobis Distance

\`\`\`
D_M = √((x - μ)ᵀ · Σ⁻¹ · (x - μ))
\`\`\`

### ASN Switching Entropy

\`\`\`
H = -Σ pᵢ · ln(pᵢ)
\`\`\`

### Louvain Modularity

\`\`\`
Q = (1/2m) Σ_ij [A_ij - (k_i·k_j)/(2m)] · δ(c_i, c_j)
\`\`\`

### PageRank

\`\`\`
PR(v) = (1-d)/N + d · Σ_{u∈B_v} PR(u)/L(u)
\`\`\`

### F1-Score

\`\`\`
F₁ = 2·P·R / (P + R)
\`\`\`

---

*Built for the Indian UPI ecosystem. Designed for 500 TPS. Every transaction scored in under 200 ms.*

