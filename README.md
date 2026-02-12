
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

```
                               ┌──────────────────────────────────────────┐
                               │            UPI Gateway / API             │
                               │          POST /api/transaction           │
                               │  • Input validation • Auth • Rate limit  │
                               └───────────────┬──────────────────────────┘
                                               │
                                               ▼
                               ┌──────────────────────────────────────────┐
                               │              Redis Streams               │
                               │        (High-throughput transaction bus) │
                               │   • Ordered ingestion • Backpressure     │
                               └───────────────┬──────────────────────────┘
                                               │
                        ┌──────────────────────┼──────────────────────┐
                        │                      │                      │
                        ▼                      ▼                      ▼
               ┌────────────────┐     ┌────────────────┐     ┌────────────────┐
               │   Worker 0     │     │   Worker 1     │     │   Worker N     │
               │  (Async Engine)│     │  (Async Engine)│     │  (Async Engine)│
               │ ┌────────────┐ │     │                │     │                │
               │ │  Ingestion │ │     │      ...       │     │      ...       │
               │ │ (Neo4j Tx) │ │     │                │     │                │
               │ ├────────────┤ │     │                │     │                │
               │ │ IPv4 ASN   │ │     │                │     │                │
               │ │ MMDB Lookup│ │     │                │     │                │
               │ ├────────────┤ │     │                │     │                │
               │ │  Risk      │ │     │                │     │                │
               │ │  Scoring   │ │     │                │     │                │
               │ └────────────┘ │     │                │     │                │
               └────────────────┘     └────────────────┘     └────────────────┘
                        │                      │                      │
                        └──────────────────────┴──────────────────────┘
                                               │
                                               ▼
       ┌────────────────────────────────────────────────────────────────────┐
       │                         Neo4j Graph Database                       │
       │  Nodes: :User  :Device  :IP  :Transaction  :Cluster               │
       │  Edges:  :USES  :SENT_TO  :CONNECTED_FROM  :MEMBER_OF            │
       │  • Real-time writes • Indexed identities • Graph projections      │
       └────────────────────────────────────────────────────────────────────┘
                                               │
                                               ▼
       ┌────────────────────────────────────────────────────────────────────┐
       │                 Background Graph Intelligence (Every 5s)          │
       │  • Louvain (Community Detection)                                  │
       │  • Betweenness Centrality                                          │
       │  • PageRank                                                        │
       │  • Clustering Coefficient                                          │
       │  • Weakly Connected Components (WCC)                              │
       │  • Collusive Ring Detection                                        │
       │  • User Behavioral Aggregations                                    │
       │  • Device Count & Identity Refresh                                 │
       │  • Dormant Account Flagging                                        │
       └────────────────────────────────────────────────────────────────────┘
                                               │
                                               ▼
       ┌────────────────────────────────────────────────────────────────────┐
       │                Real-Time Alerting & Monitoring Layer               │
       │        WebSocket: ws://host/ws/alerts                              │
       │  • Risk threshold triggers                                         │
       │  • Explainable feature breakdown                                   │
       │  • Dashboard push updates                                          │
       └────────────────────────────────────────────────────────────────────┘
```

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

---

## Hot Path (Per Transaction • Target < 200 ms End-to-End)

```
Incoming Transaction
        │
        ▼
Redis XADD (append to stream)
        │
        ▼
Worker → XREADGROUP (consumer group)
        │
        ▼
Step 1: Graph Ingestion (Neo4j Write)
        • MATCH-based write (lock-minimized)
        • Fallback to MERGE if identity not pre-seeded
        │
        ▼
Step 1b: IPv4 ASN Resolution
        • MMDB lookup (local file)
        • Extract: asn, org, country, risk_class
        • Attach IP node + relationships
        │
        ▼
Step 2: Parallel Risk Feature Extraction
        (5 Concurrent Async Extractors)

        ├── BehavioralFeatureExtractor.compute()
        ├── DeadAccountDetector.compute()
        ├── DeviceRiskExtractor.compute()
        ├── GraphIntelligenceExtractor.compute()
        └── VelocityExtractor.compute()

        │
        ▼
Step 3: Weighted Fusion Engine
        • Normalize feature scores
        • Apply weight vector
        • Produce RiskResponse (0–100)

        │
        ▼
Step 4: Risk Write-Back (Fire-and-Forget)
        • Attach risk_score
        • Store feature breakdown

        │
        ▼
Step 5: Real-Time Alert Trigger
        • If risk ≥ 40 → Push via WebSocket

        │
        ▼
Step 6: Redis Stream ACK
        • XACK after successful completion
```

---

## Cold Path (Graph Intelligence Loop • Every 5 Seconds)

```
GraphAnalyzer._loop()
        │
        ├── BATCH_UPDATE_USER_STATS
        │     • Recompute avg_amount
        │     • Recompute std_dev
        │     • Update txn_counts
        │
        ├── BATCH_UPDATE_DEVICE_STATS
        │     • Recalculate account_count per device
        │
        ├── QUERY_FLAG_DORMANT_ACCOUNTS
        │     • Flag users inactive > 30 days
        │
        ├── GDS_DROP_PROJECTION
        ├── GDS_CREATE_PROJECTION
        │
        ├── GDS_LOUVAIN
        ├── GDS_BETWEENNESS
        ├── GDS_PAGERANK
        ├── GDS_LOCAL_CLUSTERING
        │
        └── CollusiveFraudDetector.refresh()
              • Execute all 6 collusion pattern queries
              • Update cluster risk signals
```

---

## Deadlock Mitigation Strategy (Neo4j Write Retries)

```python
_MAX_RETRIES = 3
_BASE_BACKOFF_SEC = 0.02  # 20 ms

for attempt in range(_MAX_RETRIES):
    try:
        await neo4j.write_async(INGEST_TRANSACTION, params)
        break
    except TransientException:
        backoff = _BASE_BACKOFF_SEC * (2 ** attempt) + random(0, 0.01)
        await asyncio.sleep(backoff)
```

### Strategy Notes

• Exponential backoff reduces lock contention
• Random jitter prevents synchronized retry storms
• Retries limited to protect latency SLO
• ACK only after successful write + scoring


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

---

## 4.1 Master Risk Formula

```
R = w_g · S_graph
  + w_b · S_behavioral
  + w_d · S_device
  + w_a · S_dead_account
  + w_v · S_velocity
```

Where:

• Each sub-score S_i ∈ [0, 100]
• Final score is capped:

```
R_final = min(R, 100)
```

This ensures bounded risk output suitable for deterministic alert thresholds.

---

## 4.2 Weight Configuration

| Component          | Symbol | Default Weight | Rationale                                 |
| ------------------ | ------ | -------------- | ----------------------------------------- |
| Graph Intelligence | w_g    | **0.30**       | Network topology is strongest mule signal |
| Behavioural        | w_b    | **0.25**       | Amount anomaly + geo + ASN intelligence   |
| Device Risk        | w_d    | **0.20**       | Shared hardware / emulator patterns       |
| Dead Account       | w_a    | **0.15**       | Dormant reactivation fraud                |
| Velocity           | w_v    | **0.10**       | Burst / pass-through timing               |

Weights are configurable via environment or config file for calibration.

---

## 4.3 Risk Level Classification

```
HIGH     → R ≥ 70
MEDIUM   → 40 ≤ R < 70
LOW      → R < 40
```

These thresholds align with WebSocket alert trigger (≥ 40).

---

## 4.4 Concurrent Scoring Pipeline

All extractors execute concurrently using asyncio for latency minimization.

```python
behav, dead, device, graph, vel = await asyncio.gather(
    behavioral.compute(sender_id, amount, timestamp, lat, lon, channel, ip, sim),
    dead_account.compute(sender_id, amount),
    device_risk.compute(device_hash),
    graph_intel.compute(sender_id),
    velocity.compute(sender_id, amount),
)
```

### Post-Scoring Pipeline

1. Flag aggregation across extractors
2. Collusive cluster flag lookup (O(1) cached)
3. Mule heuristic evaluation
4. Deduplication (`dict.fromkeys()` order-preserving)
5. Explainability string generation
6. Persist `risk_score` + `status`
7. WebSocket alert if `risk ≥ 40`

---

# 5. Feature Extractor №1 — Behavioural Intelligence

**File:** `app/features/behavioral.py`

**Input:** sender_id, amount, timestamp, lat/lon, channel, ip_address, sim_verified
**Output:** S_behavioral ∈ [0, 100] + feature dict + flags

---

## 5.1 Amount Z-Score (UPI Only • 3σ Rule)

Computed only when `channel == UPI`.

Primary path (≥ 2 historical tx):

```
z = (A_t - Ā_25) / σ_25
```

Fallback (thin history):

```
z = (A_t - μ_profile) / σ_profile
σ_profile = max(σ_stored, 0.5 · μ_profile)
```

3σ Spike flag:

```
Spike = 1  if  A_t > Ā + 3σ
```

---

## 5.2 Dormant Burst Signal

```
DormantBurst = 1
if is_dormant AND μ_profile > 0 AND A_t > μ_profile
```

---

## 5.3 ASN Intelligence (MMDB Based)

Local IPv4 MMDB lookup.
Returns scaled ASN risk ∈ [0, 20].

---

## 5.4 SIM Verification Risk

```
SIM_Risk = 10   if sim_verified == False
           0    otherwise
```

---

## 5.5 Velocity Feature

```
Δt = t_current - t_last
```

Burst window (60s):

```
V = min(recent_tx_count / B_burst, 1.0)
```

Where B_burst = 10.

---

## 5.6 Night-Time Flag

```
Night = 1  if hour ≥ 23 OR hour ≤ 5
```

---

## 5.7 Geo Distance & Impossible Travel

Haversine formula:

```
d = 2R · arctan2(√a, √(1-a))

a = sin²(Δφ/2) + cos(φ₁) · cos(φ₂) · sin²(Δλ/2)
```

R = 6371 km.

Impossible travel condition:

```
ImpossibleTravel = 1
if (d / (Δt / 3600)) > 250 km/h
```

250 km/h chosen as realistic domestic upper bound.

---

## 5.8 Mahalanobis Outlier Score

Applicable when ≥ 5 historical tx.

```
D_M(x) = √((x - μ)ᵀ Σ⁻¹ (x - μ))
```

x = [amount, Δt]

---

## 5.9 Behavioural Risk Aggregation

```
S_behavioral = min(
    min(|z|·10, 30)
  + V·20
  + 𝟙[IT]·20
  + 𝟙[Night]·5
  + min(D_M·2, 15)
  + 𝟙[Spike]·10
  + 𝟙[DormantBurst]·15
  + R_ASN
  + 𝟙[¬SIM]·10
, 100)
```

| Component         | Max Points |
| ----------------- | ---------- |
| Amount z-score    | 30         |
| Velocity          | 20         |
| Impossible travel | 20         |
| Mahalanobis       | 15         |
| Dormant burst     | 15         |
| 3σ spike          | 10         |
| SIM risk          | 10         |
| ASN risk          | 20         |
| Night flag        | 5          |

Raw total may exceed 100 but is capped.

---

# 6. Feature Extractor №2 — Graph Intelligence

**File:** `app/features/graph_intelligence.py`

**Input:** user_id
**Output:** S_graph ∈ [0, 100] + features + flags

---

## 6.1 Precomputed GDS Properties

| Property         | Source Algorithm       | Update Cycle |
| ---------------- | ---------------------- | ------------ |
| community_id     | Louvain                | 5s batch     |
| betweenness      | Betweenness Centrality | 5s batch     |
| pagerank         | PageRank               | 5s batch     |
| clustering_coeff | Local Clustering       | 5s batch     |

These values are read directly from the user node during hot-path scoring (O(1)).


# 6. Feature Extractor №2 — Graph Intelligence (Continued)

---

## 6.2 Community Risk

If `community_id` exists, execute `QUERY_COMMUNITY_STATS`.

```
CommunityRisk =
    min(avg_risk_cluster, 100)
        if members ≥ 3 AND avg_risk_cluster > 50

    40
        if high_risk_count ≥ 2

    0
        otherwise
```

This captures collusive mule clusters and coordinated fraud rings.

---

## 6.3 Betweenness Centrality Score

```
CentralityScore = min(b · 200, 30)
```

Where:

• b = raw betweenness centrality
• In ~500-node graphs, b typically peaks near 0.1
• Scaling factor (200) normalizes into 0–30 band

High centrality indicates brokerage behavior (money router / bridge node).

---

## 6.4 PageRank Score

```
PageRankScore = min(PR · 500, 15)
```

Higher PageRank implies structural importance in the transfer graph.

Capped at 15 to prevent dominance over community-level signals.

---

## 6.5 Structural Anomaly Patterns

| Pattern               | Detection Rule                              | Points |
| --------------------- | ------------------------------------------- | ------ |
| Fan-Out (Distributor) | out_degree ≥ 5 AND in_degree ≤ 2            | +15    |
| Fan-In (Collector)    | in_degree ≥ 5 AND out_degree ≤ 2            | +15    |
| Tight Ring            | clustering_coeff > 0.5 AND total_degree > 4 | +10    |

StructuralScore = sum of triggered pattern points.

---

## 6.6 Neighbour Risk Contagion

```
Contagion = min(avg_neighbour_risk · 0.3, 15)
```

Captures first-degree fraud proximity influence.

---

## 6.7 Graph Risk Fusion

```
S_graph = min(
      0.30 · CommunityRisk
    + CentralityScore
    + PageRankScore
    + StructuralScore
    + Contagion
, 100)
```

Community risk is weighted to avoid over-amplification.

---

# 7. Feature Extractor №3 — Device Risk

**File:** `app/features/device_risk.py`

**Input:** device_hash
**Output:** S_device ∈ [0, 100] + features + flags

---

## 7.1 Multi-Account Exposure

```
MultiAccount =
    40   if N_accounts ≥ 5
    25   if N_accounts ≥ 3
    10   if N_accounts ≥ 2
    0    otherwise
```

---

## 7.2 Emulator Detection

```
Emulator = 25   if device_is_emulator
           0    otherwise
```

---

## 7.3 Device Risk Propagation

From `QUERY_DEVICE_RISK_PROPAGATION`:

```
Propagation = min(R_device / 100, 1) · 25
```

Device base risk (Cypher logic):

| Condition          | Device Score        |
| ------------------ | ------------------- |
| user_count ≥ 5     | 100                 |
| user_count ≥ 3     | 70                  |
| max_user_risk > 80 | 60                  |
| Default            | avg_user_risk · 0.5 |

---

## 7.4 High-Risk Neighbour Bonus

```
HighRiskBonus = 10   if max(user_risk) > 80
                0    otherwise
```

---

## 7.5 OS Anomaly Detection (UPI Constraint)

UPI apps are valid only on Android and iOS.

```
OSAnomaly = 15   if OS ∉ {Android*, iOS*}
            0    otherwise
```

---

## 7.6 Device Risk Fusion

```
S_device = min(
    MultiAccount
  + Emulator
  + Propagation
  + HighRiskBonus
  + OSAnomaly
, 100)
```

| Component       | Max Points |
| --------------- | ---------- |
| Multi-Account   | 40         |
| Emulator        | 25         |
| Propagation     | 25         |
| High-Risk Bonus | 10         |
| OS Anomaly      | 15         |

Raw maximum = 115 (capped to 100).

---

# 8. Feature Extractor №4 — Dead Account Detection

**File:** `app/features/dead_account.py`
**Input:** user_id, tx_amount
**Output:** S_dead ∈ [0, 100] + features + flags

---

## Detection Philosophy

Dormant accounts (inactive > 30 days) that suddenly initiate large transfers are strong mule-activation indicators in layered fraud.

---

## 8.1 First-Strike Optimized Path (Single Neo4j Round-Trip)

`QUERY_DORMANT_WAKEUP` retrieves inactivity duration + recent activity in one query.

```cypher
WITH u,
     duration.between(u.last_active, datetime()).days AS days_slept
OPTIONAL MATCH (u)-[:SENT]->(tx:Transaction)
  WHERE tx.timestamp > datetime() - duration({hours: 1})
...
CASE
  WHEN days_slept > $dormant_days AND recent_tx_count > 0
  THEN true ELSE false
END AS is_first_strike
```

---

## 8.2 Inactivity Score

```
Inactivity = min(days_slept / 30, 1) · 30
```

Max contribution = 30.

---

## 8.3 Spike Score

```
Spike =
    min((A_t / Ā_profile) / 10, 1) · 30    if Ā_profile > 0

    25                                      if no history AND A_t > 5000

    0                                       otherwise
```

---

## 8.4 First-Strike Bonus

```
FirstStrike =
    25   if first_strike AND volume_spike
    20   if first_strike only
    0    otherwise
```

---

## 8.5 Low Activity Bonus

```
LowActivity = 10   if N_tx ≤ 3
              0    otherwise
```

---

## 8.6 Dead Account Risk Fusion

```
S_dead =
    min(Inactivity + Spike + FirstStrike + LowActivity, 100)
        if dormant OR first_strike

    Spike · 0.3
        otherwise
```

---

## 8.7 Legacy Fallback Path (Two Queries)

If optimized query fails:

1. `QUERY_DORMANT_STATUS`
2. `QUERY_RECENT_INFLOW_OUTFLOW`

### Pass-Through Ratio (Legacy Path)

```
PT = outflow_window / inflow_window
PassThroughScore = min(PT / 0.80, 1) · 30
```

---

# 9. Feature Extractor №5 — Velocity & Pass-Through

**File:** `app/features/velocity.py`
**Input:** user_id, tx_amount
**Output:** S_velocity ∈ [0, 100] + features + flags

---

## 9.1 Burst Detection

Let activity = send_count + receive_count (60s window).

```
Burst =
    30   if activity ≥ 10
    15   if activity ≥ 5
    0    otherwise
```

---

## 9.2 Pass-Through Score

```
r = total_sent_window / total_received_window

PassThrough =
    min(r / 1.5, 1) · 35   if r > 0.80
    10                     if r > 0.50
    0                      otherwise
```

---

## 9.3 Velocity Component

```
VelocityComponent = min(activity / 10, 1) · 20
```

---

## 9.4 Single Transaction Ratio

```
SingleTxRatio = 15   if A_t / total_sent > 0.80
                0    otherwise
```

---

## 9.5 Velocity Risk Fusion

```
S_velocity = min(
    Burst
  + PassThrough
  + VelocityComponent
  + SingleTxRatio
, 100)
```

| Component       | Max Points |
| --------------- | ---------- |
| Burst           | 30         |
| Pass-Through    | 35         |
| Velocity        | 20         |
| Single Tx Ratio | 15         |

---

# 10. Indian IPv4 ASN Intelligence (8-Step Pipeline)

**File:** `app/features/asn_intelligence.py`
**Data Source:** Local MMDB (IPv4 only)
**Output:** asn_risk ∈ [0,1] → scaled to [0,20]

---

## Step 1 — IPv4 Validation

```
Valid(IP) = 1
if IPv4 AND NOT Private AND NOT Loopback
   AND NOT Reserved AND NOT LinkLocal
```

Else → ASN = NULL, risk = 0.

---

## Step 2 — ASN Extraction (MMDB)

```python
reader = maxminddb.open_database("asn_ipv4_small.mmdb")
data = reader.get(ip_address)
```

IP → (ASN_number, Org_name, Country)

---

## Step 3 — Indian Filter

```
ForeignFlag = 1   if Country ≠ "IN"
              0   otherwise
```

---

## Step 4 — ASN Classification

Tier 1: Curated ASN maps (O(1) lookup)
Tier 2: Keyword fallback on organization name
Foreign ASNs → Class = FOREIGN

### Base Risk

```
ASN_base =
    0.0   Mobile ISP
    0.1   Broadband
    0.3   Enterprise
    0.6   Indian Cloud
    0.7   Hosting
    0.5   Unknown (Indian)
    0.8   Foreign
```

---

## Step 5 — ASN Density

```
ASN_density = ln(1 + N_accounts_in_ASN)
ASN_density_norm = min(ln(1 + N) / 6.909, 1)
```

---

## Step 6 — ASN Drift

```
ASN_drift = 1   if ASN_t ≠ ASN_mode
            0   otherwise
```


# 10. Indian IPv4 ASN Intelligence (Continued)

---

## Step 7 — ASN Switching Entropy

Let p_i be the probability distribution over historical ASNs used by the user.

```
H_ASN = -Σ p_i · ln(p_i)
```

Higher entropy → frequent ASN switching → infrastructure hopping.

### Normalisation

```
H_norm = min(H_ASN / 2.5, 1)
```

2.5 ≈ ln(12), practical upper bound for distinct ASNs per normal user.

---

## Step 8 — Final ASN Risk Fusion

```
ASN_risk =
      0.4 · ASN_base
    + 0.3 · ASN_density_norm
    + 0.2 · ASN_drift
    + 0.2 · ForeignFlag
    + 0.1 · H_norm

ASN_risk = clamp(ASN_risk, 0, 1)
ASN_risk_scaled = ASN_risk · 20
```

Maximum behavioural contribution = 20 points.

---

# 11. Collusive Fraud Detection (Batch Engine)

**File:** `app/detection/collusive_fraud.py`
**Execution Interval:** 5 seconds
**Hot-Path Access:** In-memory cache (O(1) lookup)

---

## Detection Pattern Matrix

| Pattern        | Description                                    | Key Parameter        |
| -------------- | ---------------------------------------------- | -------------------- |
| Fraud Islands  | Louvain clusters with ≥3 users & avg risk > 40 | min_avg_risk=40      |
| Money Routers  | High betweenness nodes                         | min_betweenness=0.01 |
| Circular Flows | A→B→C→A cycles (7 days)                        | rolling window       |
| Rapid Chains   | 2–4 hop transfers < 300s gap                   | layered timing       |
| Star Hubs      | High fan-in / fan-out structures               | degree ≥ 5           |
| Relay Mules    | outflow/inflow > 0.75 within 10 min            | flow ratio           |

---

## Circular Flow Pattern

```cypher
MATCH (a:User)-[r1:TRANSFERRED_TO]->(b:User)
      -[r2:TRANSFERRED_TO]->(c:User)
      -[r3:TRANSFERRED_TO]->(a)
WHERE a <> b AND b <> c AND a <> c
  AND r1.last_tx > datetime() - duration({days: 7})
```

---

## Rapid Chain Detection

```cypher
MATCH path = (start:User)-[:TRANSFERRED_TO*2..4]->(finish:User)
WHERE ALL(i IN range(0, size(timestamps)-2)
  WHERE duration.between(timestamps[i], timestamps[i+1]).seconds < 300)
```

---

## O(1) Hot-Path Lookup

```python
def get_user_flags(self, user_id: str):
    clusters = self._user_clusters.get(user_id, set())
    is_relay = user_id in self._relay_mule_ids
```

---

# 12. Mule Account Classification Engine

**File:** `app/detection/mule_detection.py`
**Execution:** Per-transaction
**Output:** {is_mule: bool, confidence: float, reasons: list}

---

## Signal Accumulator

| Signal               | Score |
| -------------------- | ----- |
| First-strike dormant | +0.30 |
| Dormant activation   | +0.25 |
| High pass-through    | +0.20 |
| Shared device        | +0.15 |
| Emulator             | +0.10 |
| High-risk cluster    | +0.15 |
| Relay pattern        | +0.10 |
| Impossible travel    | +0.10 |
| Amount spike         | +0.05 |

---

## Classification Rule

```
IsMule = (score ≥ 0.5) OR (R_fused ≥ 65)
```

---

# 13. Graph Data Science (Batch Layer)

**File:** `app/core/graph_analyzer.py`
**Projection:** 'fraud-graph'
**Interval:** 5 seconds

---

## Algorithm Pipeline

| Order | Algorithm        | Property Written |
| ----- | ---------------- | ---------------- |
| 1     | Louvain          | community_id     |
| 2     | Betweenness      | betweenness      |
| 3     | PageRank         | pagerank         |
| 4     | Local Clustering | clustering_coeff |
| 5     | WCC              | component_id     |

---

## Louvain Modularity

```
Q = (1/2m) Σ_ij [A_ij - (k_i·k_j)/(2m)] δ(c_i, c_j)
```

---

## Betweenness Centrality

```
g(v) = Σ_{s≠v≠t} σ_st(v) / σ_st
```

---

## PageRank

```
PR(v) = (1-d)/N + d · Σ PR(u)/L(u)
```

Configuration: dampingFactor = 0.85

---

## Local Clustering Coefficient

```
C_i = 2·E_i / (k_i · (k_i - 1))
```

---

## Pre-GDS Batch Aggregation

| Query                       | Purpose                       |
| --------------------------- | ----------------------------- |
| BATCH_UPDATE_USER_STATS     | Refresh rolling avg/std/count |
| BATCH_UPDATE_DEVICE_STATS   | Refresh device account_count  |
| QUERY_FLAG_DORMANT_ACCOUNTS | Mark inactive >30 days        |

Batch layer remains fully decoupled from hot-path scoring.


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

# 15. Anomaly Detection Primitives

**File:** `app/detection/anomaly_detection.py`

Lightweight statistical utilities reused across feature extractors.

---

## Z-Score

```
z = (x - x̄) / σ
```

Returns 0 if:

• len(values) < 2
• σ = 0

---

## IQR Outlier Detection

```
Outlier = 1
if x < Q1 - k·IQR
   OR x > Q3 + k·IQR
```

Default: k = 1.5
Minimum required samples: 4

---

## Rolling Statistics

```
(x̄_w, σ_w) = stats(values[:w])
```

Default window: w = 25

---

## Time Velocity

```
V(t_ref, W) = |{t_i : t_ref - t_i ≤ W}|
```

---

## Burst Condition

```
Burst = 1  if V(t_ref, W) ≥ θ
```

Defaults: θ = 10, W = 60 seconds

---

# 16. Evaluation Metrics

**File:** `app/utils/metrics.py`

---

## Confusion Matrix

```
                Predicted Fraud    Predicted Legit
Actual Fraud         TP                  FN
Actual Legit         FP                  TN
```

---

## Precision

```
P = TP / (TP + FP)
```

---

## Recall (Sensitivity)

```
R = TP / (TP + FN)
```

---

## F1-Score

```
F1 = 2PR / (P + R)
```

---

## False Positive Rate

```
FPR = FP / (FP + TN)
```

---

## Latency & Throughput Metrics

| Metric       | Formula                   |
| ------------ | ------------------------- |
| Mean Latency | L̄ = (1/N) Σ L_i          |
| P95          | percentile(latencies, 95) |
| P99          | percentile(latencies, 99) |
| Throughput   | TPS = N / T_total         |

---

# 17. REST API Reference

**Base URL:** `http://localhost:8000/api`

---

## Endpoints

| Method | Path                 | Description              |
| ------ | -------------------- | ------------------------ |
| GET    | /health              | Service + DB health      |
| POST   | /transaction         | Score single transaction |
| GET    | /dashboard/stats     | Aggregate statistics     |
| GET    | /viz/fraud-network   | Graph nodes + edges      |
| GET    | /viz/device-sharing  | Device clusters          |
| GET    | /detection/collusive | Collusion summary        |
| GET    | /analytics/status    | Batch analytics status   |
| GET    | /db/counts           | Graph counts             |

---

## POST /api/transaction

### Request Model (TransactionInput)

```json
{
  "tx_id": "uuid",
  "sender_id": "U0001",
  "receiver_id": "U0042",
  "amount": 15000.0,
  "timestamp": "2026-02-12T14:30:00",
  "device_hash": "DEV0001",
  "device_os": "Android 14",
  "device_model": "Model-7",
  "device_is_emulator": false,
  "ip_address": "49.36.128.42",
  "sim_verified": true,
  "sender_lat": 19.076,
  "sender_lon": 72.8777,
  "channel": "UPI",
  "upi_id_sender": "user1@upi",
  "upi_id_receiver": "user42@upi"
}
```

### Response Model (RiskResponse)

```json
{
  "tx_id": "a1b2c3d4",
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
  "flags": ["First-Strike Dormant", "ASN Hosting"],
  "reason": "Dormant activation with high ASN risk",
  "processing_time_ms": 87.3,
  "timestamp": "2026-02-12T14:30:00"
}
```

---

# 18. WebSocket Alerts

**URL:** `ws://localhost:8000/ws/alerts`

• Broadcast when risk ≥ 40
• Payload identical to RiskResponse
• Dead connections pruned automatically

---

# 19. Configuration Reference

**File:** `app/config.py`

All parameters overrideable via environment variables.

---

## Core Settings

| Variable | Default |
| -------- | ------- |
| HOST     | 0.0.0.0 |
| PORT     | 8000    |
| DEBUG    | true    |

---

## Neo4j

| Variable            | Default               |
| ------------------- | --------------------- |
| NEO4J_URI           | bolt://localhost:7687 |
| NEO4J_MAX_POOL_SIZE | 50                    |

---

## Redis

| Variable             | Default       |
| -------------------- | ------------- |
| REDIS_STREAM_KEY     | transactions  |
| REDIS_CONSUMER_GROUP | fraud_workers |

---

## Worker Pool

| Variable          | Default |
| ----------------- | ------- |
| WORKER_COUNT      | 4       |
| WORKER_BATCH_SIZE | 10      |

---

## Risk Weights (Sum = 1.0)

| Variable            | Default |
| ------------------- | ------- |
| WEIGHT_GRAPH        | 0.30    |
| WEIGHT_BEHAVIORAL   | 0.25    |
| WEIGHT_DEVICE       | 0.20    |
| WEIGHT_DEAD_ACCOUNT | 0.15    |
| WEIGHT_VELOCITY     | 0.10    |

---

## Thresholds

| Variable              | Default |
| --------------------- | ------- |
| HIGH_RISK_THRESHOLD   | 70      |
| MEDIUM_RISK_THRESHOLD | 40      |

---

## Feature Parameters

| Variable               | Default |
| ---------------------- | ------- |
| DORMANT_DAYS_THRESHOLD | 30      |
| VELOCITY_WINDOW_SEC    | 60      |
| BURST_TX_THRESHOLD     | 10      |
| IMPOSSIBLE_TRAVEL_KMH  | 250     |

---

# 20. Deployment Guide

## Prerequisites

• Docker + Docker Compose
• Python 3.11+
• ~4 GB RAM

---

## Quick Start

```bash
docker compose up -d
pip install -r requirements.txt
python scripts/setup_neo4j.py
python scripts/seed_data.py
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

---

## Infrastructure Services

| Service | Image                | Purpose          |
| ------- | -------------------- | ---------------- |
| neo4j   | neo4j:5.15-community | Graph DB + GDS   |
| redis   | redis:7-alpine       | Streams + alerts |

---

## Graceful Degradation

If MMDB file missing:

• ASN module disabled
• asn_risk = 0
• System remains fully operational

---

System is fully modular, horizontally scalable, and hot-path isolated from batch analytics.


# 21. Project Structure

```
fraud-detection-system/
├── README.md
├── backend/
│   ├── docker-compose.yml
│   ├── requirements.txt
│   └── app/
│       ├── __init__.py
│       ├── config.py
│       ├── main.py
│       ├── neo4j_manager.py
│       │
│       ├── api/
│       │   ├── routes.py
│       │   └── websocket.py
│       │
│       ├── core/
│       │   ├── graph_analyzer.py
│       │   ├── risk_engine.py
│       │   └── worker_pool.py
│       │
│       ├── detection/
│       │   ├── anomaly_detection.py
│       │   ├── collusive_fraud.py
│       │   └── mule_detection.py
│       │
│       ├── features/
│       │   ├── asn_intelligence.py
│       │   ├── behavioral.py
│       │   ├── dead_account.py
│       │   ├── device_risk.py
│       │   ├── graph_intelligence.py
│       │   └── velocity.py
│       │
│       ├── models/
│       │   ├── risk_score.py
│       │   └── transaction.py
│       │
│       ├── streaming/
│       │   ├── redis_stream.py
│       │   └── transaction_simulator.py
│       │
│       └── utils/
│           ├── cypher_queries.py
│           └── metrics.py
│
├── scripts/
│   ├── run_simulation.py
│   ├── seed_data.py
│   └── setup_neo4j.py
│
└── frontend/
    └── src/
        └── components/
            └── FraudClusterGraph.tsx
```

### Architectural Separation

• `core/` → orchestration + runtime engine
• `features/` → pure scoring logic (stateless)
• `detection/` → higher-level fraud patterns
• `streaming/` → ingestion layer
• `models/` → Pydantic schemas
• `utils/` → reusable infrastructure helpers

This separation enforces low coupling and high testability.

---

# 22. Privacy & Compliance (DPDP Act Alignment)

## Design Principles

| Principle                  | Implementation                                            |
| -------------------------- | --------------------------------------------------------- |
| No balance storage         | No `balance`, `wealth`, or account ledger values stored   |
| No Sensitive Personal Data | No Aadhaar, PAN, biometrics, health, or financial profile |
| Behavioural anchors only   | Rolling aggregates (`avg`, `std`, `count`)                |
| Offline ASN resolution     | Local MMDB, no third-party API                            |
| Data minimisation          | Store only transaction metadata + risk                    |

---

## Regulatory Positioning

• DPDP Act (2023): System processes transactional metadata and derived statistical features only.
• RBI Alignment: Fraud scoring occurs post-authentication layer.
• Data Retention: Supports time-window pruning via Cypher deletes.

---

# 23. Mathematical Summary

## Global Risk Fusion

```
R = 0.30·S_graph
  + 0.25·S_behavioral
  + 0.20·S_device
  + 0.15·S_dead_account
  + 0.10·S_velocity
```

---

## Behavioural Sub-Score

```
S_b = min(
    min(|z|·10, 30)
  + V·20
  + 𝟙[IT]·20
  + 𝟙[Night]·5
  + min(D_M·2, 15)
  + 𝟙[Spike]·10
  + 𝟙[DormantBurst]·15
  + R_ASN·20
  + 𝟙[¬SIM]·10
, 100)
```

---

## ASN Risk (8-Step Model)

```
R_ASN = clamp(
      0.4·B
    + 0.3·D̂
    + 0.2·δ
    + 0.2·F
    + 0.1·Ĥ
, 0, 1)
```

Where:

• B = ASN_base
• D̂ = ln(1+N)/ln(1001)
• δ = drift flag
• F = foreign flag
• Ĥ = H/2.5

---

## Haversine Distance

```
d = 2R · arctan2(
        √(sin²(Δφ/2) + cos(φ₁)·cos(φ₂)·sin²(Δλ/2)),
        √(1 - a)
    )
```

---

## Mahalanobis Distance

```
D_M = √((x - μ)ᵀ Σ⁻¹ (x - μ))
```

---

## ASN Switching Entropy

```
H = -Σ p_i · ln(p_i)
```

---

## Louvain Modularity

```
Q = (1/2m) Σ_ij [A_ij - (k_i·k_j)/(2m)] δ(c_i, c_j)
```

---

## PageRank

```
PR(v) = (1-d)/N + d Σ PR(u)/L(u)
```

---

## F1 Score

```
F1 = 2PR / (P + R)
```

---

The system now has:

• Full mathematical formalisation
• Clear modular architecture
• Privacy-aware design
• Regulatory positioning
• Production deployment path

This document is structurally equivalent to a technical whitepaper for a fintech-grade fraud intelligence engine.

---

*Built for the Indian UPI ecosystem. Designed for 500 TPS. Every transaction scored in under 200 ms.*

