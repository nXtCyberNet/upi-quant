// ══════════════════════════════════════════════════════════════
// Mock Data Layer — Comprehensive fraud detection mock data
// ══════════════════════════════════════════════════════════════

// ── Types ────────────────────────────────────────────────────

export interface Transaction {
  id: string;
  timestamp: Date;
  senderName: string;
  senderUPI: string;
  receiverName: string;
  receiverUPI: string;
  amount: number;
  status: "SUCCESS" | "FAILED" | "BLOCKED";
  riskScore: number;
  latencyMs: number;
  senderIP: string;
  deviceId: string;
  city: string;
  features: FeatureScores;
  triggeredRules: TriggeredRule[];
  geoEvidence: GeoEvidence;
  behavioralSignature: BehavioralSignature;
  semanticAlert: string;
  probabilityMatrix: ProbabilityMatrixRow[];
}

export interface FeatureScores {
  graph: number;
  behavioral: number;
  device: number;
  deadAccount: number;
  velocity: number;
}

export interface TriggeredRule {
  severity: "CRITICAL" | "WARNING" | "INFO";
  rule: string;
  detail: string;
  scoreImpact: number;
}

export interface GeoEvidence {
  deviceGeo: { city: string; lat: number; lng: number };
  ipGeo: { city: string; lat: number; lng: number };
  distanceKm: number;
  timeDeltaMin: number;
  speedKmh: number;
  isImpossible: boolean;
}

export interface BehavioralSignature {
  amountEntropy: number;       // 0-100
  fanInRatio: number;          // 0-100
  temporalAlignment: number;   // 0-100 (circadian regularity)
  deviceAging: number;         // 0-100 (trust via age)
  networkDiversity: number;    // 0-100 (ASN hopping)
  velocityBurst: number;       // 0-100 (tx velocity spike)
  circadianBitmask: number;    // 0-100 (hour-of-day consistency)
  ispConsistency: number;      // 0-100 (ISP/ASN switching rate)
}

export interface ProbabilityMatrixRow {
  category: string;
  rawValue: string;
  weight: number;
  weightedScore: number;
  scenario: string;
}

export interface GraphNode {
  id: string;
  name: string;
  upi: string;
  type: "user" | "mule" | "aggregator";
  riskScore: number;
  fanIn: number;
  fanOut: number;
  betweennessCentrality: number;
  pageRank: number;
  deviceCount: number;
  city: string;
  lastActive: Date;
  isFlagged: boolean;
  isBlocked: boolean;
  cluster?: number;
  cycleDetected: boolean;
  localClusterCoeff: number;
}

export interface GraphEdge {
  source: string;
  target: string;
  amount: number;
  count: number;
  timestamp: Date;
  is3Hop: boolean;
}

export interface SystemHealth {
  neo4j: {
    activeConnections: number;
    idleConnections: number;
    avgQueryMs: number;
    nodesCount: number;
    relsCount: number;
  };
  redis: {
    streamDepth: number;
    lagMs: number;
    memoryUsedMB: number;
    pendingMessages: number;
  };
  workers: {
    active: number;
    total: number;
    cpuPercent: number;
    ramPercent: number;
    processedPerSec: number;
  };
  tps: number;
  meanLatencyMs: number;
  uptime: string;
  graphAnalytics: {
    modularity: number;
    clusters: number;
    bfsLatencyMs: number;
  };
  redisWindow: {
    windowSec: number;
    eventsInWindow: number;
  };
}

export interface AggregatorNode {
  id: string;
  name: string;
  upi: string;
  betweennessCentrality: number;
  pageRank: number;
  fanIn: number;
  fanOut: number;
  totalVolume: number;
  riskScore: number;
  flaggedAt: Date;
  cluster: number;
  deviceCount: number;
}

export interface ASNEntry {
  asn: string;
  provider: string;
  txCount: number;
  riskTxCount: number;
  percentage: number;
  isRisky: boolean;
}

export interface DeviceCluster {
  deviceId: string;
  userCount: number;
  users: string[];
  firstSeen: Date;
  lastSeen: Date;
  riskScore: number;
}

export interface LatencyBucket {
  index: number;
  latencyMs: number;
  timestamp: Date;
}

// ── Indian Names & UPI Banks ─────────────────────────────────

const FIRST_NAMES = [
  "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Reyansh", "Sai",
  "Arnav", "Dhruv", "Kabir", "Ananya", "Diya", "Saanvi", "Prisha",
  "Isha", "Anika", "Riya", "Neha", "Meera", "Tanvi", "Rohan", "Karan",
  "Nikhil", "Rahul", "Amit", "Priya", "Sneha", "Kavya", "Pooja", "Shreya",
  "Rajesh", "Suresh", "Mohit", "Gaurav", "Sanjay", "Deepak", "Anjali",
  "Swati", "Nisha", "Pallavi", "Manish", "Vikram", "Harsh", "Kunal", "Varun",
  "Jatin", "Tarun", "Sachin", "Ashwin", "Lakshmi",
];

const LAST_NAMES = [
  "Sharma", "Patel", "Singh", "Kumar", "Gupta", "Jain", "Reddy",
  "Verma", "Mehta", "Shah", "Nair", "Pillai", "Iyer", "Rao", "Das",
  "Bose", "Mukherjee", "Banerjee", "Ghosh", "Mishra", "Tiwari", "Pandey",
  "Chauhan", "Yadav", "Thakur", "Malhotra", "Kapoor", "Arora", "Saxena",
  "Agarwal", "Chopra", "Bhatia", "Khanna", "Sinha", "Goyal", "Bajaj",
];

const BANKS = ["oksbi", "okhdfcbank", "okicici", "okaxis", "okybl", "okpaytm"];

const CITIES = [
  "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai",
  "Kolkata", "Pune", "Jaipur", "Ahmedabad", "Lucknow",
  "Surat", "Kanpur", "Nagpur", "Indore", "Bhopal",
];

const CITY_COORDS: Record<string, [number, number]> = {
  "Mumbai": [19.076, 72.878],
  "Delhi": [28.704, 77.103],
  "Bangalore": [12.972, 77.595],
  "Hyderabad": [17.385, 78.487],
  "Chennai": [13.083, 80.271],
  "Kolkata": [22.573, 88.364],
  "Pune": [18.520, 73.857],
  "Jaipur": [26.912, 75.787],
  "Ahmedabad": [23.023, 72.571],
  "Lucknow": [26.847, 80.947],
  "Surat": [21.170, 72.831],
  "Kanpur": [26.449, 80.332],
  "Nagpur": [21.146, 79.089],
  "Indore": [22.720, 75.858],
  "Bhopal": [23.260, 77.413],
};

const ASN_PROVIDERS = [
  { asn: "AS9829", provider: "BSNL", risky: false },
  { asn: "AS55836", provider: "Jio", risky: false },
  { asn: "AS24560", provider: "Airtel", risky: false },
  { asn: "AS45609", provider: "Vodafone-Idea", risky: false },
  { asn: "AS14061", provider: "DigitalOcean", risky: true },
  { asn: "AS16509", provider: "AWS", risky: true },
  { asn: "AS13335", provider: "Cloudflare", risky: true },
  { asn: "AS396982", provider: "Google Cloud", risky: true },
  { asn: "AS45194", provider: "ACT Fibernet", risky: false },
  { asn: "AS17488", provider: "Hathway", risky: false },
  { asn: "AS18209", provider: "Tata Comm", risky: false },
  { asn: "AS9498", provider: "Bharti", risky: false },
];

const DEVICE_IDS = Array.from({ length: 60 }, (_, i) =>
  `dev_${(i + 1).toString().padStart(3, "0")}_${Math.random().toString(36).substring(2, 8)}`
);

// ── Helpers ──────────────────────────────────────────────────

function pick<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randFloat(min: number, max: number): number {
  return Math.random() * (max - min) + min;
}

function generateUPI(): string {
  return `${randInt(1000000000, 9999999999)}@${pick(BANKS)}`;
}

function generateIP(risky = false): string {
  if (risky) {
    // Cloud provider IPs
    return `${pick([104, 172, 35, 13])}.${randInt(0, 255)}.${randInt(0, 255)}.${randInt(1, 254)}`;
  }
  // Indian ISP ranges
  return `${pick([49, 59, 103, 106, 122, 182, 223])}.${randInt(0, 255)}.${randInt(0, 255)}.${randInt(1, 254)}`;
}

function generateName(): string {
  return `${pick(FIRST_NAMES)} ${pick(LAST_NAMES)}`;
}

// ── Transaction Generator ────────────────────────────────────

let txCounter = 0;

export function generateTransaction(forceHighRisk = false): Transaction {
  txCounter++;
  const isHighRisk = forceHighRisk || Math.random() < 0.15;
  const isMediumRisk = !isHighRisk && Math.random() < 0.25;
  const riskScore = isHighRisk
    ? randInt(65, 98)
    : isMediumRisk
    ? randInt(35, 64)
    : randInt(2, 34);

  const amount = isHighRisk
    ? randFloat(15000, 95000)
    : isMediumRisk
    ? randFloat(5000, 25000)
    : randFloat(50, 10000);

  const latencyMs = randInt(18, 280);
  const city = pick(CITIES);

  const features: FeatureScores = {
    graph: isHighRisk ? randInt(50, 95) : randInt(5, 45),
    behavioral: isHighRisk ? randInt(40, 90) : randInt(5, 40),
    device: isHighRisk ? randInt(30, 85) : randInt(5, 35),
    deadAccount: isHighRisk ? randInt(20, 80) : randInt(0, 25),
    velocity: isHighRisk ? randInt(45, 95) : randInt(5, 35),
  };

  const triggeredRules: TriggeredRule[] = [];
  if (isHighRisk) {
    triggeredRules.push(
      { severity: "CRITICAL", rule: "Geo-IP Jump", detail: `${randInt(800, 2200)}km in ${randInt(5, 30)}min`, scoreImpact: randInt(30, 50) },
      { severity: "CRITICAL", rule: "Mule Ring Member", detail: `Cluster #${randInt(1, 8)} — ${randInt(3, 7)} hop chain`, scoreImpact: randInt(25, 45) },
    );
    if (Math.random() > 0.5) {
      triggeredRules.push(
        { severity: "WARNING", rule: "Device Sharing", detail: `${randInt(3, 8)} users on same device`, scoreImpact: randInt(10, 25) },
      );
    }
  } else if (isMediumRisk) {
    triggeredRules.push(
      { severity: "WARNING", rule: "Unusual Hour", detail: `Transaction at ${randInt(1, 5)}:${randInt(10, 59).toString().padStart(2, "0")} AM`, scoreImpact: randInt(10, 20) },
    );
    if (Math.random() > 0.4) {
      triggeredRules.push(
        { severity: "INFO", rule: "Amount Deviation", detail: `Z-Score: ${randFloat(1.5, 3.2).toFixed(2)}σ`, scoreImpact: randInt(5, 15) },
      );
    }
  }

  // Geo evidence
  const deviceCity = city;
  const ipCity = isHighRisk ? pick(CITIES.filter(c => c !== city)) : city;
  const deviceCoords = CITY_COORDS[deviceCity] || [19.076, 72.878];
  const ipCoords = CITY_COORDS[ipCity] || [28.704, 77.103];
  const distanceKm = isHighRisk
    ? randInt(800, 2200)
    : randInt(0, 50);
  const timeDeltaMin = isHighRisk ? randInt(5, 30) : randInt(60, 480);
  const speedKmh = timeDeltaMin > 0 ? Math.round((distanceKm / timeDeltaMin) * 60) : 0;

  const geoEvidence: GeoEvidence = {
    deviceGeo: { city: deviceCity, lat: deviceCoords[0], lng: deviceCoords[1] },
    ipGeo: { city: ipCity, lat: ipCoords[0], lng: ipCoords[1] },
    distanceKm,
    timeDeltaMin,
    speedKmh,
    isImpossible: speedKmh > 250,
  };

  // Behavioral signature (8-axis)
  const behavioralSignature: BehavioralSignature = isHighRisk ? {
    amountEntropy: randInt(5, 25),       // mules have low entropy
    fanInRatio: randInt(60, 95),         // high fan-in
    temporalAlignment: randInt(10, 35),  // irregular hours
    deviceAging: randInt(5, 30),         // new/rotating devices
    networkDiversity: randInt(60, 95),   // ASN hopping
    velocityBurst: randInt(65, 98),      // burst tx patterns
    circadianBitmask: randInt(5, 25),    // off-hours activity
    ispConsistency: randInt(10, 30),     // frequent ISP switching
  } : {
    amountEntropy: randInt(55, 90),      // shopkeepers have high entropy
    fanInRatio: randInt(10, 45),         // normal fan-in
    temporalAlignment: randInt(60, 95),  // regular hours
    deviceAging: randInt(65, 100),       // stable device
    networkDiversity: randInt(10, 40),   // stable ASN
    velocityBurst: randInt(5, 25),       // steady tx patterns
    circadianBitmask: randInt(65, 95),   // daytime activity
    ispConsistency: randInt(70, 95),     // stable ISP
  };

  // Semantic alert
  const alertParts: string[] = [];
  if (isHighRisk) {
    if (speedKmh > 250) alertParts.push(`Geo-Jump ${distanceKm}km (${speedKmh} km/h)`);
    alertParts.push(`${randInt(3, 5)}-Hop Cycle Detected`);
    if (Math.random() > 0.4) alertParts.push(`Device Multiplexing (${randInt(3, 7)} IDs/24h)`);
    if (Math.random() > 0.6) alertParts.push(`Phishing VPA ('${pick(["paytmm", "phonepay", "zomatoo", "gpayy", "swiggyy"])}')`);
    alertParts.push(`Expected: ₹${randInt(200, 800)}, Actual: ₹${Math.round(amount).toLocaleString("en-IN")}`);
  } else if (isMediumRisk) {
    alertParts.push(`Unusual temporal pattern`);
    if (Math.random() > 0.5) alertParts.push(`Amount Z-Score: ${randFloat(1.5, 3.0).toFixed(2)}σ`);
  }
  const semanticAlert = alertParts.length > 0
    ? `ALERT: ${alertParts.join(" | ")}`
    : "";

  // Probability Matrix
  const integrityRaw = features.graph * 0.6 + features.deadAccount * 0.4;
  const geoRaw = geoEvidence.isImpossible ? randInt(75, 98) : randInt(5, 35);
  const behavRaw = (behavioralSignature.amountEntropy + behavioralSignature.fanInRatio + behavioralSignature.temporalAlignment) / 3;
  const phishRaw = isHighRisk && Math.random() > 0.4 ? randInt(60, 95) : randInt(0, 15);
  const tempRaw = features.velocity * 0.7 + (100 - behavioralSignature.circadianBitmask) * 0.3;

  const integrityWeight = 0.25;
  const geoWeight = 0.20;
  const behavWeight = 0.25;
  const phishWeight = 0.15;
  const tempWeight = 0.15;

  const probabilityMatrix: ProbabilityMatrixRow[] = [
    {
      category: "Integrity",
      rawValue: `${integrityRaw.toFixed(1)}/100`,
      weight: integrityWeight,
      weightedScore: Math.round(integrityRaw * integrityWeight * 10) / 10,
      scenario: integrityRaw > 60 ? "High-value chain detected in mule cluster" : integrityRaw > 30 ? "Moderate graph anomaly" : "Normal graph pattern",
    },
    {
      category: "Geo-Spatial",
      rawValue: `${geoRaw}/100 (${geoEvidence.speedKmh} km/h)`,
      weight: geoWeight,
      weightedScore: Math.round(geoRaw * geoWeight * 10) / 10,
      scenario: geoEvidence.isImpossible ? `Impossible travel: ${geoEvidence.distanceKm}km in ${geoEvidence.timeDeltaMin}min` : "Geo-location consistent",
    },
    {
      category: "Behavioral",
      rawValue: `${behavRaw.toFixed(1)}/100`,
      weight: behavWeight,
      weightedScore: Math.round(behavRaw * behavWeight * 10) / 10,
      scenario: behavRaw > 50 ? "Deviant behavioral fingerprint vs baseline" : "Behavioral signature within norms",
    },
    {
      category: "Phishing",
      rawValue: `${phishRaw}/100`,
      weight: phishWeight,
      weightedScore: Math.round(phishRaw * phishWeight * 10) / 10,
      scenario: phishRaw > 50 ? "VPA typosquat / impersonation detected" : "No phishing indicators",
    },
    {
      category: "Temporal",
      rawValue: `${tempRaw.toFixed(1)}/100`,
      weight: tempWeight,
      weightedScore: Math.round(tempRaw * tempWeight * 10) / 10,
      scenario: tempRaw > 50 ? "Off-hours velocity burst + circadian anomaly" : "Normal temporal pattern",
    },
  ];

  return {
    id: `tx_${Date.now()}_${txCounter.toString().padStart(5, "0")}`,
    timestamp: new Date(),
    senderName: generateName(),
    senderUPI: generateUPI(),
    receiverName: generateName(),
    receiverUPI: generateUPI(),
    amount: Math.round(amount * 100) / 100,
    status: isHighRisk && Math.random() > 0.6 ? "BLOCKED" : Math.random() > 0.1 ? "SUCCESS" : "FAILED",
    riskScore,
    latencyMs,
    senderIP: generateIP(isHighRisk && Math.random() > 0.7),
    deviceId: pick(DEVICE_IDS),
    city,
    features,
    triggeredRules,
    geoEvidence,
    behavioralSignature,
    semanticAlert,
    probabilityMatrix,
  };
}

export function generateTransactionBatch(count: number): Transaction[] {
  return Array.from({ length: count }, () => generateTransaction());
}

// Generate transactions attributed to a specific ASN provider
export function generateASNTransactions(provider: string, count = 15): Transaction[] {
  const asnInfo = ASN_PROVIDERS.find((p) => p.provider === provider);
  const isRiskyASN = asnInfo?.risky ?? false;
  return Array.from({ length: count }, () => {
    const tx = generateTransaction(isRiskyASN && Math.random() > 0.3);
    // Tag the transaction with ASN context
    return {
      ...tx,
      id: `asn_${provider.toLowerCase().replace(/[\s-]/g, "_")}_${tx.id}`,
      senderIP: isRiskyASN ? generateIP(true) : generateIP(false),
      semanticAlert: isRiskyASN
        ? `ASN-FLAGGED: Source IP resolves to ${provider} (${asnInfo?.asn}) — ${tx.semanticAlert || "Cloud-sourced transaction"}`
        : tx.semanticAlert,
    };
  });
}

// Generate transactions associated with a specific subgraph node
export function generateNodeTransactions(node: SubgraphNode, count = 10): Transaction[] {
  const isRisky = node.riskScore >= 50 || node.type !== "user";
  return Array.from({ length: count }, () => {
    const tx = generateTransaction(isRisky);
    return {
      ...tx,
      id: `node_${node.id}_${tx.id}`,
      senderName: node.name,
      senderUPI: node.upi || tx.senderUPI,
      city: node.city || tx.city,
      riskScore: isRisky ? randInt(Math.max(0, node.riskScore - 15), Math.min(100, node.riskScore + 15)) : tx.riskScore,
      semanticAlert: node.type === "aggregator"
        ? `AGGREGATOR NODE: ${node.name} — Fan-In: ${node.fanIn}, Fan-Out: ${node.fanOut}, Devices: ${node.deviceCount} | ${tx.semanticAlert}`
        : node.type === "mule"
        ? `MULE NODE: ${node.name} — Risk: ${node.riskScore} | ${tx.semanticAlert}`
        : tx.semanticAlert,
    };
  });
}

// ── Graph Data ───────────────────────────────────────────────

export function generateGraphData(): { nodes: GraphNode[]; edges: GraphEdge[] } {
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];

  // Regular users
  for (let i = 0; i < 40; i++) {
    const name = generateName();
    nodes.push({
      id: `user_${i}`,
      name,
      upi: generateUPI(),
      type: "user",
      riskScore: randInt(2, 35),
      fanIn: randInt(1, 5),
      fanOut: randInt(1, 8),
      betweennessCentrality: randFloat(0, 0.15),
      pageRank: randFloat(0.001, 0.02),
      deviceCount: 1,
      city: pick(CITIES),
      lastActive: new Date(Date.now() - randInt(0, 86400000)),
      isFlagged: false,
      isBlocked: false,
      cycleDetected: false,
      localClusterCoeff: randFloat(0.01, 0.15),
    });
  }

  // Mule accounts
  for (let i = 0; i < 12; i++) {
    const name = generateName();
    nodes.push({
      id: `mule_${i}`,
      name,
      upi: generateUPI(),
      type: "mule",
      riskScore: randInt(60, 95),
      fanIn: randInt(8, 25),
      fanOut: randInt(5, 20),
      betweennessCentrality: randFloat(0.2, 0.7),
      pageRank: randFloat(0.02, 0.08),
      deviceCount: randInt(1, 3),
      city: pick(CITIES),
      lastActive: new Date(Date.now() - randInt(0, 3600000)),
      isFlagged: true,
      isBlocked: false,
      cluster: Math.floor(i / 3),
      cycleDetected: Math.random() > 0.3,
      localClusterCoeff: randFloat(0.15, 0.55),
    });
  }

  // Aggregators (high betweenness centrality)
  for (let i = 0; i < 5; i++) {
    const name = generateName();
    nodes.push({
      id: `agg_${i}`,
      name,
      upi: generateUPI(),
      type: "aggregator",
      riskScore: randInt(80, 98),
      fanIn: randInt(15, 40),
      fanOut: randInt(3, 8),
      betweennessCentrality: randFloat(0.6, 0.95),
      pageRank: randFloat(0.05, 0.15),
      deviceCount: randInt(2, 6),
      city: pick(CITIES),
      lastActive: new Date(Date.now() - randInt(0, 1800000)),
      isFlagged: true,
      isBlocked: i < 2,
      cluster: i,
      cycleDetected: true,
      localClusterCoeff: randFloat(0.4, 0.85),
    });
  }

  // Edges: user → mule → aggregator patterns
  for (let i = 0; i < 40; i++) {
    // Random user-to-user
    if (Math.random() > 0.6) {
      const target = `user_${randInt(0, 39)}`;
      if (target !== `user_${i}`) {
        edges.push({
          source: `user_${i}`,
          target,
          amount: randFloat(100, 5000),
          count: randInt(1, 5),
          timestamp: new Date(Date.now() - randInt(0, 172800000)),
          is3Hop: false,
        });
      }
    }
    // Some users feed into mules
    if (Math.random() > 0.7) {
      edges.push({
        source: `user_${i}`,
        target: `mule_${randInt(0, 11)}`,
        amount: randFloat(2000, 15000),
        count: randInt(1, 8),
        timestamp: new Date(Date.now() - randInt(0, 86400000)),
        is3Hop: false,
      });
    }
  }

  // Mule-to-mule chain
  for (let i = 0; i < 12; i++) {
    const nextMule = (i + 1) % 12;
    edges.push({
      source: `mule_${i}`,
      target: `mule_${nextMule}`,
      amount: randFloat(5000, 30000),
      count: randInt(3, 15),
      timestamp: new Date(Date.now() - randInt(0, 43200000)),
      is3Hop: false,
    });
    // Mule to aggregator
    edges.push({
      source: `mule_${i}`,
      target: `agg_${Math.floor(i / 3) % 5}`,
      amount: randFloat(10000, 50000),
      count: randInt(5, 20),
      timestamp: new Date(Date.now() - randInt(0, 21600000)),
      is3Hop: false,
    });
  }

  // 3-hop connections (dashed lines)
  for (let i = 0; i < 8; i++) {
    edges.push({
      source: `user_${randInt(0, 39)}`,
      target: `agg_${randInt(0, 4)}`,
      amount: randFloat(500, 5000),
      count: 1,
      timestamp: new Date(Date.now() - randInt(0, 172800000)),
      is3Hop: true,
    });
  }

  return { nodes, edges };
}

// ── Top Aggregators ──────────────────────────────────────────

export function generateTopAggregators(): AggregatorNode[] {
  return Array.from({ length: 10 }, (_, i) => ({
    id: `agg_top_${i}`,
    name: generateName(),
    upi: generateUPI(),
    betweennessCentrality: randFloat(0.3, 0.95),
    pageRank: randFloat(0.02, 0.15),
    fanIn: randInt(10, 45),
    fanOut: randInt(3, 15),
    totalVolume: randFloat(100000, 5000000),
    riskScore: randInt(65, 98),
    flaggedAt: new Date(Date.now() - randInt(0, 172800000)),
    cluster: randInt(0, 7),
    deviceCount: randInt(1, 8),
  })).sort((a, b) => b.betweennessCentrality - a.betweennessCentrality);
}

// ── ASN Distribution ─────────────────────────────────────────

export function generateASNData(): ASNEntry[] {
  const total = randInt(8000, 12000);
  return ASN_PROVIDERS.map((p) => {
    const txCount = p.risky ? randInt(50, 400) : randInt(500, 3000);
    return {
      asn: p.asn,
      provider: p.provider,
      txCount,
      riskTxCount: p.risky ? randInt(20, txCount) : randInt(0, Math.floor(txCount * 0.05)),
      percentage: Math.round((txCount / total) * 10000) / 100,
      isRisky: p.risky,
    };
  }).sort((a, b) => b.txCount - a.txCount);
}

// ── Device Clusters ──────────────────────────────────────────

export function generateDeviceClusters(): DeviceCluster[] {
  return Array.from({ length: 15 }, (_, i) => {
    const userCount = i < 5 ? randInt(5, 12) : randInt(1, 4);
    return {
      deviceId: DEVICE_IDS[i] || `dev_${i}`,
      userCount,
      users: Array.from({ length: userCount }, () => generateName()),
      firstSeen: new Date(Date.now() - randInt(86400000, 2592000000)),
      lastSeen: new Date(Date.now() - randInt(0, 86400000)),
      riskScore: userCount > 4 ? randInt(60, 95) : randInt(5, 40),
    };
  }).sort((a, b) => b.userCount - a.userCount);
}

// ── System Health ────────────────────────────────────────────

export function generateSystemHealth(): SystemHealth {
  return {
    neo4j: {
      activeConnections: randInt(8, 18),
      idleConnections: randInt(30, 42),
      avgQueryMs: randInt(28, 65),
      nodesCount: randInt(45000, 52000),
      relsCount: randInt(120000, 150000),
    },
    redis: {
      streamDepth: randInt(0, 15),
      lagMs: randInt(1, 8),
      memoryUsedMB: randInt(80, 180),
      pendingMessages: randInt(0, 5),
    },
    workers: {
      active: randInt(3, 4),
      total: 4,
      cpuPercent: randFloat(15, 65),
      ramPercent: randFloat(25, 55),
      processedPerSec: randInt(380, 520),
    },
    tps: randInt(420, 510),
    meanLatencyMs: randInt(42, 145),
    uptime: "2d 14h 32m",
    graphAnalytics: {
      modularity: randFloat(0.72, 0.92),
      clusters: randInt(8, 18),
      bfsLatencyMs: randInt(28, 58),
    },
    redisWindow: {
      windowSec: 60,
      eventsInWindow: randInt(12000, 16000),
    },
  };
}

// ── Latency Heatmap Data ─────────────────────────────────────

export function generateLatencyHeatmap(): LatencyBucket[] {
  return Array.from({ length: 100 }, (_, i) => ({
    index: i,
    latencyMs: randInt(15, 350),
    timestamp: new Date(Date.now() - (100 - i) * 2000),
  }));
}

// ── Time-series for charts ───────────────────────────────────

export function generateTPSTimeSeries(points = 60): { time: string; tps: number; risk: number }[] {
  return Array.from({ length: points }, (_, i) => ({
    time: `${(points - i) * -1}s`,
    tps: randInt(380, 530),
    risk: randFloat(15, 65),
  }));
}

export function generateRiskDistribution(): { range: string; count: number; color: string }[] {
  return [
    { range: "0-20", count: randInt(3000, 5000), color: "#10b981" },
    { range: "20-40", count: randInt(1500, 3000), color: "#10b981" },
    { range: "40-60", count: randInt(300, 800), color: "#f59e0b" },
    { range: "60-80", count: randInt(80, 300), color: "#ef4444" },
    { range: "80-100", count: randInt(10, 80), color: "#dc2626" },
  ];
}

// ── Real-Time Subgraph (3-Level per Transaction) ─────────────

export interface SubgraphNode {
  id: string;
  name: string;
  upi: string;
  level: 0 | 1 | 2 | 3; // 0=tx-center, 1=sender/receiver, 2=48h recipients, 3=aggregator sinks
  type: "user" | "mule" | "aggregator";
  riskScore: number;
  city: string;
  deviceCount: number;
  fanIn: number;
  fanOut: number;
}

export interface SubgraphEdge {
  source: string;
  target: string;
  amount: number;
  timestamp: Date;
  level: 1 | 2 | 3;
  velocity: number; // ₹/min transfer speed (for marching ants)
}

export interface RealtimeSubgraph {
  txId: string;
  timestamp: Date;
  nodes: SubgraphNode[];
  edges: SubgraphEdge[];
  // Graph-augmented quant metrics
  reachabilityScore: number;     // Total Path Count / Unique Senders at L1+L2
  circularityIndex: number;      // 0-1, probability of wash trade (A→B→C→A loops)
  hopAdjustedVelocity: number;   // avg ₹/min across L1→L3
  cycleDetected: boolean;
  cycleNodes: string[];          // IDs involved in the cycle
  networkPathVelocityMin: number; // L1→L3 time in minutes
  betweennessCentrality: number; // of tx center node
  geoIpConvergence: number;     // 0-1, how many tx share same IP
  identityDensity: number;      // users per device in subgraph
}

export function generateRealtimeSubgraph(tx: Transaction): RealtimeSubgraph {
  const isHighRisk = tx.riskScore >= 60;

  const nodes: SubgraphNode[] = [];
  const edges: SubgraphEdge[] = [];

  // ── Level 0: Transaction center (virtual) ──────────────
  const centerId = `c_${tx.id}`;
  nodes.push({
    id: centerId,
    name: `TX-${tx.id.slice(-5)}`,
    upi: "",
    level: 0,
    type: "user",
    riskScore: tx.riskScore,
    city: tx.city,
    deviceCount: 1,
    fanIn: 0,
    fanOut: 0,
  });

  // ── Level 1: Sender + Receiver ─────────────────────────
  const senderId = `s_${tx.id}`;
  const receiverId = `r_${tx.id}`;
  nodes.push({
    id: senderId,
    name: tx.senderName,
    upi: tx.senderUPI,
    level: 1,
    type: isHighRisk && Math.random() > 0.5 ? "mule" : "user",
    riskScore: isHighRisk ? randInt(40, 85) : randInt(5, 35),
    city: tx.city,
    deviceCount: isHighRisk ? randInt(1, 4) : 1,
    fanIn: randInt(1, 8),
    fanOut: randInt(1, 12),
  });
  nodes.push({
    id: receiverId,
    name: tx.receiverName,
    upi: tx.receiverUPI,
    level: 1,
    type: isHighRisk ? (Math.random() > 0.3 ? "mule" : "aggregator") : "user",
    riskScore: isHighRisk ? randInt(50, 92) : randInt(5, 30),
    city: pick(CITIES),
    deviceCount: isHighRisk ? randInt(1, 3) : 1,
    fanIn: isHighRisk ? randInt(8, 25) : randInt(1, 5),
    fanOut: isHighRisk ? randInt(5, 15) : randInt(1, 5),
  });
  edges.push({
    source: senderId, target: centerId, amount: tx.amount,
    timestamp: tx.timestamp, level: 1,
    velocity: tx.amount / Math.max(1, randInt(1, 10)),
  });
  edges.push({
    source: centerId, target: receiverId, amount: tx.amount,
    timestamp: tx.timestamp, level: 1,
    velocity: tx.amount / Math.max(1, randInt(1, 10)),
  });

  // ── Level 2: Receiver's 48h recipients ─────────────────
  const l2Count = isHighRisk ? randInt(3, 6) : randInt(1, 3);
  const l2Ids: string[] = [];
  for (let i = 0; i < l2Count; i++) {
    const l2Id = `l2_${tx.id}_${i}`;
    l2Ids.push(l2Id);
    const isMule = isHighRisk && Math.random() > 0.4;
    nodes.push({
      id: l2Id,
      name: generateName(),
      upi: generateUPI(),
      level: 2,
      type: isMule ? "mule" : "user",
      riskScore: isMule ? randInt(55, 90) : randInt(5, 40),
      city: pick(CITIES),
      deviceCount: isMule ? randInt(1, 3) : 1,
      fanIn: randInt(1, 10),
      fanOut: randInt(1, 8),
    });
    const fwdAmount = tx.amount * randFloat(0.3, 0.9);
    edges.push({
      source: receiverId, target: l2Id, amount: fwdAmount,
      timestamp: new Date(tx.timestamp.getTime() + randInt(60000, 3600000)),
      level: 2,
      velocity: fwdAmount / Math.max(1, randInt(2, 30)),
    });
  }

  // ── Level 3: Aggregator sinks ──────────────────────────
  const l3Count = isHighRisk ? randInt(2, 4) : randInt(0, 1);
  const l3Ids: string[] = [];
  for (let i = 0; i < l3Count; i++) {
    const l3Id = `l3_${tx.id}_${i}`;
    l3Ids.push(l3Id);
    nodes.push({
      id: l3Id,
      name: generateName(),
      upi: generateUPI(),
      level: 3,
      type: "aggregator",
      riskScore: randInt(70, 98),
      city: pick(CITIES),
      deviceCount: randInt(2, 6),
      fanIn: randInt(15, 40),
      fanOut: randInt(2, 8),
    });
    // Connect from random L2 node
    const sourceL2 = pick(l2Ids);
    const sinkAmount = tx.amount * randFloat(0.2, 0.7);
    edges.push({
      source: sourceL2, target: l3Id, amount: sinkAmount,
      timestamp: new Date(tx.timestamp.getTime() + randInt(120000, 7200000)),
      level: 3,
      velocity: sinkAmount / Math.max(1, randInt(5, 60)),
    });
  }

  // ── Cycle injection (for high-risk): L3 → L1 back-edge ─
  let cycleDetected = false;
  const cycleNodes: string[] = [];
  if (isHighRisk && l3Ids.length > 0 && Math.random() > 0.3) {
    cycleDetected = true;
    const cycleSource = pick(l3Ids);
    const cycleTarget = senderId;
    edges.push({
      source: cycleSource, target: cycleTarget,
      amount: tx.amount * randFloat(0.1, 0.5),
      timestamp: new Date(tx.timestamp.getTime() + randInt(300000, 14400000)),
      level: 3,
      velocity: tx.amount * 0.3 / Math.max(1, randInt(10, 60)),
    });
    cycleNodes.push(senderId, receiverId, pick(l2Ids), cycleSource);
  }

  // ── Compute graph-augmented quant metrics ──────────────
  const uniqueSendersL1L2 = new Set([senderId, ...l2Ids]).size;
  const totalPaths = edges.length;
  const reachabilityScore = uniqueSendersL1L2 > 0 ? totalPaths / uniqueSendersL1L2 : 0;

  const circularityIndex = cycleDetected ? randFloat(0.75, 0.95) : randFloat(0, 0.15);

  // Hop-adjusted velocity: avg time for ₹1 to traverse L1→L3
  const l1ToL3Edges = edges.filter(e => e.level >= 2);
  const avgVelocity = l1ToL3Edges.length > 0
    ? l1ToL3Edges.reduce((s, e) => s + e.velocity, 0) / l1ToL3Edges.length
    : 0;

  // Network Path Velocity: time from L1 edge to L3 edge
  const l1Time = Math.min(...edges.filter(e => e.level === 1).map(e => e.timestamp.getTime()));
  const l3Time = l3Ids.length > 0
    ? Math.max(...edges.filter(e => e.level === 3).map(e => e.timestamp.getTime()))
    : l1Time;
  const networkPathVelocityMin = (l3Time - l1Time) / 60000;

  const betweennessCentrality = isHighRisk ? randFloat(0.3, 0.85) : randFloat(0.01, 0.2);
  const geoIpConvergence = isHighRisk ? randFloat(0.5, 0.9) : randFloat(0.05, 0.3);
  const identityDensity = isHighRisk ? randFloat(2.5, 6) : randFloat(1, 1.5);

  return {
    txId: tx.id,
    timestamp: tx.timestamp,
    nodes,
    edges,
    reachabilityScore: Math.round(reachabilityScore * 100) / 100,
    circularityIndex: Math.round(circularityIndex * 100) / 100,
    hopAdjustedVelocity: Math.round(avgVelocity * 100) / 100,
    cycleDetected,
    cycleNodes,
    networkPathVelocityMin: Math.round(networkPathVelocityMin * 10) / 10,
    betweennessCentrality: Math.round(betweennessCentrality * 1000) / 1000,
    geoIpConvergence: Math.round(geoIpConvergence * 100) / 100,
    identityDensity: Math.round(identityDensity * 10) / 10,
  };
}

export { CITY_COORDS };
