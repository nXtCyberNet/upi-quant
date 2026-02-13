"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import {
  generateTransaction,
  generateSystemHealth,
  generateLatencyHeatmap,
  type Transaction,
  type SystemHealth,
  type LatencyBucket,
} from "@/lib/mock-data";

const MAX_TRANSACTIONS = 200;
const STREAM_INTERVAL_MS = 150; // ~6-7 TPS for visual smoothness

// Stable initial values to avoid hydration mismatch
const INITIAL_HEALTH: SystemHealth = {
  neo4j: { activeConnections: 0, idleConnections: 0, avgQueryMs: 0, nodesCount: 0, relsCount: 0 },
  redis: { lagMs: 0, streamDepth: 0, memoryUsedMB: 0, pendingMessages: 0 },
  workers: { active: 0, total: 8, cpuPercent: 0, ramPercent: 0, processedPerSec: 0 },
  tps: 0,
  meanLatencyMs: 0,
  uptime: "0h 0m",
  graphAnalytics: { modularity: 0, clusters: 0, bfsLatencyMs: 0 },
  redisWindow: { windowSec: 60, eventsInWindow: 0 },
};

export function useMockStream() {
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [systemHealth, setSystemHealth] = useState<SystemHealth>(INITIAL_HEALTH);
  const [latencyBuckets, setLatencyBuckets] = useState<LatencyBucket[]>([]);
  const [isPaused, setIsPaused] = useState(false);
  const [totalProcessed, setTotalProcessed] = useState(0);
  const [totalBlocked, setTotalBlocked] = useState(0);
  const [blockedVolume, setBlockedVolume] = useState(0);
  const [globalRiskAvg, setGlobalRiskAvg] = useState(25);

  const isPausedRef = useRef(isPaused);
  isPausedRef.current = isPaused;

  // Initialize on mount only (client-side)
  const initialized = useRef(false);
  useEffect(() => {
    if (initialized.current) return;
    initialized.current = true;
    setSystemHealth(generateSystemHealth());
    setLatencyBuckets(generateLatencyHeatmap());
  }, []);

  // Transaction stream
  useEffect(() => {
    const interval = setInterval(() => {
      if (isPausedRef.current) return;

      const tx = generateTransaction();
      setTransactions((prev) => {
        const next = [tx, ...prev].slice(0, MAX_TRANSACTIONS);
        return next;
      });
      setTotalProcessed((p) => p + 1);

      if (tx.status === "BLOCKED") {
        setTotalBlocked((p) => p + 1);
        setBlockedVolume((p) => p + tx.amount);
      }

      // Rolling risk average
      setGlobalRiskAvg((prev) => {
        const alpha = 0.05;
        return prev * (1 - alpha) + tx.riskScore * alpha;
      });

      // Update latency heatmap
      setLatencyBuckets((prev) => {
        const next = [...prev.slice(1), { index: prev.length, latencyMs: tx.latencyMs, timestamp: new Date() }];
        return next;
      });
    }, STREAM_INTERVAL_MS);

    return () => clearInterval(interval);
  }, []);

  // System health refresh
  useEffect(() => {
    const interval = setInterval(() => {
      setSystemHealth(generateSystemHealth());
    }, 3000);
    return () => clearInterval(interval);
  }, []);

  const togglePause = useCallback(() => {
    setIsPaused((p) => !p);
  }, []);

  return {
    transactions,
    systemHealth,
    latencyBuckets,
    isPaused,
    togglePause,
    totalProcessed,
    totalBlocked,
    blockedVolume,
    globalRiskAvg,
  };
}
