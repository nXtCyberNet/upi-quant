"use client";

import type { SystemHealth } from "@/lib/mock-data";
import { Database, HardDrive, Cpu, Clock, GitBranch, Activity } from "lucide-react";

interface SystemHealthBarProps {
  health: SystemHealth;
}

export function SystemHealthBar({ health }: SystemHealthBarProps) {
  return (
    <div className="fixed bottom-0 left-0 right-0 h-10 bg-[#020617]/95 backdrop-blur-sm border-t border-slate-800 border-t-sky-500/10 z-50 flex items-center px-4 gap-4 text-xs font-mono">
      {/* Neo4j + Entropy */}
      <div className="flex items-center gap-2">
        <Database size={12} className="text-[#38bdf8]" />
        <span className="text-[#475569]">Neo4j</span>
        <span className="text-[#94a3b8]">
          <span className="text-[#10b981]">{health.neo4j.activeConnections}</span>/{health.neo4j.idleConnections + health.neo4j.activeConnections}
        </span>
        <span className="text-[#334155]">│</span>
        <span className="text-[#94a3b8]">
          MATCH <span className={health.neo4j.avgQueryMs < 50 ? "text-[#10b981]" : "text-[#f59e0b]"}>{health.neo4j.avgQueryMs}ms</span>
        </span>
        {health.graphAnalytics && (
          <>
            <span className="text-[#334155]">│</span>
            <span className="text-[#64748b]">Mod:</span>
            <span className={health.graphAnalytics.modularity > 0.5 ? "text-[#f59e0b]" : "text-[#10b981]"}>
              {health.graphAnalytics.modularity.toFixed(2)}
            </span>
            <span className="text-[#64748b]">Cls:</span>
            <span className="text-[#a78bfa]">{health.graphAnalytics.clusters}</span>
          </>
        )}
      </div>

      <div className="w-px h-4 bg-[#1e293b]" />

      {/* Redis + Window */}
      <div className="flex items-center gap-2">
        <HardDrive size={12} className="text-[#ef4444]" />
        <span className="text-[#475569]">Redis</span>
        <span className="text-[#94a3b8]">
          Lag <span className="text-[#10b981]">{health.redis.lagMs}ms</span>
        </span>
        <span className="text-[#334155]">│</span>
        <span className="text-[#94a3b8]">
          Depth <span className={health.redis.streamDepth > 10 ? "text-[#f59e0b]" : "text-[#10b981]"}>{health.redis.streamDepth}</span>
        </span>
        {health.redisWindow && (
          <>
            <span className="text-[#334155]">│</span>
            <span className="text-[#64748b]">Win:</span>
            <span className="text-[#22d3ee]">{health.redisWindow.windowSec}s</span>
            <span className="text-[#64748b]">Evt:</span>
            <span className="text-[#f59e0b]">{(health.redisWindow.eventsInWindow / 1000).toFixed(1)}k</span>
          </>
        )}
      </div>

      <div className="w-px h-4 bg-[#1e293b]" />

      {/* Workers */}
      <div className="flex items-center gap-2">
        <Cpu size={12} className="text-[#a78bfa]" />
        <span className="text-[#475569]">Workers</span>
        <span className="text-[#94a3b8]">
          <span className="text-[#10b981]">{health.workers.active}</span>/{health.workers.total}
        </span>
        <span className="text-[#334155]">│</span>
        <span className="text-[#94a3b8]">
          CPU <span className={health.workers.cpuPercent > 50 ? "text-[#f59e0b]" : "text-[#10b981]"}>{health.workers.cpuPercent.toFixed(0)}%</span>
          {" "}RAM <span className={health.workers.ramPercent > 50 ? "text-[#f59e0b]" : "text-[#10b981]"}>{health.workers.ramPercent.toFixed(0)}%</span>
        </span>
      </div>

      <div className="w-px h-4 bg-[#1e293b]" />

      {/* 3-Hop BFS Latency */}
      {health.graphAnalytics && (
        <div className="flex items-center gap-2">
          <GitBranch size={12} className="text-[#22d3ee]" />
          <span className="text-[#475569]">3-Hop BFS</span>
          <span className={health.graphAnalytics.bfsLatencyMs < 30 ? "text-[#10b981]" : health.graphAnalytics.bfsLatencyMs < 60 ? "text-[#f59e0b]" : "text-[#ef4444]"}>
            {health.graphAnalytics.bfsLatencyMs}ms
          </span>
        </div>
      )}

      <div className="flex-1" />

      {/* Status indicators */}
      <div className="flex items-center gap-3">
        <div className="flex items-center gap-1.5">
          <Activity size={10} className="text-[#10b981] animate-pulse" />
          <span className="text-[10px] text-[#64748b]">Operational</span>
        </div>
        <div className="flex items-center gap-1">
          <Clock size={10} className="text-[#64748b]" />
          <span className="text-[10px] text-[#64748b]">{health.uptime}</span>
        </div>
      </div>
    </div>
  );
}
