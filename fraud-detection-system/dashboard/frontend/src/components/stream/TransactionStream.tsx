"use client";

import { useState, useRef, useCallback, useMemo } from "react";
import { formatINR, getRiskColor, getRiskBadgeClass, getRiskLabel } from "@/lib/utils";
import type { Transaction } from "@/lib/mock-data";
import { motion, AnimatePresence } from "framer-motion";
import { Search, Filter, Pause, Play, ChevronDown } from "lucide-react";

interface TransactionStreamProps {
  transactions: Transaction[];
  onSelect: (tx: Transaction) => void;
  selectedId?: string;
  onContextMenu?: (e: React.MouseEvent, tx: Transaction) => void;
}

type RiskFilter = "all" | "critical" | "high" | "medium" | "low";

export function TransactionStream({ transactions, onSelect, selectedId, onContextMenu }: TransactionStreamProps) {
  const [isHovering, setIsHovering] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [riskFilter, setRiskFilter] = useState<RiskFilter>("all");
  const [showFilters, setShowFilters] = useState(false);
  const [isFrozen, setIsFrozen] = useState(false);

  // Freeze snapshot: when user hovers or explicitly freezes, we capture the
  // current list so new incoming txs don't shift items under the cursor
  const frozenTxRef = useRef<Transaction[]>([]);
  const displayFrozen = isHovering || isFrozen;

  // When entering hover state, capture the snapshot
  const handleMouseEnter = useCallback(() => {
    frozenTxRef.current = transactions;
    setIsHovering(true);
  }, [transactions]);

  const handleMouseLeave = useCallback(() => {
    setIsHovering(false);
  }, []);

  // Use frozen snapshot when hovering, live data otherwise
  const baseTxList = displayFrozen ? frozenTxRef.current : transactions;

  // Apply search + filter
  const filteredTxs = useMemo(() => {
    let list = baseTxList;

    // Risk filter
    if (riskFilter !== "all") {
      list = list.filter((tx) => {
        if (riskFilter === "critical") return tx.riskScore >= 80;
        if (riskFilter === "high") return tx.riskScore >= 60 && tx.riskScore < 80;
        if (riskFilter === "medium") return tx.riskScore >= 40 && tx.riskScore < 60;
        return tx.riskScore < 40;
      });
    }

    // Search filter
    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase();
      list = list.filter((tx) =>
        tx.senderUPI.toLowerCase().includes(q) ||
        tx.receiverUPI.toLowerCase().includes(q) ||
        tx.senderName.toLowerCase().includes(q) ||
        tx.receiverName.toLowerCase().includes(q) ||
        tx.city.toLowerCase().includes(q) ||
        tx.id.toLowerCase().includes(q) ||
        tx.amount.toString().includes(q)
      );
    }

    return list.slice(0, 50);
  }, [baseTxList, riskFilter, searchQuery]);

  // Pin selected tx to top if it exists and isn't already in filtered list top
  const selectedTx = selectedId ? baseTxList.find((tx) => tx.id === selectedId) : null;
  const pinnedAtTop = selectedTx && filteredTxs[0]?.id !== selectedId;

  const riskFilterOptions: { key: RiskFilter; label: string; color: string; count: number }[] = [
    { key: "all", label: "All", color: "#94a3b8", count: baseTxList.length },
    { key: "critical", label: "Critical", color: "#ef4444", count: baseTxList.filter((t) => t.riskScore >= 80).length },
    { key: "high", label: "High", color: "#f59e0b", count: baseTxList.filter((t) => t.riskScore >= 60 && t.riskScore < 80).length },
    { key: "medium", label: "Medium", color: "#38bdf8", count: baseTxList.filter((t) => t.riskScore >= 40 && t.riskScore < 60).length },
    { key: "low", label: "Low", color: "#10b981", count: baseTxList.filter((t) => t.riskScore < 40).length },
  ];

  return (
    <div className="flex flex-col gap-2 h-full">
      {/* ── Controls Bar ── */}
      <div className="flex flex-col gap-1.5 shrink-0">
        {/* Search + Freeze */}
        <div className="flex items-center gap-1.5">
          <div className="flex-1 relative">
            <Search size={12} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-[#64748b]" />
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Search UPI, name, city…"
              className="w-full pl-7 pr-3 py-1.5 rounded-lg text-[11px] bg-[#0f172a] border border-[#1e293b] text-[#f1f5f9] placeholder-[#475569] focus:border-sky-500/40 focus:outline-none focus:shadow-[0_0_8px_rgba(56,189,248,0.1)] transition-all font-mono"
            />
          </div>
          <button
            onClick={() => setIsFrozen(!isFrozen)}
            className={`flex items-center gap-1 px-2.5 py-1.5 rounded-lg text-[10px] font-semibold border transition-all ${
              isFrozen
                ? "bg-amber-500/15 text-amber-400 border-amber-500/30 shadow-[0_0_8px_rgba(245,158,11,0.1)]"
                : "bg-[#0f172a] text-[#64748b] border-[#1e293b] hover:text-[#94a3b8] hover:border-[#334155]"
            }`}
            title={isFrozen ? "Resume live updates" : "Freeze stream to browse"}
          >
            {isFrozen ? <Play size={10} /> : <Pause size={10} />}
            {isFrozen ? "LIVE" : "FREEZE"}
          </button>
          <button
            onClick={() => setShowFilters(!showFilters)}
            className={`flex items-center gap-1 px-2.5 py-1.5 rounded-lg text-[10px] font-semibold border transition-all ${
              riskFilter !== "all"
                ? "bg-sky-500/15 text-sky-400 border-sky-500/30"
                : "bg-[#0f172a] text-[#64748b] border-[#1e293b] hover:text-[#94a3b8] hover:border-[#334155]"
            }`}
          >
            <Filter size={10} />
            <ChevronDown size={10} className={`transition-transform ${showFilters ? "rotate-180" : ""}`} />
          </button>
        </div>

        {/* Risk filter pills */}
        {showFilters && (
          <div className="flex items-center gap-1 flex-wrap">
            {riskFilterOptions.map((opt) => (
              <button
                key={opt.key}
                onClick={() => setRiskFilter(opt.key)}
                className={`flex items-center gap-1 px-2 py-1 rounded-md text-[10px] font-semibold border transition-all ${
                  riskFilter === opt.key
                    ? "border-sky-500/40 bg-[#1e293b] text-[#f1f5f9]"
                    : "border-transparent bg-[#0f172a] text-[#64748b] hover:text-[#94a3b8]"
                }`}
              >
                <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: opt.color }} />
                {opt.label}
                <span className="text-[9px] text-[#475569] font-mono">({opt.count})</span>
              </button>
            ))}
          </div>
        )}

        {/* Status bar */}
        <div className="flex items-center justify-between text-[9px] font-mono px-1">
          <span className="text-[#475569]">
            {filteredTxs.length} shown · {baseTxList.length} buffered
          </span>
          {displayFrozen && (
            <span className="text-amber-400/80 flex items-center gap-1">
              <span className="w-1.5 h-1.5 rounded-full bg-amber-400 animate-pulse" />
              {isFrozen ? "FROZEN" : "HOVER-PAUSED"}
            </span>
          )}
        </div>
      </div>

      {/* ── Transaction List ── */}
      <div
        className="flex flex-col gap-1 overflow-y-auto flex-1 pr-1 min-h-0"
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
      >
        {/* Pinned selected tx */}
        {pinnedAtTop && selectedTx && (
          <div className="shrink-0 mb-1">
            <div className="text-[9px] text-sky-400/60 font-mono px-1 mb-0.5 flex items-center gap-1">
              <span className="w-1 h-1 rounded-full bg-sky-400" />
              PINNED SELECTION
            </div>
            <TxRow tx={selectedTx} isSelected onSelect={onSelect} onContextMenu={onContextMenu} />
          </div>
        )}

        <AnimatePresence initial={false}>
          {filteredTxs.map((tx) => (
            <motion.div
              key={tx.id}
              initial={displayFrozen ? false : { opacity: 0, x: -20, height: 0 }}
              animate={{ opacity: 1, x: 0, height: "auto" }}
              exit={{ opacity: 0, height: 0 }}
              transition={{ duration: displayFrozen ? 0 : 0.2 }}
            >
              <TxRow
                tx={tx}
                isSelected={selectedId === tx.id}
                onSelect={onSelect}
                onContextMenu={onContextMenu}
              />
            </motion.div>
          ))}
        </AnimatePresence>

        {filteredTxs.length === 0 && (
          <div className="flex flex-col items-center justify-center py-8 text-[#64748b] gap-2">
            <Search size={24} className="opacity-30" />
            <p className="text-xs">No transactions match your filters</p>
          </div>
        )}
      </div>
    </div>
  );
}

// ── Individual Transaction Row ───────────────────────────────
function TxRow({ tx, isSelected, onSelect, onContextMenu }: {
  tx: Transaction;
  isSelected: boolean;
  onSelect: (tx: Transaction) => void;
  onContextMenu?: (e: React.MouseEvent, tx: Transaction) => void;
}) {
  return (
    <div
      onClick={() => onSelect(tx)}
      onContextMenu={(e) => { if (onContextMenu) { e.preventDefault(); onContextMenu(e, tx); } }}
      className={`flex items-center gap-3 px-3 py-2 rounded-lg cursor-pointer transition-all duration-150 border
        ${isSelected
          ? "bg-[#1e293b] border-slate-700 border-t-sky-500/20"
          : "bg-[#0f172a]/60 border-transparent hover:bg-[#0f172a] hover:border-slate-800"
        }`}
      style={
        tx.riskScore >= 80
          ? { boxShadow: "0 0 15px rgba(239, 68, 68, 0.15), inset 0 1px 0 rgba(239, 68, 68, 0.08)" }
          : undefined
      }
    >
      {/* Risk indicator dot */}
      <div
        className={`w-2 h-2 rounded-full shrink-0 ${tx.riskScore >= 80 ? "animate-pulse" : ""}`}
        style={{
          backgroundColor: getRiskColor(tx.riskScore),
          boxShadow: tx.riskScore >= 80
            ? `0 0 12px ${getRiskColor(tx.riskScore)}80`
            : `0 0 8px ${getRiskColor(tx.riskScore)}60`,
        }}
      />

      {/* Transaction details */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 text-xs">
          <span className="font-mono text-[#94a3b8] truncate">{tx.senderUPI.split("@")[0].slice(0, 6)}…</span>
          <span className="text-[#64748b]">→</span>
          <span className="font-mono text-[#94a3b8] truncate">{tx.receiverUPI.split("@")[0].slice(0, 6)}…</span>
        </div>
        <div className="flex items-center gap-2 mt-0.5">
          <span className="text-xs text-[#64748b]">{tx.city}</span>
          <span className="text-xs text-[#334155]">•</span>
          <span className="text-xs font-mono text-[#64748b]">{tx.latencyMs}ms</span>
        </div>
      </div>

      {/* Amount */}
      <div className="text-right shrink-0">
        <div className="text-sm font-semibold font-mono" style={{ color: getRiskColor(tx.riskScore) }}>
          {formatINR(tx.amount)}
        </div>
        <div className="flex items-center justify-end gap-1 mt-0.5">
          <span className={`text-[10px] font-semibold px-1.5 py-0.5 rounded ${getRiskBadgeClass(tx.riskScore)}`}>
            {getRiskLabel(tx.riskScore)} {tx.riskScore}
          </span>
          {tx.status === "BLOCKED" && (
            <span className="text-[10px] font-semibold px-1.5 py-0.5 rounded bg-red-500/20 text-red-400 border border-red-500/30">
              BLOCKED
            </span>
          )}
        </div>
      </div>
    </div>
  );
}
