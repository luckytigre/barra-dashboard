"use client";

import { useMemo, useState } from "react";

export type SortDirection = "asc" | "desc";

export type SortComparator<T> = (left: T, right: T) => number;

export function compareText(left: string | null | undefined, right: string | null | undefined): number {
  return String(left || "").localeCompare(String(right || ""), undefined, {
    numeric: true,
    sensitivity: "base",
  });
}

export function compareNumber(left: number | null | undefined, right: number | null | undefined): number {
  const leftValue = typeof left === "number" && Number.isFinite(left) ? left : Number.NEGATIVE_INFINITY;
  const rightValue = typeof right === "number" && Number.isFinite(right) ? right : Number.NEGATIVE_INFINITY;
  return leftValue - rightValue;
}

export function compareDateText(left: string | null | undefined, right: string | null | undefined): number {
  const leftTime = Date.parse(String(left || ""));
  const rightTime = Date.parse(String(right || ""));
  const leftValue = Number.isFinite(leftTime) ? leftTime : Number.NEGATIVE_INFINITY;
  const rightValue = Number.isFinite(rightTime) ? rightTime : Number.NEGATIVE_INFINITY;
  return leftValue - rightValue;
}

export function useSortableRows<T, K extends string>({
  rows,
  comparators,
  initialKey = null,
  initialDirection = "desc",
}: {
  rows: T[];
  comparators: Record<K, SortComparator<T>>;
  initialKey?: K | null;
  initialDirection?: SortDirection;
}) {
  const [sortKey, setSortKey] = useState<K | null>(initialKey);
  const [sortAsc, setSortAsc] = useState(initialDirection === "asc");

  const sortedRows = useMemo(() => {
    if (!sortKey) return rows;
    const comparator = comparators[sortKey];
    const nextRows = [...rows];
    nextRows.sort((left, right) => {
      const value = comparator(left, right);
      return sortAsc ? value : -value;
    });
    return nextRows;
  }, [comparators, rows, sortAsc, sortKey]);

  const handleSort = (nextKey: K) => {
    if (sortKey === nextKey) {
      setSortAsc((current) => !current);
      return;
    }
    setSortKey(nextKey);
    setSortAsc(false);
  };

  const arrow = (key: K) => (sortKey === key ? (sortAsc ? " ↑" : " ↓") : "");

  return {
    arrow,
    handleSort,
    sortAsc,
    sortKey,
    sortedRows,
  };
}
