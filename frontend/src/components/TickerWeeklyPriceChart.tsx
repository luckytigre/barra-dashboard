"use client";

import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  type ChartData,
  type ChartOptions,
  type ScriptableContext,
} from "chart.js";
import { Line } from "react-chartjs-2";
import type { WeeklyPricePoint } from "@/lib/types/cuse4";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Filler, Tooltip);

interface TickerWeeklyPriceChartProps {
  ticker: string;
  points: WeeklyPricePoint[];
  variant?: "full" | "sparkline";
  className?: string;
}

const USD_FORMATTER = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 2,
});

function formatCurrency(value: number): string {
  return USD_FORMATTER.format(value);
}

export default function TickerWeeklyPriceChart({
  ticker,
  points,
  variant = "full",
  className = "",
}: TickerWeeklyPriceChartProps) {
  const isSparkline = variant === "sparkline";

  if (!points || points.length === 0) {
    return (
      <div className={`detail-history-empty${isSparkline ? " compact" : ""}`}>
        No 5Y weekly close history available for {ticker}.
      </div>
    );
  }

  const labels = points.map((p) => p.date);
  const values = points.map((p) => Number(p.close) || 0);
  const first = values[0] ?? 0;
  const latest = values[values.length - 1] ?? 0;
  const isPositive = latest >= first;
  const lineColor = isPositive ? "#6bcf9a" : "#e0577f";

  const data: ChartData<"line", number[], string> = {
    labels,
    datasets: [
      {
        label: "Weekly Close",
        data: values,
        borderColor: lineColor,
        borderWidth: isSparkline ? 1.6 : 1.8,
        pointRadius: 0,
        pointHoverRadius: isSparkline ? 2 : 3,
        pointHoverBackgroundColor: lineColor,
        pointHoverBorderColor: "#fff",
        pointHoverBorderWidth: 1.5,
        tension: isSparkline ? 0.28 : 0.22,
        fill: true,
        backgroundColor: (ctx: ScriptableContext<"line">) => {
          const { chart } = ctx;
          const { ctx: canvasCtx, chartArea } = chart;
          if (!chartArea) return "transparent";
          const gradient = canvasCtx.createLinearGradient(0, chartArea.top, 0, chartArea.bottom);
          if (isPositive) {
            gradient.addColorStop(0, "rgba(79, 160, 116, 0.45)");
            gradient.addColorStop(0.4, "rgba(79, 160, 116, 0.12)");
            gradient.addColorStop(1, "rgba(79, 160, 116, 0.02)");
          } else {
            gradient.addColorStop(0, "rgba(196, 63, 116, 0.02)");
            gradient.addColorStop(0.6, "rgba(196, 63, 116, 0.12)");
            gradient.addColorStop(1, "rgba(196, 63, 116, 0.45)");
          }
          return gradient;
        },
      },
    ],
  };

  const options: ChartOptions<"line"> = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: {
      mode: "index",
      intersect: false,
    },
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: "rgba(20, 22, 30, 0.92)",
        borderColor: "rgba(154, 171, 214, 0.18)",
        borderWidth: 1,
        cornerRadius: 4,
        padding: { top: 6, bottom: 6, left: isSparkline ? 8 : 10, right: isSparkline ? 8 : 10 },
        titleColor: "rgba(232, 237, 249, 0.6)",
        bodyColor: "#e8edf9",
        titleFont: { size: isSparkline ? 9 : 10, weight: "normal" as const },
        bodyFont: { size: isSparkline ? 10 : 11, weight: 500 },
        displayColors: false,
        callbacks: {
          title: (items) => String(items[0]?.label ?? ""),
          label: (ctx) => formatCurrency(Number(ctx.parsed.y ?? 0)),
        },
      },
    },
    scales: {
      x: {
        display: !isSparkline,
        border: { display: false },
        grid: { display: false },
        ticks: {
          color: "rgba(169, 182, 210, 0.5)",
          autoSkip: true,
          maxTicksLimit: 6,
          callback: (_value, idx) => {
            const raw = labels[idx] || "";
            return raw.length >= 7 ? raw.slice(0, 7) : raw;
          },
          font: { size: 9 },
        },
      },
      y: {
        display: !isSparkline,
        border: { display: false },
        grid: { color: "rgba(154, 171, 214, 0.10)" },
        ticks: {
          color: "rgba(169, 182, 210, 0.5)",
          callback: (v) => formatCurrency(Number(v)),
          font: { size: 9 },
        },
      },
    },
  };

  return (
    <div
      className={`detail-history-chart ticker-weekly-history-chart${isSparkline ? " sparkline" : ""}${className ? ` ${className}` : ""}`}
    >
      <Line data={data} options={options} />
    </div>
  );
}
