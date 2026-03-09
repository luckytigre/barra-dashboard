"use client";

import { useEffect, useRef, useState, type ReactNode } from "react";

interface LazyMountOnVisibleProps {
  children: ReactNode;
  fallback?: ReactNode;
  rootMargin?: string;
  minHeight?: number | string;
  once?: boolean;
}

export default function LazyMountOnVisible({
  children,
  fallback,
  rootMargin = "240px 0px",
  minHeight = 220,
  once = true,
}: LazyMountOnVisibleProps) {
  const ref = useRef<HTMLDivElement | null>(null);
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    if (visible && once) return;
    const node = ref.current;
    if (!node) return;
    const observer = new IntersectionObserver(
      (entries) => {
        const entry = entries[0];
        if (!entry?.isIntersecting) return;
        setVisible(true);
        if (once) observer.disconnect();
      },
      { rootMargin },
    );
    observer.observe(node);
    return () => observer.disconnect();
  }, [once, rootMargin, visible]);

  return (
    <div ref={ref} style={{ minHeight }}>
      {visible ? children : fallback ?? <div className="detail-history-empty loading-pulse">Loading section…</div>}
    </div>
  );
}
