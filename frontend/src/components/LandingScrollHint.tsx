"use client";

import { useEffect, useRef } from "react";

export default function LandingScrollHint() {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    const onScroll = () => {
      const y = window.scrollY;
      const fade = Math.max(0, 1 - y / 120);
      el.style.opacity = String(fade);
    };

    window.addEventListener("scroll", onScroll, { passive: true });
    onScroll();
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

  return (
    <div ref={ref} className="landing-scroll-hint" aria-hidden="true">
      <span className="landing-scroll-hint-text">see more</span>
      <svg width="14" height="8" viewBox="0 0 14 8" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M1 1l6 6 6-6" />
      </svg>
    </div>
  );
}
