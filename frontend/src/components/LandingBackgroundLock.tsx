"use client";

import { useEffect, useRef } from "react";
import { useBackground, type BgMode } from "./BackgroundContext";

export default function LandingBackgroundLock() {
  const { mode, setMode } = useBackground();
  const initialModeRef = useRef<BgMode | null>(null);

  if (initialModeRef.current === null) {
    initialModeRef.current = mode;
  }

  useEffect(() => {
    const previousMode = initialModeRef.current ?? "topo";
    if (previousMode !== "topo") {
      setMode("topo");
    }

    return () => {
      if (previousMode !== "topo") {
        setMode(previousMode);
      }
    };
  }, [setMode]);

  return null;
}
