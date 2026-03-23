"use client";

import { createContext, useCallback, useContext, useState, type ReactNode } from "react";

export type CparFactorHistoryMode = "residual" | "market_adjusted";

interface AppSettingsContextValue {
  cparFactorHistoryMode: CparFactorHistoryMode;
  setCparFactorHistoryMode: (mode: CparFactorHistoryMode) => void;
}

const DEFAULT_CPAR_FACTOR_HISTORY_MODE: CparFactorHistoryMode = "market_adjusted";
const CPAR_FACTOR_HISTORY_MODE_KEY = "cpar-factor-history-mode";

const AppSettingsContext = createContext<AppSettingsContextValue>({
  cparFactorHistoryMode: DEFAULT_CPAR_FACTOR_HISTORY_MODE,
  setCparFactorHistoryMode: () => {},
});

export function AppSettingsProvider({ children }: { children: ReactNode }) {
  const [cparFactorHistoryMode, setCparFactorHistoryModeRaw] = useState<CparFactorHistoryMode>(() => {
    if (typeof window === "undefined") return DEFAULT_CPAR_FACTOR_HISTORY_MODE;
    const stored = String(localStorage.getItem(CPAR_FACTOR_HISTORY_MODE_KEY) || "").trim().toLowerCase();
    return stored === "residual" ? "residual" : DEFAULT_CPAR_FACTOR_HISTORY_MODE;
  });

  const setCparFactorHistoryMode = useCallback((mode: CparFactorHistoryMode) => {
    setCparFactorHistoryModeRaw(mode);
    if (typeof window !== "undefined") {
      localStorage.setItem(CPAR_FACTOR_HISTORY_MODE_KEY, mode);
    }
  }, []);

  return (
    <AppSettingsContext.Provider
      value={{
        cparFactorHistoryMode,
        setCparFactorHistoryMode,
      }}
    >
      {children}
    </AppSettingsContext.Provider>
  );
}

export function useAppSettings() {
  return useContext(AppSettingsContext);
}
