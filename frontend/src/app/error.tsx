"use client";

import { useEffect } from "react";
import ApiErrorState from "@/components/ApiErrorState";

export default function AppError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error(error);
  }, [error]);

  return <ApiErrorState surface="this page" error={error} onRetry={reset} />;
}
