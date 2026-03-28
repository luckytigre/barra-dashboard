// Shared holdings/account frontend API helpers.
// Family-specific barrels may re-export these only where shared holdings plumbing is intentional.

export const holdingsApiPath = {
  holdingsModes: () => "/api/holdings/modes",
  holdingsAccounts: () => "/api/holdings/accounts",
  holdingsPositions: (accountId?: string | null) =>
    accountId === undefined
      ? null
      : accountId && accountId.trim().length > 0
      ? `/api/holdings/positions?account_id=${encodeURIComponent(accountId.trim())}`
      : "/api/holdings/positions",
  holdingsImport: () => "/api/holdings/import",
  holdingsPosition: () => "/api/holdings/position",
  holdingsPositionRemove: () => "/api/holdings/position/remove",
  portfolioWhatIfApply: () => "/api/portfolio/whatif/apply",
} as const;

export const apiPath = holdingsApiPath;
