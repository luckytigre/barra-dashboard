"use client";

import { useAuthSession } from "@/components/AuthSessionContext";
import { accountTypeFromSession, describeUiError, type UiAccountType } from "@/lib/uiErrors";

export default function InlineApiError({
  error,
  surface,
  impact,
  accountType,
  accountName,
  className = "explore-error",
}: {
  error: unknown;
  surface: string;
  impact?: string;
  accountType?: UiAccountType | null;
  accountName?: string | null;
  className?: string;
}) {
  const { authenticated, session, context } = useAuthSession();
  const description = describeUiError(error, {
    surface,
    accountType: accountType ?? accountTypeFromSession(session, context),
    accountName,
    authenticated,
    authProvider: session?.authProvider,
    isAdmin: Boolean(context?.is_admin || session?.isAdmin),
  });

  return (
    <div className={className} role="alert">
      <strong>{description.title}.</strong>
      <span>{description.message}{impact ? ` ${impact}` : ""}</span>
      {description.diagnostic ? (
        <details className="ui-error-diagnostic">
          <summary>Admin details</summary>
          <code>{description.diagnostic}</code>
        </details>
      ) : null}
    </div>
  );
}
