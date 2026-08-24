"use client";

interface HoldingsMutationFeedbackProps {
  resultMessage: string;
  errorMessage: string;
  rejectionPreview: Array<{ rowNumber: number | null; message: string }>;
  draftCount?: number;
  draftDeleteCount?: number;
}

export default function HoldingsMutationFeedback({
  resultMessage,
  errorMessage,
  rejectionPreview,
  draftCount = 0,
  draftDeleteCount = 0,
}: HoldingsMutationFeedbackProps) {
  return (
    <>
      {draftCount > 0 && (
        <div className="feedback-warn">
          {draftCount} staged edit{draftCount === 1 ? "" : "s"} pending
          {draftDeleteCount > 0 ? ` (${draftDeleteCount} remove${draftDeleteCount === 1 ? "" : "s"})` : ""}.
          Changes stay local until you hit `RECALC`.
        </div>
      )}
      {resultMessage && (
        <div className="feedback-success">{resultMessage}</div>
      )}
      {errorMessage && (
        <div className="feedback-error">{errorMessage}</div>
      )}
      {rejectionPreview.length > 0 && (
        <div className="feedback-rejection">
          Preview rejections:
          <ul>
            {rejectionPreview.map((row, index) => (
              <li key={`${row.rowNumber ?? "unknown"}-${index}`}>
                {row.rowNumber == null ? "Row" : `Row ${row.rowNumber}`}: {row.message}
              </li>
            ))}
          </ul>
        </div>
      )}
    </>
  );
}
