"use client";

interface HoldingsMutationFeedbackProps {
  resultMessage: string;
  errorMessage: string;
  rejectionPreview: Array<Record<string, unknown>>;
}

export default function HoldingsMutationFeedback({
  resultMessage,
  errorMessage,
  rejectionPreview,
}: HoldingsMutationFeedbackProps) {
  return (
    <>
      {resultMessage && (
        <div style={{ marginTop: 10, color: "rgba(107, 207, 154, 0.88)", fontSize: 12 }}>
          {resultMessage}
        </div>
      )}
      {errorMessage && (
        <div style={{ marginTop: 10, color: "rgba(224, 87, 127, 0.92)", fontSize: 12 }}>
          {errorMessage}
        </div>
      )}
      {rejectionPreview.length > 0 && (
        <div style={{ marginTop: 10, fontSize: 11, color: "rgba(232, 237, 249, 0.75)" }}>
          Preview rejections:
          <pre style={{ marginTop: 6, whiteSpace: "pre-wrap" }}>
            {JSON.stringify(rejectionPreview, null, 2)}
          </pre>
        </div>
      )}
    </>
  );
}
