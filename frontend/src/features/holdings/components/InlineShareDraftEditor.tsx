"use client";

interface InlineShareDraftEditorProps {
  quantityText: string;
  disabled?: boolean;
  draftActive?: boolean;
  invalid?: boolean;
  step?: number;
  titleBase: string;
  onQuantityTextChange: (value: string) => void;
  onStep: (delta: number) => void;
}

function parseQuantityText(raw: string): number | null {
  const clean = String(raw || "").trim().replaceAll(",", "");
  if (!clean) return null;
  const quantity = Number(clean);
  return Number.isFinite(quantity) ? quantity : null;
}

export default function InlineShareDraftEditor({
  quantityText,
  disabled = false,
  draftActive = false,
  invalid = false,
  step = 5,
  titleBase,
  onQuantityTextChange,
  onStep,
}: InlineShareDraftEditorProps) {
  const currentQty = parseQuantityText(quantityText);
  const isShort = currentQty !== null && currentQty < 0;
  const increaseLabel = `${isShort ? "Increase short" : "Increase"} ${titleBase} by ${step} shares`;
  const decreaseLabel = `${isShort ? "Decrease short" : "Decrease"} ${titleBase} by ${step} shares`;

  function handleIncrease() {
    if (isShort) {
      onStep(-step);
      return;
    }
    onStep(step);
  }

  function handleDecrease() {
    if (currentQty === null || currentQty === 0) {
      onStep(-step);
      return;
    }
    if (isShort) {
      onStep(Math.min(step, Math.abs(currentQty)));
      return;
    }
    onStep(-Math.min(step, currentQty));
  }

  return (
    <span className={`share-draft-editor${draftActive ? " draft" : ""}${invalid ? " invalid" : ""}`}>
      <input
        className="share-draft-input"
        inputMode="decimal"
        value={quantityText}
        onChange={(e) => onQuantityTextChange(e.target.value)}
        aria-label={`${titleBase} quantity`}
        disabled={disabled}
      />
      <button
        className="share-adjuster-btn"
        onClick={handleIncrease}
        disabled={disabled}
        title={increaseLabel}
        aria-label={increaseLabel}
        type="button"
      >
        ↑
      </button>
      <button
        className="share-adjuster-btn"
        onClick={handleDecrease}
        disabled={disabled}
        title={decreaseLabel}
        aria-label={decreaseLabel}
        type="button"
      >
        ↓
      </button>
    </span>
  );
}
