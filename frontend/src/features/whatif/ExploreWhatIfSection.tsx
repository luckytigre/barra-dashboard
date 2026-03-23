"use client";

import WhatIfBuilderPanel from "@/features/whatif/WhatIfBuilderPanel";
import WhatIfPreviewPanel from "@/features/whatif/WhatIfPreviewPanel";
import { useWhatIfScenarioLab } from "@/features/whatif/useWhatIfScenarioLab";
import type { UniverseSearchItem, UniverseTickerItem } from "@/lib/types/cuse4";
import type { ExplorePositionSummary } from "@/features/whatif/whatIfUtils";

export default function ExploreWhatIfSection({
  item,
  priceMap,
  searchQuery,
  onSearchQueryChange,
  searchResults,
  onSelectTicker,
  positionMap,
}: {
  item: UniverseTickerItem | null | undefined;
  priceMap: Map<string, number>;
  searchQuery: string;
  onSearchQueryChange: (q: string) => void;
  searchResults: UniverseSearchItem[];
  onSelectTicker: (ticker: string) => void;
  positionMap: Map<string, ExplorePositionSummary>;
}) {
  const scenario = useWhatIfScenarioLab({
    item,
    priceMap,
    searchQuery,
    searchResults,
    onSelectTicker,
  });

  return (
    <div className="whatif-builder" ref={scenario.wrapRef}>
      <WhatIfBuilderPanel
        accountId={scenario.accountId}
        accountOptions={scenario.accountOptions}
        activeIndex={scenario.activeIndex}
        applyReady={scenario.applyReady}
        awaitingRefresh={scenario.awaitingRefresh}
        busy={scenario.busy}
        builderStatus={scenario.builderStatus}
        controlsBusy={scenario.controlsBusy}
        discardReady={scenario.discardReady}
        dropdownOpen={scenario.dropdownOpen}
        entryMv={scenario.entryMv}
        entryPrice={scenario.entryPrice}
        errorMessage={scenario.errorMessage}
        onAccountIdChange={scenario.setAccountId}
        onApply={() => void scenario.applyScenario()}
        onDiscard={scenario.discardScenario}
        onPreview={() => void scenario.runPreview()}
        onQuantityTextChange={scenario.setQuantityText}
        onSearchQueryChange={onSearchQueryChange}
        onSetActiveIndex={scenario.setActiveIndex}
        onStage={scenario.stageSelectedTicker}
        onTickerBlur={scenario.handleTickerBlur}
        onTickerFocus={scenario.handleTickerFocus}
        onTickerKeyDown={scenario.handleTickerKeyDown}
        onTickerSelect={scenario.selectFromTypeahead}
        positionMap={positionMap}
        previewNeedsAttention={scenario.previewNeedsAttention}
        previewReady={scenario.previewReady}
        priceMap={priceMap}
        quantityText={scenario.quantityText}
        resultMessage={scenario.resultMessage}
        scenarioRows={scenario.scenarioRows}
        searchQuery={searchQuery}
        searchResults={searchResults}
        stageReady={scenario.stageReady}
        updateScenarioRow={scenario.updateScenarioRow}
        adjustScenarioRow={scenario.adjustScenarioRow}
        removeScenarioRow={scenario.removeScenarioRow}
      />

      <WhatIfPreviewPanel
        currentModeFactorOrder={scenario.currentModeFactorOrder}
        mode={scenario.mode}
        onModeChange={scenario.setMode}
        onToggleResults={() => scenario.setShowResults((prev) => !prev)}
        previewData={scenario.previewData}
        showResults={scenario.showResults}
        toggleRef={scenario.toggleRef}
      />
    </div>
  );
}
