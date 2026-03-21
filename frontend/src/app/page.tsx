import LandingFamilyPicker from "@/components/LandingFamilyPicker";
import LandingScrollHint from "@/components/LandingScrollHint";
import LandingSummary from "@/components/LandingSummary";

export default function Home() {
  return (
    <>
      <div className="landing-hero">
        <LandingFamilyPicker />
        <LandingScrollHint />
      </div>
      <LandingSummary />
    </>
  );
}
