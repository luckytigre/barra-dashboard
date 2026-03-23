import type { Metadata } from "next";
import "./globals.css";
import { AppSettingsProvider } from "@/components/AppSettingsContext";
import { BackgroundProvider } from "@/components/BackgroundContext";
import Neo2DotBackground from "@/components/Neo2DotBackground";
import TabNav from "@/components/TabNav";
import PageTransition from "@/components/PageTransition";

export const metadata: Metadata = {
  title: "Ceiora",
  description: "Portfolio factor risk model dashboard",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <AppSettingsProvider>
          <BackgroundProvider>
            <Neo2DotBackground />
            <TabNav />
            <main className="dash-main">
              <PageTransition>{children}</PageTransition>
            </main>
          </BackgroundProvider>
        </AppSettingsProvider>
      </body>
    </html>
  );
}
