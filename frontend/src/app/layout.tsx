import type { Metadata } from "next";
import "./globals.css";
import { AppSettingsProvider } from "@/components/AppSettingsContext";
import { BackgroundProvider } from "@/components/BackgroundContext";
import Neo2DotBackground from "@/components/Neo2DotBackground";
import TabNav from "@/components/TabNav";
import PageTransition from "@/components/PageTransition";

const THEME_BOOTSTRAP = `
(() => {
  try {
    const stored = String(localStorage.getItem('theme-mode') || '').trim().toLowerCase();
    const mode = stored === 'light' ? 'light' : 'dark';
    document.documentElement.dataset.theme = mode;
    document.body.dataset.theme = mode;
    document.documentElement.style.colorScheme = mode;
    document.body.style.colorScheme = mode;
  } catch {}
})();
`;

export const metadata: Metadata = {
  title: "Ceiora",
  description: "Portfolio factor risk model dashboard",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" data-theme="dark" suppressHydrationWarning>
      <body data-theme="dark">
        <script dangerouslySetInnerHTML={{ __html: THEME_BOOTSTRAP }} />
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
