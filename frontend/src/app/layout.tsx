import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import { Header } from "@/components/layout/Header";
import { Footer } from "@/components/layout/Footer";
import { Providers } from "@/components/Providers";

const geistSans = Geist({
  variable: "--font-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: {
    default: "WorldOfTaxonomy - Global Classification Knowledge Graph",
    template: "%s | WorldOfTaxonomy",
  },
  description:
    "Explore 1,000+ global classification systems with 1.2M+ codes. Search NAICS, ISIC, HS, ICD, SOC codes and discover cross-system mappings.",
  keywords: [
    "NAICS codes",
    "ISIC codes",
    "HS codes",
    "industry classification",
    "taxonomy",
    "crosswalk",
    "ICD-10",
    "SOC codes",
    "NACE codes",
    "classification system",
  ],
  openGraph: {
    type: "website",
    locale: "en_US",
    url: "https://worldoftaxonomy.com",
    siteName: "WorldOfTaxonomy",
    title: "WorldOfTaxonomy - Global Classification Knowledge Graph",
    description:
      "1,000+ systems, 1.2M+ codes, 321K+ crosswalks. Search, browse, and translate classification codes across NAICS, ISIC, HS, ICD, and more.",
    images: [{ url: "/og-default.png", width: 1200, height: 630 }],
  },
  twitter: {
    card: "summary_large_image",
    title: "WorldOfTaxonomy",
    description: "Unified global classification knowledge graph",
    images: ["/og-default.png"],
  },
  robots: {
    index: true,
    follow: true,
    "max-snippet": -1,
    "max-image-preview": "large",
  },
  alternates: {
    canonical: "https://worldoftaxonomy.com",
  },
};

const jsonLd = {
  "@context": "https://schema.org",
  "@type": "DataCatalog",
  name: "WorldOfTaxonomy",
  description:
    "Unified global classification knowledge graph with 1,000+ systems, 1.2M+ codes, and 321K+ crosswalk edges.",
  url: "https://worldoftaxonomy.com",
  creator: {
    "@type": "Organization",
    name: "Colaberry",
    url: "https://colaberry.com",
  },
  license: "https://opensource.org/licenses/MIT",
  numberOfItems: 1000,
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="en"
      className={`${geistSans.variable} ${geistMono.variable} h-full antialiased`}
      suppressHydrationWarning
    >
      <head>
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
        />
      </head>
      <body className="min-h-full flex flex-col">
        <Providers>
          <Header />
          <main className="flex-1">{children}</main>
          <Footer />
        </Providers>
      </body>
    </html>
  );
}
