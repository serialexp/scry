import { Background } from "./components/Background";
import { Nav } from "./components/Nav";
import { Hero } from "./components/Hero";
import { Problem } from "./components/Problem";
import { Features } from "./components/Features";
import { GetStarted } from "./components/GetStarted";
import { Protocols } from "./components/Protocols";
import { CTA } from "./components/CTA";
import { Footer } from "./components/Footer";

export function App() {
  return (
    <>
      <Background />
      <Nav />
      <main>
        <Hero />
        <Problem />
        <Features />
        <GetStarted />
        <Protocols />
        <CTA />
      </main>
      <Footer />
    </>
  );
}
