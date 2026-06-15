# scry website

The marketing site for [scry](https://github.com/serialexp/scry) — a static
SolidJS single-page app. **No backend.** `bun run build` emits a self-contained
bundle into `dist/`, served in production by an nginx container.

Theme: dark-minimal with tasteful motion (scroll-reveal, drifting aurora
backdrop, gradient accents). Respects `prefers-reduced-motion`.

## Develop

```bash
bun install
bun run dev          # vite dev server with HMR
bun run typecheck    # tsc --noEmit
bun run build        # → dist/
bun run preview      # serve the built dist/ locally
```

## Docker (nginx)

```bash
docker build -t scry-website .
docker run --rm -p 8080:80 scry-website
# open http://localhost:8080/   ·   health: http://localhost:8080/healthz
```

The image is a two-stage build: a `bun` builder produces `dist/`, then
`nginx:alpine` serves it with gzip, long-lived caching for hashed assets, a
no-cache `index.html`, an SPA fallback, and a `/healthz` endpoint.

## Layout

```
index.html              # document shell + meta/OG tags + fonts
src/
  index.tsx             # entry — renders <App>
  App.tsx               # section composition
  content.ts            # all marketing copy + structured data
  styles/global.css     # design tokens, base, shared utilities
  lib/reveal.ts         # use:reveal scroll-in directive (IntersectionObserver)
  components/            # Nav, Hero, Problem, Features, Architecture,
                         #   HowItWorks, Protocols, CTA, Footer, Background, Logo
public/                 # favicon.svg, og.svg
Dockerfile · nginx.conf # static nginx image
```

Editing copy? It almost all lives in `src/content.ts`.
