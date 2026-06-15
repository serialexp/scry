# syntax=docker/dockerfile:1
#
# The scry marketing site: a static SolidJS SPA built with Vite, served by
# nginx. No backend, no runtime config — `bun run build` emits a self-contained
# bundle into dist/, which the runtime image serves directly.
#
# Two stages: a bun builder produces dist/, then a minimal nginx:alpine serves
# it. The runtime carries no toolchain — just nginx + the static assets.

# ── builder ────────────────────────────────────────────────────────────
FROM oven/bun:1.3-alpine AS builder
WORKDIR /src

# Install deps first so the layer caches across source-only changes.
COPY package.json ./
RUN bun install

# Then the sources + build.
COPY tsconfig.json vite.config.ts index.html ./
COPY public ./public
COPY src ./src
RUN bun run build

# ── runtime ────────────────────────────────────────────────────────────
# nginx:alpine: tiny, no app runtime. The :alpine tag tracks current stable.
FROM nginx:1.27-alpine AS runtime

# Our SPA-aware server config (gzip + long-lived asset caching).
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /src/dist /usr/share/nginx/html

EXPOSE 80

# nginx:alpine ships a sensible default CMD (`nginx -g 'daemon off;'`); keep it.
