# Edited 2026-10-03 by Claude (AI assistant): members-only skating dashboard (Next.js standalone).
FROM node:22-slim AS build
WORKDIR /app
# SKATING_API_URL must be present at BUILD time too: next.config bakes the /api/* rewrite destination.
ENV NEXT_TELEMETRY_DISABLED=1 NODE_OPTIONS=--max-old-space-size=640 SKATING_API_URL=http://skating-api:8000
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY . .
RUN mkdir -p public && npm run build

FROM node:22-slim AS run
WORKDIR /app
ENV NODE_ENV=production NEXT_TELEMETRY_DISABLED=1 PORT=3000 HOSTNAME=0.0.0.0
COPY --from=build /app/.next/standalone ./
COPY --from=build /app/.next/static ./.next/static
COPY --from=build /app/public ./public
EXPOSE 3000
CMD ["node", "server.js"]
