"use client";
/**
 * Top-level client providers (TanStack Query). Wrap the app body in
 * `app/layout.tsx` so any client component can call `useQuery`.
 *
 * Why a singleton client? In Next.js dev the root component remounts on
 * fast refresh; recreating QueryClient would drop in-flight requests and
 * cache. The `useState(() => …)` pattern gives us one client per browser
 * tab.
 */
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ReactQueryDevtools } from "@tanstack/react-query-devtools";
import { useState, type ReactNode } from "react";

export function Providers({ children }: { children: ReactNode }) {
  const [client] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            staleTime: 30_000,
            refetchOnWindowFocus: false,
            retry: 1,
          },
        },
      }),
  );
  return (
    <QueryClientProvider client={client}>
      {children}
      {process.env.NODE_ENV === "development" ? <ReactQueryDevtools initialIsOpen={false} /> : null}
    </QueryClientProvider>
  );
}
