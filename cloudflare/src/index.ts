import { Container, getContainer } from "@cloudflare/containers";
import { checkOverdueAndNotify, checkBorrowDueAndNotify } from "./notifications";
import { checkWishlistPricesAndNotify } from "./wishlist";
import { sendDailyDigest } from "./digest";

export interface Env {
  LIANES_API: DurableObjectNamespace<LianesApi>;
  DB: D1Database;
  PINECONE_INDEX_NAME: string;
  PINECONE_API_KEY: string;
  HF_API_TOKEN: string;
  INTERNAL_D1_TOKEN: string;
  JWT_SECRET: string;
  // Optional — every job that reads these degrades gracefully when unset
  // (mock-logs the email, skips price lookups) rather than failing.
  RESEND_API_KEY?: string;
  RESEND_FROM_EMAIL?: string;
  GOOGLE_BOOKS_API_KEY?: string;
}

// Stateless FastAPI backend — every request is independent, state lives in
// D1 and Pinecone, not in the container. Single instance (getContainer +
// fixed name) instead of getRandom() load-balancing: the Containers beta was
// intermittently failing to start some instance slots, and this app's
// traffic doesn't need horizontal scaling anyway.
export class LianesApi extends Container {
  defaultPort = 8080;
  requiredPorts = [8080];
  sleepAfter = "10m";
  enableInternet = true; // needed to reach the D1 proxy, Pinecone, HuggingFace
}

const MAX_ATTEMPTS = 3;

interface D1Statement {
  sql: string;
  params?: unknown[];
}

// D1 bindings only exist inside the Worker, not inside the container, so the
// FastAPI app reaches D1 by calling back into this same Worker over HTTPS.
// Guarded by a shared secret instead of a Cloudflare API token, since it only
// needs to expose this app's own database, not full account-level D1 access.
//
// Body is either a single statement or an array — arrays run atomically via
// env.DB.batch() so a create-loan (insert transaction + update book) can't
// leave the two tables out of sync if one write fails.
async function handleD1Proxy(request: Request, env: Env): Promise<Response> {
  if (request.headers.get("X-Internal-Token") !== env.INTERNAL_D1_TOKEN) {
    return new Response("Forbidden", { status: 403 });
  }

  const body = await request.json<D1Statement | D1Statement[]>();

  try {
    if (Array.isArray(body)) {
      const results = await env.DB.batch(
        body.map((stmt) => env.DB.prepare(stmt.sql).bind(...(stmt.params ?? [])))
      );
      return Response.json({
        success: true,
        results: results.map((r) => r.results),
        meta: results.map((r) => r.meta),
      });
    }

    const result = await env.DB.prepare(body.sql).bind(...(body.params ?? [])).all();
    return Response.json({ success: true, results: result.results, meta: result.meta });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return Response.json({ success: false, error: message }, { status: 500 });
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/__d1/query" && request.method === "POST") {
      return handleD1Proxy(request, env);
    }

    // Manual trigger for the same job the daily cron runs — lets the overdue
    // detection + email templates be tested without waiting for the schedule.
    if (url.pathname === "/__internal/check-overdue" && request.method === "POST") {
      if (request.headers.get("X-Internal-Token") !== env.INTERNAL_D1_TOKEN) {
        return new Response("Forbidden", { status: 403 });
      }
      const result = await checkOverdueAndNotify(env);
      return Response.json(result);
    }

    // Manual trigger for the "borrowed by me" due-date reminder job.
    if (url.pathname === "/__internal/check-borrow-due" && request.method === "POST") {
      if (request.headers.get("X-Internal-Token") !== env.INTERNAL_D1_TOKEN) {
        return new Response("Forbidden", { status: 403 });
      }
      const result = await checkBorrowDueAndNotify(env);
      return Response.json(result);
    }

    // Manual trigger for the wishlist price-monitoring job.
    if (url.pathname === "/__internal/check-prices" && request.method === "POST") {
      if (request.headers.get("X-Internal-Token") !== env.INTERNAL_D1_TOKEN) {
        return new Response("Forbidden", { status: 403 });
      }
      const result = await checkWishlistPricesAndNotify(env);
      return Response.json(result);
    }

    // Manual trigger for the consolidated daily email digest.
    if (url.pathname === "/__internal/send-digest" && request.method === "POST") {
      if (request.headers.get("X-Internal-Token") !== env.INTERNAL_D1_TOKEN) {
        return new Response("Forbidden", { status: 403 });
      }
      const result = await sendDailyDigest(env);
      return Response.json(result);
    }

    let lastError: unknown;

    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        const container = getContainer(env.LIANES_API, "singleton");

        await container.startAndWaitForPorts({
          startOptions: {
            envVars: {
              D1_PROXY_URL: `${url.origin}/__d1/query`,
              INTERNAL_D1_TOKEN: env.INTERNAL_D1_TOKEN,
              JWT_SECRET: env.JWT_SECRET,
              PINECONE_API_KEY: env.PINECONE_API_KEY,
              PINECONE_INDEX_NAME: env.PINECONE_INDEX_NAME,
              HF_API_TOKEN: env.HF_API_TOKEN,
            },
          },
        });

        // Container beta occasionally reports ports ready just before the
        // instance actually stops — retry on that specific race instead of
        // surfacing a 500 to the browser.
        return await container.fetch(request);
      } catch (err) {
        lastError = err;
        const message = err instanceof Error ? err.message : String(err);
        if (!message.includes("container is not running") && !message.includes("internal error connecting")) {
          throw err;
        }
      }
    }

    throw lastError;
  },

  // Three time slots so a job never reads state the previous one hasn't
  // written yet: detect (8am) -> price-check (9am, needs its own slot since
  // it makes an external API call per wishlist item) -> digest (10am,
  // reads the notifications rows the first two just created).
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    switch (event.cron) {
      case "0 8 * * *":
        ctx.waitUntil(checkOverdueAndNotify(env));
        ctx.waitUntil(checkBorrowDueAndNotify(env));
        break;
      case "0 9 * * *":
        ctx.waitUntil(checkWishlistPricesAndNotify(env));
        break;
      case "0 10 * * *":
        ctx.waitUntil(sendDailyDigest(env));
        break;
    }
  },
};
