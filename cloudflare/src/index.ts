import { Container, getContainer } from "@cloudflare/containers";

export interface Env {
  LIANES_API: DurableObjectNamespace<LianesApi>;
  PINECONE_INDEX_NAME: string;
  DB_HOST: string;
  DB_PORT: string;
  DB_USER: string;
  DB_PASS: string;
  DB_NAME: string;
  PINECONE_API_KEY: string;
  HF_API_TOKEN: string;
}

// Stateless FastAPI backend — every request is independent, state lives in
// MySQL (Aiven) and Pinecone, not in the container. Single instance
// (getContainer + fixed name) instead of getRandom() load-balancing: the
// Containers beta was intermittently failing to start some instance slots,
// and this app's traffic doesn't need horizontal scaling anyway.
export class LianesApi extends Container {
  defaultPort = 8080;
  requiredPorts = [8080];
  sleepAfter = "10m";
  enableInternet = true; // needed to reach Aiven MySQL, Pinecone, HuggingFace
}

const MAX_ATTEMPTS = 3;

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    let lastError: unknown;

    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        const container = getContainer(env.LIANES_API, "singleton");

        await container.startAndWaitForPorts({
          startOptions: {
            envVars: {
              DB_HOST: env.DB_HOST,
              DB_PORT: env.DB_PORT,
              DB_USER: env.DB_USER,
              DB_PASS: env.DB_PASS,
              DB_NAME: env.DB_NAME,
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
};
