import { Container, getRandom } from "@cloudflare/containers";

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
// MySQL (Aiven) and Pinecone, not in the container. Load-balanced via
// getRandom() rather than session-affinity (getByName()).
export class LianesApi extends Container {
  defaultPort = 8080;
  requiredPorts = [8080];
  sleepAfter = "10m";
  enableInternet = true; // needed to reach Aiven MySQL, Pinecone, HuggingFace
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const container = await getRandom(env.LIANES_API, 3);

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

    return container.fetch(request);
  },
};
