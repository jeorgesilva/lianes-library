# Liane's Library — Web

React + Vite + Tailwind frontend for Liane's Library, replacing the Streamlit UI in `src/frontend/`.

```bash
npm install
npm run dev      # local dev, talks to VITE_API_URL (see .env)
npm run build    # production build to dist/
```

`.env` sets `VITE_API_URL` — the deployed API from `../cloudflare/`.
