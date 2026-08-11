# Prompt para Claude Code — Liane's Library: redesign estilo Netflix + 6 novas funcionalidades

## Como usar este documento

Este documento é o prompt a ser colado no Claude Code, rodando dentro do repositório `lianes-library`. Ele é longo de propósito — cobre 6 funcionalidades novas mais um redesign de UI. Duas formas de usar:

1. **Tudo de uma vez**: colar o documento inteiro e pedir para seguir as fases da seção 7 em ordem, uma de cada vez, com commit ao final de cada fase.
2. **Fase por fase** (recomendado para revisão mais tranquila): colar apenas a seção 0–3 (contexto + design system) na primeira sessão, depois colar cada bloco da seção 4 separadamente nas sessões seguintes.

Tudo abaixo foi escrito depois de ler o código real do repositório (não é um roteiro genérico) e de pesquisa sobre o mercado atual de apps de leitura, APIs de livros/preços/eventos e padrões de UI estilo Netflix em 2026. As decisões de produto e design já foram tomadas — o que precisa de julgamento de engenharia (nomes de arquivo, componentes exatos, etc.) fica a critério do Claude Code, seguindo as convenções do projeto descritas abaixo.

---

## 0. Contexto do projeto (leia antes de codar)

**Stack real (confirmado no código, não no README — o README está um pouco desatualizado):**

- Frontend: React 19 + TypeScript + Vite 8 + TanStack Query v5 + React Router v7 + Tailwind CSS v4, em `web/`. Deploy via Cloudflare Pages (`web/wrangler.jsonc`).
- Backend: FastAPI rodando dentro de um **Cloudflare Container** (Durable Object), não em Workers puro. O Worker (`cloudflare/src/index.ts`) roteia requests para o container e expõe um proxy interno (`/__d1/query`) para o container falar com o D1, já que bindings do D1 só existem no Worker. Há um Cron Trigger diário (`0 8 * * *`) já configurado.
- Banco: Cloudflare D1 (SQLite). Schema real em `cloudflare/migrations/0001_initial.sql` e `0002_users_and_ownership.sql` — **essa é a fonte da verdade**, não `src/schemas/*.py` (esses schemas Pydantic estão parcialmente desatualizados; ex.: `BookOut` usa `id` genérico enquanto a tabela real e o frontend usam `book_id`).
- IA/busca semântica: já existe embeddings (`src/ai/embeddings.py`), vector store Pinecone (`src/ai/vectorstore.py`, `PINECONE_INDEX_NAME`), indexação (`src/ai/indexer.py`) e um endpoint `/search/vibe` usado pela página `SmartAssistant`. **Reaproveite essa infraestrutura para o motor de recomendações (seção 4.1) em vez de criar um sistema de embeddings paralelo.**
- Notificações: `cloudflare/src/notifications.ts` já detecta empréstimos atrasados e monta e-mails, mas o envio é **mockado** (`console.log`, sem provedor real conectado). Reaproveite esse padrão para as novas notificações (preço, devolução) — ver seção 5.
- Scanner de código de barras (`web/src/components/BarcodeScanner.tsx`, via `@zxing`) e lookup por ISBN na Open Library (`api.openLibrary.lookup`) já existem e devem ser reaproveitados nos fluxos de adicionar livro à wishlist/estante.
- Tema visual já parcialmente encaminhado: dark mode com paleta índigo/roxo definida em `web/src/index.css` (`--color-bg`, `--color-surface`, `--color-primary: #6c63ff`, `--color-accent: #a855f7`), e um componente `BookCarousel.tsx` que já é uma primeira tentativa de layout estilo Netflix na `Dashboard.tsx` ("🍿 Liane's Discovery"). **Não comece do zero — evolua isso.**
- Linter: `oxlint` (`npm run lint` em `web/`). Não há suíte de testes automatizados hoje.

**Confusão a evitar — muito importante:** o app já tem uma feature de **"Loans"** (`transactions` table, `/loans` rota, página `Loans.tsx`), que representa livros que **Liane empresta PARA outras pessoas** (`borrowers`). A nova funcionalidade #5 pedida pelo usuário é o **oposto**: livros que a própria Liane **pegou emprestado de terceiros** (biblioteca, amigos). São conceitos e tabelas diferentes — não reutilize `transactions`/`Loans` para isso. Ver seção 4.5.

---

## 1. Objetivo e princípios de design

Construir sobre o app existente (não substituir) para que Liane's Library vire um hub pessoal de leitura: descobrir o que ler a seguir, acompanhar o que está lendo, não perder dinheiro comprando o que já tem parecido, não esquecer de devolver livros emprestados, e descobrir eventos literários — tudo com uma UI que tem a densidade e o ritmo de navegação de um serviço de streaming (Netflix), mas com identidade visual própria de "biblioteca pessoal", não um clone genérico vermelho/preto.

Princípios (baseados em pesquisa de tendências de UI 2026 e nos apps líderes de tracking de leitura hoje — StoryGraph, Fable, Bookly):

1. **Evolua a identidade visual existente, não a substitua.** A paleta roxo/índigo já escolhida é um diferencial (evita parecer clone de streaming de vídeo). Mantenha `--color-primary`/`--color-accent`, mas amplie o sistema de tokens (ver seção 2).
2. **Mecânica Netflix = fileiras horizontais + hero + hover rico + baixa carga cognitiva por tela**, aplicada a "prateleiras" de livros em vez de pôsteres de filme. Cada fileira é um filtro implícito (gênero, status de leitura, recomendação) — não jogue tudo em uma grade só.
3. **"Crafted, not templated"**: tipografia com personalidade (um par de fontes, não só a stack padrão do sistema), motion que comunica estado (ex.: card de livro migrando de "Quero Ler" para "Lendo"), não decoração gratuita.
4. **IA como copiloto, não piloto automático**: toda recomendação/sugestão precisa de um motivo visível ("Porque você tem *Duna* na estante"), e uma forma fácil de dispensar ("não me interessa").
5. **Funciona sem as APIs externas pagas.** Toda funcionalidade que depende de API de terceiros (preço, eventos) deve degradar graciosamente (cache vazio, mensagem clara, sem crash) se a chave não estiver configurada — o usuário pode não configurar tudo de primeira.

---

## 2. Sistema visual (design system)

Evoluir `web/src/index.css` (tema Tailwind v4 via `@theme`) e os componentes em `web/src/components/`:

**Cores** — manter `bg`, `surface`, `primary`, `accent` existentes e adicionar:
- `surface-hover` / `surface-raised` (elevação em 2 níveis, para cards vs. modais/popovers)
- `success` (verde, item disponível / meta batida), `warning` (âmbar, devolução ou vencimento próximo), `danger` (vermelho, atrasado) — hoje o app usa cores ad-hoc (`text-red-400` hardcoded em vários lugares); centralizar como tokens.
- `price-drop` (um tom quente, ex. âmbar/dourado) para destacar promoções na wishlist — contraste proposital com o roxo frio do resto da UI, para chamar atenção sem parecer erro.

**Tipografia**: adicionar um par de fontes via `@font-face`/Google Fonts self-hosted (evitar CDN externo se possível, por performance) — uma serifada com caráter para títulos de livros e headers (remete a "biblioteca", diferencia de UI de streaming genérica) + manter a sans existente para UI/corpo de texto. Definir `--font-display` e `--font-sans` como tokens.

**Componentes a criar/evoluir em `web/src/components/`:**
- `HeroBanner.tsx` — banner grande no topo da Home, mostra o livro "Continue lendo" (do reading queue, seção 4.3) com progresso, ou um destaque de recomendação se nada estiver em andamento. Botões de ação primária/secundária (ex. "Abrir ficha de leitura" / "Ver detalhes").
- Evoluir `BookCarousel.tsx`: setas de navegação nas bordas (aparecem no hover, scroll suave, não só `overflow-x-auto` cru), hover card mais rico (capa + título + autor + 1-2 metadados + ações rápidas: adicionar à wishlist/agenda) em vez do overlay preto atual que só mostra o título, skeleton de loading, e suporte a "porque..." como subtítulo opcional da fileira.
- `BookCard.tsx` — extrair o card individual do carrossel para um componente reutilizável (hoje está inline dentro de `BookCarousel.tsx`), usado também em grids (Wishlist, Catalog, Recommendations).
- `StatusBadge.tsx` — badge genérico usando os novos tokens semânticos (disponível/atrasado/vencendo/promoção).
- `NotificationBell.tsx` — sino no header/sidebar com contador, abre um painel com a central de notificações (seção 5).
- `EmptyState.tsx` e `Skeleton.tsx` — hoje várias páginas só mostram texto simples de loading/vazio; padronizar.

**Motion**: transições de 150–250ms em hover/scale de cards, animação de entrada em fileiras (stagger leve), respeitar `prefers-reduced-motion`. Nada decorativo sem função (ex.: não usar confete/parallax só por estética).

**Responsivo**: fileiras viram carrossel com swipe por toque no mobile; sidebar (`Sidebar.tsx`) vira navegação inferior ou menu hambúrguer abaixo de um breakpoint — hoje o layout é fixo `flex` sem tratamento mobile.

---

## 3. Arquitetura de informação / navegação

Reorganizar `Sidebar.tsx` (hoje uma lista plana de 6 itens) em grupos com headers, mantendo rotas existentes estáveis sempre que possível:

```
DESCOBRIR
  Home                /            (Dashboard.tsx redesenhado com hero + fileiras)
  Recomendações       /recommendations   [NOVO]
  Eventos             /events            [NOVO]

MINHA LEITURA
  Minha Leitura       /reading           [NOVO — abas: Fila de Leitura | Diário]

MEU ACERVO
  Catálogo            /catalog     (existente)
  Wishlist            /wishlist          [NOVO]

EMPRÉSTIMOS
  Emprestei           /loans       (existente — considerar renomear label para "Loaned Out" na UI para reduzir ambiguidade com o item abaixo)
  Peguei Emprestado   /borrowed-by-me    [NOVO]

PESSOAS
  Borrowers           /borrowers   (existente)

Smart Assistant       /assistant   (existente, fica fora dos grupos — é uma ferramenta transversal)

INSIGHTS
  Analytics            /analytics  (existente, expandido — ver 6.1)
```

Nota de decisão: "Agenda de Leitura" (fila quero-ler/lendo/lido) e "Ficha de Leitura" (diário) foram agrupadas em uma única página com abas (`/reading`) em vez de dois itens de menu separados, porque são o mesmo objeto visto de dois ângulos (o diário de um livro só existe no contexto de um item da fila) — isso evita inflar a sidebar para 12+ itens. Journal individual de um livro também deve ser acessível a partir do próprio card do livro (catálogo, fila, hero), não só pela aba central.

Idioma: a UI existente é majoritariamente em inglês (`Dashboard`, `Book Catalog`, `Loans`, `Your shelf is empty...`), com uma exceção pontual em português (`Sair` no botão de logout). **Novas strings de UI devem seguir o inglês**, para manter consistência — não deixe a mistura crescer. Isso não afeta este documento, que é a especificação em português para você (humano) revisar.

---

## 4. Funcionalidades novas

### 4.1 Recomendações baseadas no acervo

**Objetivo:** sugerir livros parecidos com o que Liane já tem, agrupados por categoria, com link de onde comprar e preço estimado.

**Motor de recomendação — reaproveitar a infra de IA existente, não recriar:**
1. Calcular um "perfil de gosto" a partir da coleção: frequência de gênero/autor (campo `genre` e `author` da tabela `books`) e, para um sinal melhor, o centróide dos embeddings já gerados para a busca semântica (`src/ai/embeddings.py` + índice Pinecone).
2. Buscar candidatos que **não estão no acervo** (dedupe por ISBN e por título+autor normalizado) via Google Books API (gêneros/autores dominantes) e/ou Open Library subjects API.
3. Ranquear candidatos gerando embedding da sinopse de cada um e comparando com o centróide de gosto (mesma pipeline de `sentence-transformers` já usada).
4. Cachear os resultados (tabela `recommendation_cache`, TTL sugerido de 7 dias) — não recalcular a cada carregamento de tela.

**Onde comprar / preço estimado — abstração de provedor (interface, não hardcode):**
- Google Books API: campo `saleInfo` já traz preço quando disponível (grátis, 100 req/dia sem chave adicional).
- Bookshop.org: sem API pública documentada, mas tem link de afiliado (10% de comissão) — usar como CTA "Comprar em livraria independente", prioridade visual sobre Amazon (diferencial ético, também é tendência de mercado — leitores valorizam apoiar livrarias locais).
- Amazon (link de afiliado, se Liane tiver Associates tag) como fallback.
- BookScouter API como fonte adicional de preço (usados/seminovos), especialmente útil combinado com a ideia de "valor de revenda" (ver seção 6.3).
- Cachear preços junto com o candidato (mesma tabela ou uma `recommendation_prices`), já que preço não precisa ser realtime aqui (diferente da wishlist, que monitora ativamente).

**Migration (`cloudflare/migrations/0003_recommendations.sql`):**
```sql
CREATE TABLE recommendation_cache (
  recommendation_id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_id INTEGER NOT NULL REFERENCES users(user_id),
  title TEXT NOT NULL,
  author TEXT,
  isbn TEXT,
  cover_url TEXT,
  reason TEXT,                -- ex: "Because you have Dune on your shelf"
  source_genre TEXT,
  match_score REAL,
  best_price REAL,
  best_price_source TEXT,
  best_price_url TEXT,
  generated_at TEXT NOT NULL DEFAULT (datetime('now')),
  expires_at TEXT NOT NULL
);

CREATE TABLE recommendation_dismissals (
  dismissal_id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_id INTEGER NOT NULL REFERENCES users(user_id),
  isbn TEXT,
  title TEXT NOT NULL,
  dismissed_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_recommendation_cache_owner ON recommendation_cache(owner_id);
CREATE INDEX idx_recommendation_dismissals_owner ON recommendation_dismissals(owner_id);
```

**Backend:** novo router `src/api/routers/recommendations.py` (`GET /recommendations/`, `POST /recommendations/{id}/dismiss`, `POST /recommendations/refresh` para forçar recálculo).

**Frontend:** página `/recommendations` (grid + fileiras por gênero na Home), reaproveitando `BookCarousel`/`BookCard`. Cada card mostra o motivo ("Porque você lê Fantasia" / "Porque você tem X"), preço estimado, botão "Comprar" (abre o link do melhor provedor) e "Não me interessa".

---

### 4.2 Wishlist com monitoramento diário de preço

**Objetivo:** Liane salva títulos que quer comprar; o app busca preço diariamente e avisa quando achar promoção.

**Migration (`0004_wishlist.sql`):**
```sql
CREATE TABLE wishlist_items (
  wishlist_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_id INTEGER NOT NULL REFERENCES users(user_id),
  title TEXT NOT NULL,
  author TEXT,
  isbn TEXT,
  cover_url TEXT,
  target_price REAL,             -- opcional: "me avise abaixo de R$ X"
  notes TEXT,
  status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE','PURCHASED','ARCHIVED')),
  added_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE wishlist_price_snapshots (
  snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
  wishlist_item_id INTEGER NOT NULL REFERENCES wishlist_items(wishlist_item_id),
  source TEXT NOT NULL,          -- 'google_books' | 'bookshop' | 'amazon' | 'bookscouter'
  price REAL NOT NULL,
  currency TEXT NOT NULL DEFAULT 'BRL',
  url TEXT,
  checked_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_wishlist_owner ON wishlist_items(owner_id);
CREATE INDEX idx_wishlist_snapshots_item ON wishlist_price_snapshots(wishlist_item_id, checked_at);
```

**Job diário (Cloudflare Cron):** adicionar um segundo horário ao array `triggers.crons` em `cloudflare/wrangler.jsonc` (ex.: `"0 9 * * *"`) e, no handler `scheduled()` de `cloudflare/src/index.ts`, distinguir os jobs por `event.cron` (hoje só existe um job, então essa ramificação ainda não existe — precisa ser adicionada). Para cada `wishlist_items` ativo: consultar os provedores de preço (mesma abstração da seção 4.1), gravar um snapshot, e comparar com o menor preço anterior / `target_price`; se caiu de forma relevante (ex. >10%) ou bateu o alvo, gerar uma notificação (seção 5).

**Entradas para adicionar à wishlist:** formulário manual, busca por título/autor (Google Books), scan de ISBN (reaproveitar `BarcodeScanner.tsx` + `openLibrary.lookup`, mesmo padrão já usado em `BookCatalog.tsx`), e um botão "Adicionar à wishlist" direto nos cards de Recomendações.

**Frontend:** página `/wishlist` em grid, cada card com sparkline simples do histórico de preço (reaproveitar padrão de gráfico já usado em `Analytics.tsx`/`charts/`), badge de "menor preço encontrado" e % de queda, filtros por status/queda de preço.

---

### 4.3 Minha Leitura — fila (quero ler / lendo / lido) + diário

**Objetivo:** Liane organiza o que vai ler a seguir, o que está lendo agora, o que já leu — e escreve um diário sobre cada livro em andamento.

**Migration (`0005_reading.sql`):**
```sql
CREATE TABLE reading_log (
  reading_log_id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_id INTEGER NOT NULL REFERENCES users(user_id),
  book_id INTEGER REFERENCES books(book_id),   -- NULL se ainda não está no acervo (ex.: veio da wishlist)
  title TEXT NOT NULL,
  author TEXT,
  status TEXT NOT NULL DEFAULT 'WANT_TO_READ' CHECK(status IN ('WANT_TO_READ','READING','READ','DNF')),
  queue_position INTEGER,        -- ordenação manual dentro de WANT_TO_READ
  started_at TEXT,
  finished_at TEXT,
  rating INTEGER CHECK(rating BETWEEN 1 AND 5),
  current_page INTEGER,
  total_pages INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE journal_entries (
  entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_id INTEGER NOT NULL REFERENCES users(user_id),
  reading_log_id INTEGER NOT NULL REFERENCES reading_log(reading_log_id),
  entry_date TEXT NOT NULL DEFAULT (datetime('now')),
  content TEXT NOT NULL,
  page_at_entry INTEGER,
  mood TEXT,                     -- tag livre curta, ex. "emocionante", "lento"
  contains_spoilers INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_reading_log_owner_status ON reading_log(owner_id, status);
CREATE INDEX idx_journal_entries_reading_log ON journal_entries(reading_log_id, entry_date);
```

**Backend:** router `src/api/routers/reading.py` — CRUD de `reading_log` (incluindo endpoint de mudança de status, ex. `PATCH /reading/{id}/status`, que seta `started_at`/`finished_at` automaticamente) e sub-rotas de `journal_entries` (`GET/POST /reading/{id}/entries`).

**Frontend:** página `/reading` com duas abas:
- **Fila**: board estilo kanban com 3 colunas (Quero Ler / Lendo / Lido) — mover card entre colunas via drag-and-drop se o escopo permitir, ou botões de avançar/voltar como alternativa mais simples caso drag-and-drop não caiba no tempo disponível. Coluna "Lendo" mostra barra de progresso (`current_page`/`total_pages`).
- **Diário**: lista cronológica de entradas do livro selecionado (estilo diário/timeline, com data), toggle de spoiler que borra o texto até o clique, textarea para nova entrada.

O livro "Lendo agora" (o primeiro item com `status = READING`, ou o mais recente) alimenta o `HeroBanner` da Home (seção 2) com progresso e um atalho direto para abrir o diário desse livro.

---

### 4.4 Peguei Emprestado (livros que Liane pegou emprestado de terceiros)

**⚠️ Não é o mesmo que a feature "Loans" existente** (que é o oposto: Liane emprestando PARA outras pessoas, tabela `transactions`). Esta é uma tabela e fluxo totalmente novos.

**Migration (`0006_borrowed_by_me.sql`):**
```sql
CREATE TABLE borrow_records (
  borrow_id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_id INTEGER NOT NULL REFERENCES users(user_id),
  title TEXT NOT NULL,
  author TEXT,
  isbn TEXT,
  cover_url TEXT,
  lender_name TEXT NOT NULL,      -- pessoa ou instituição (ex. "Biblioteca Municipal", "Marina")
  borrowed_date TEXT NOT NULL,
  due_date TEXT,
  returned_date TEXT,
  reminder_lead_days INTEGER NOT NULL DEFAULT 3,
  notes TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_borrow_records_owner ON borrow_records(owner_id);
CREATE INDEX idx_borrow_records_due_date ON borrow_records(due_date) WHERE returned_date IS NULL;
```

**Backend:** router `src/api/routers/borrowed.py`, CRUD completo + `POST /borrowed/{id}/return` (seta `returned_date`), reaproveitando o padrão de `loans.py` existente (mesmo estilo de código, tabela e semântica diferentes).

**Frontend:** página `/borrowed-by-me`, lista/cards similar à página `Loans.tsx` (reaproveitar `StatusBadge` para atrasado/vencendo/em dia), mas com rótulos e formulário próprios ("De quem peguei emprestado", "Data de devolução combinada").

**Lembretes:** entram no job diário unificado (seção 5) — X dias antes do `due_date` (configurável por item via `reminder_lead_days`) e no dia seguinte se ainda não devolvido.

---

### 4.5 Eventos literários

**Objetivo:** o app busca e mostra eventos para quem gosta de livros/literatura perto de Liane.

**Nota de viabilidade importante (verificada na documentação atual):** a API pública de descoberta/busca do Eventbrite foi descontinuada — a API deles hoje só gerencia eventos da própria organização, não serve para "buscar eventos de terceiros por cidade". Não implemente a integração assumindo que isso funciona. Alternativas reais:
- **Ticketmaster Discovery API** — tem busca de verdade, chave de desenvolvedor gratuita, boa cobertura para feiras literárias grandes e eventos com autores conhecidos; cobertura fraca para clubes de leitura pequenos/independentes.
- **SerpApi (Google Events)** — bom para cobertura ampla de eventos locais pequenos (o que o Google já agrega), mas é um serviço pago de terceiros.
- Fallback manual: um formulário simples (mesmo que só para a própria Liane) de "sugerir/cadastrar evento" para não deixar a feature 100% refém da cobertura de API externa.

**Decisão de escopo:** implementar por trás de uma interface `EventProvider` (não acoplar direto a um SDK), começando com **uma** fonte (a que Liane conseguir obter chave mais facilmente) + o formulário manual como fallback sempre disponível. Não prometa cobertura completa automática de eventos pequenos — isso hoje realisticamente exige um serviço pago ou curadoria manual.

**Migration (`0007_events.sql`):**
```sql
CREATE TABLE literary_events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,           -- 'ticketmaster' | 'serpapi' | 'manual'
  external_id TEXT,
  title TEXT NOT NULL,
  description TEXT,
  venue_name TEXT,
  city TEXT,
  event_date TEXT NOT NULL,
  url TEXT,
  image_url TEXT,
  cached_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE user_event_preferences (
  owner_id INTEGER PRIMARY KEY REFERENCES users(user_id),
  city TEXT,
  radius_km INTEGER DEFAULT 30
);

CREATE INDEX idx_literary_events_date ON literary_events(event_date);
```

**Job semanal** (eventos não mudam a toda hora — não precisa ser diário): novo horário em `triggers.crons`, atualiza `literary_events` filtrando por cidade salva em `user_event_preferences` e por palavras-chave (feira de livro, lançamento, clube do livro, sarau, encontro de leitura, festival literário).

**Frontend:** página `/events` (grid de cards: data, local, distância aproximada, botão "Exportar para calendário" gerando um `.ics` — não depende de API externa, é só geração de arquivo), campo de cidade nas configurações do usuário, e opcionalmente uma fileira "Eventos perto de você" na Home para manter a consistência com o padrão Netflix.

---

## 5. Infraestrutura transversal

### 5.1 Notificações unificadas

Hoje `cloudflare/src/notifications.ts` só cobre empréstimos atrasados, e o envio de e-mail é mockado. Para as novas features (queda de preço, devolução próxima), **não crie 3 sistemas de notificação paralelos** — consolide:

1. **Central de notificações in-app** (nova tabela, sempre funciona, não depende de provedor externo):
```sql
CREATE TABLE notifications (
  notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_id INTEGER NOT NULL REFERENCES users(user_id),
  type TEXT NOT NULL CHECK(type IN ('OVERDUE_LOAN','PRICE_DROP','BORROW_DUE_SOON','EVENT_NEARBY')),
  title TEXT NOT NULL,
  body TEXT,
  link TEXT,
  read_at TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
```
   Exposta via `GET /notifications/`, `POST /notifications/{id}/read`, renderizada no `NotificationBell.tsx` (seção 2).

2. **E-mail real via Resend**: Cloudflare Workers roda em V8 e não suporta SMTP (bibliotecas tipo Nodemailer não funcionam) — o caminho padrão é uma API HTTP, e Resend é a opção mais direta de integrar com Workers (tutorial oficial da Cloudflare existe para esse par). Free tier cobre bem o uso pessoal (milhares de e-mails/mês, mais que suficiente para 1 usuário). Trocar a função `sendEmail()` mockada em `notifications.ts` por uma chamada `fetch` para `https://api.resend.com/emails`, usando `RESEND_API_KEY` via `wrangler secret put`. Consolidar em **um digest diário** (não um e-mail por evento) cobrindo: empréstimos atrasados (já existe), devoluções próximas/atrasadas (novo), quedas de preço na wishlist (novo) — evita spammar Liane com e-mails separados.

3. Estender `scheduled()` em `cloudflare/src/index.ts` para rodar, no mesmo job diário (ou jobs adicionais conforme granularidade da seção 4.2/4.5), a checagem de preço, a checagem de devolução e a geração de linhas em `notifications`, além do que já existe para empréstimos.

### 5.2 Variáveis de ambiente / chaves necessárias

Documentar em `.env`/README (não commitar valores reais) e listar para Liane obter antes de rodar em produção:
- `RESEND_API_KEY` — e-mails reais (hoje mockado).
- `GOOGLE_BOOKS_API_KEY` — opcional, aumenta rate limit de 100/dia para uso autenticado.
- `BOOKSCOUTER_API_KEY` — preço de usados (se optar por essa fonte).
- Tag de afiliado Amazon e/ou Bookshop.org (se optar por monetizar/rastrear cliques).
- `TICKETMASTER_API_KEY` ou `SERPAPI_KEY` — eventos (escolher uma fonte, seção 4.5).

Toda integração deve checar se a env var existe e desabilitar a funcionalidade específica com uma mensagem clara na UI ("Configure uma chave de API para ativar preços de eventos") em vez de quebrar a aplicação.

---

## 6. Funcionalidades adicionais sugeridas (além do pedido original)

Baseado no que hoje diferencia os líderes do mercado (StoryGraph = estatísticas e mood tracking; Fable = social; Bookly = hábitos/streaks) e em lacunas que nenhum concorrente cobre bem (nenhum faz gestão de empréstimo físico + wishlist com monitoramento de preço + assistente semântico da própria estante como este app). Priorizadas, não obrigatórias:

1. **Retrospectiva de leitura / estatísticas** (alto valor, reaproveita `Analytics.tsx` já existente): páginas lidas por mês, ritmo de leitura, distribuição por gênero, "ano em resumo" — é o principal motivo dos usuários preferirem StoryGraph a Goodreads hoje.
2. **Tags de humor/ritmo** (mood/pace, estilo StoryGraph) em `reading_log`/`journal_entries` — melhora tanto o diário quanto o motor de recomendação (seção 4.1) a custo baixo de implementação.
3. **Valor de revenda do acervo**: usando a mesma integração BookScouter da wishlist, mostrar no Catálogo quanto cada livro vale hoje usado — reaproveita infraestrutura já construída.
4. **Metas e sequências de leitura** (streaks, meta anual de livros) — padrão Bookly, gamificação leve.
5. **Rótulos explicáveis nas recomendações** ("Porque você tem X") — já especificado na 4.1, reforçando aqui como princípio geral a manter em qualquer sugestão automática futura.
6. **Priorizar livrarias independentes nos links de compra** (Bookshop.org antes de Amazon) — diferencial de posicionamento, não só técnico.
7. **Exportação `.ics`** para eventos e para prazos de devolução (peguei emprestado) — baixo custo, sem dependência externa, alto valor prático.
8. **Command palette (⌘K / Ctrl+K)** para busca universal entre catálogo, wishlist, diário e eventos — tendência de UI 2026 ("crafted, not templated") e reduz a necessidade de navegar pela sidebar cada vez mais cheia.
9. **PWA + Web Push** (stretch, mais custoso): e-mail pode passar despercebido; notificação push no navegador/celular é mais confiável para "achou promoção" e "devolver amanhã". Vale considerar depois que o digest por e-mail e a central in-app estiverem estáveis.

Não incluí features sociais completas (clubes de leitura estilo Fable) porque o app é pessoal/familiar por natureza (é literalmente "a biblioteca da Liane") — se o objetivo for compartilhar com família, uma alternativa mais barata é um link somente-leitura da estante, não uma rede social inteira.

---

## 7. Plano de execução por fases

Ordem sugerida (cada fase termina em estado funcional e pode ser um commit/PR separado):

- **Fase 0** — Sistema visual (seção 2) + reorganização da sidebar/IA (seção 3), sem novas features ainda. Dá a base visual para todo o resto.
- **Fase 1** — Minha Leitura: fila + diário (seção 4.3). Não depende de nenhuma API externa — bom primeiro incremento para validar o padrão de UI novo.
- **Fase 2** — Peguei Emprestado (seção 4.4) + notificação in-app básica (seção 5.1, item 1). Também sem dependência externa.
- **Fase 3** — Wishlist + monitoramento de preço (seção 4.2) + Resend real (seção 5.1, item 2) + cron.
- **Fase 4** — Recomendações (seção 4.1), reaproveitando embeddings/Pinecone existentes.
- **Fase 5** — Eventos literários (seção 4.5).
- **Fase 6** — Itens da seção 6 conforme prioridade/tempo disponível.

## 8. Critérios de aceite

Antes de considerar cada fase pronta:
- `npm run lint` (oxlint) e `tsc -b` passam sem erro em `web/`.
- Toda nova tabela tem índice nas colunas usadas em `WHERE`/`JOIN` (seguindo o padrão já usado nas migrations existentes).
- Toda feature que depende de chave de API externa ausente falha graciosamente na UI, não quebra a tela.
- Dark mode e responsivo (mobile) testados visualmente em cada tela nova.
- Nenhuma tabela/rota nova colide em nome ou semântica com `transactions`/`Loans` (ver aviso da seção 4.4).
- Job de cron novo testável manualmente (seguir o padrão já existente de `/__internal/check-overdue` em `cloudflare/src/index.ts` — criar equivalentes internos para os novos jobs).

## 9. Restrições / não-objetivos (por agora)

- Não migrar `src/frontend/app.py` (Streamlit) — está sendo substituído pelo app React e não vale investir ali.
- Não implementar rede social completa (seguir usuários, feed) — fora de escopo, ver nota no final da seção 6.
- Não trocar a paleta de cor roxa/índigo existente por vermelho/preto "Netflix literal" — a instrução de "parecido com Netflix" é sobre padrão de navegação e densidade de UI, não sobre clonar a marca visual.
- Não bloquear nenhuma feature nova por falta de chave de API paga — sempre ter um caminho gratuito ou manual, mesmo que mais limitado.
