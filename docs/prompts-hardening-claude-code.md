# Liane's Library — Prompts de Execução (Claude Code)

Companion do `Lianes-Library_Auditoria-Prontidao-Venda.docx`. Cada bloco abaixo é um prompt autocontido, pronto para colar numa sessão nova do Claude Code dentro do repositório `lianes-library`. Já vêm com os arquivos exatos a ler, o comportamento-alvo e os limites de escopo — a ideia é que o Code vá direto para a edição em vez de explorar o repo do zero.

**Como usar:** rode um prompt por vez, revise o diff, dê commit, só então passe para o próximo. Não peça para rodar todos de uma vez numa sessão só — cada um é uma unidade de revisão/rollback independente. Nenhum prompt manda fazer deploy; isso fica com você depois de revisar.

**Ordem e dependências:**

| # | Prompt | Prioridade (relatório) | Depende de |
|---|---|---|---|
| 1 | Corrigir vazamento entre usuários (Pinecone) | Crítico | — |
| 2 | Endurecer superfície pública da API | Alto | — |
| 3 | Remover MySQL/Aiven legado | Alto | — |
| 4 | Medir uso por conta | Alto (monetização) | — |
| 5 | Clerk — backend | Base da migração de auth | — |
| 6 | Clerk — frontend | Base da migração de auth | 5 |
| 7 | Exclusão de conta em cascata | Crítico/Alto (LGPD) | 5, 6 |
| 8 | Cobrança com Stripe | Crítico (monetização) | 4, 5, 6 |

1, 2, 3 e 4 são independentes entre si — pode rodar em qualquer ordem ou em paralelo em worktrees separadas. 5→6→7 e 5→6→8 são sequenciais.

---

## Antes de começar (ações suas, fora do Claude Code)

Nenhum agente de código consegue fazer isso — são passos manuais em painéis externos:

- **Aiven:** rotacionar ou revogar a senha do usuário `avnadmin` no painel da Aiven (e decidir se desliga a instância). O Prompt 3 só limpa o código/repositório — a credencial em si só morre quando você a revoga lá.
- **Stripe:** criar conta (free tier), modo teste, pegar `Secret Key` e `Publishable Key`. Depois de rodar o Prompt 8, criar o produto "Pro" com preço mensal e pegar o `Price ID`.
- **Clerk:** criar conta (free tier), criar uma aplicação, pegar `Publishable Key` e `Secret Key`. Habilitar Google como login social é rápido (alguns cliques). **Apple exige Apple Developer Program (US$ 99/ano) e configuração extra no console da Apple** — se não quiser esse custo agora, lance só com Google + e-mail/senha (padrão do Clerk) e adicione Apple depois sem precisar tocar no código de novo.
- Depois de cada prompt relevante: configurar os secrets reais via `wrangler secret put` (`CLERK_SECRET_KEY`, `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, `STRIPE_PRICE_ID_PRO`) e a env var do frontend (`VITE_CLERK_PUBLISHABLE_KEY`). Os prompts deixam o código pronto mas não colocam nenhuma chave real em lugar nenhum.

---

## Prompt 1 — Corrigir vazamento de dados entre usuários (Pinecone / Vibe Search / Chat)

```
Você vai corrigir uma falha de isolamento entre usuários (multi-tenancy) no backend Python
(FastAPI) do repositório lianes-library. É correção de segurança, escopo restrito — não
mexa em nada fora do listado abaixo.

Leia primeiro: src/api/routers/search.py, src/api/routers/chat.py, src/ai/vectorstore.py,
src/ai/agents/librarian.py, src/ai/indexer.py, src/db/crud/books.py (só a assinatura de
get_books), src/db/crud/users.py.

Problema: GET /search/vibe e POST /chat consultam o índice Pinecone inteiro sem filtrar por
dono do livro. vibe_search() em src/ai/vectorstore.py não recebe owner_id, e o índice nunca
grava owner_id nos metadados de cada vetor (src/ai/indexer.py). Resultado: qualquer conta
autenticada vê livros de outras contas via busca semântica e via chat. Já há um comentário em
src/ai/recommendations.py reconhecendo esse problema — ele não precisa de alteração, já está
contornado ali.

Faça:
1. Em src/db/crud/users.py: adicione list_all_user_ids() -> list[int], retornando todos os
   user_id da tabela users.
2. Em src/ai/indexer.py: reescreva sync_books_to_vectorstore para iterar por todos os
   usuários (passo 1) e, para cada um, buscar livros via get_books(owner_id=...) e gravar em
   cada Document.metadata o campo "owner_id": owner_id (int). Corrija o texto de log que hoje
   menciona "MySQL (Aiven)" — a fonte é o D1, via get_books.
3. Em src/ai/vectorstore.py: adicione parâmetro owner_id: int obrigatório em
   vibe_search(query, owner_id, limit=5) e passe filter={"owner_id": owner_id} para
   db.similarity_search_with_score(...).
4. Em src/api/routers/search.py: renomeie _owner_id para owner_id e repasse para
   vibe_search(q, owner_id=owner_id, limit=limit).
5. Em src/ai/agents/librarian.py: adicione parâmetro owner_id em
   chat_with_librarian(user_message, owner_id) e repasse para vibe_search.
6. Em src/api/routers/chat.py: renomeie _owner_id para owner_id e repasse para
   chat_with_librarian(request.message, owner_id).
7. Adicione um teste (novo arquivo, ex. tests/test_vibe_search_isolation.py, pytest) que cria
   dois usuários fictícios com livros diferentes e garante que a busca semântica de um nunca
   retorna livros do outro. Use mocks para a chamada ao Pinecone se não houver ambiente de
   teste real disponível — o importante é travar o contrato de que owner_id sempre é passado
   e respeitado.

Não faça: não mexa em src/ai/recommendations.py. Não faça deploy nem rode reindexação em
produção. Ao final, me avise explicitamente que depois do merge preciso rodar o indexer de
novo (ele agora itera todos os usuários) para regravar os vetores existentes com owner_id,
já que os vetores atuais não têm esse metadado.

Critério de pronto: código importa sem erro, teste novo passa, grep confirma que nenhuma
chamada a vibe_search ficou sem owner_id.
```

---

## Prompt 2 — Endurecer superfície pública da API (CORS, Swagger, Rate Limiting)

```
Você vai reduzir a superfície de ataque pública da API do repositório lianes-library. Escopo
restrito aos itens abaixo.

Leia primeiro: src/api/main.py, cloudflare/src/index.ts, cloudflare/wrangler.jsonc,
web/vite.config.ts (só para confirmar a porta do dev server local).

Faça:
1. CORS (src/api/main.py): troque allow_origins=["*"] por uma lista vinda da env var
   ALLOWED_ORIGINS (string separada por vírgula), com fallback padrão para
   ["https://lianes-library.pages.dev", "http://localhost:<porta do vite>"] se a env var não
   existir. Documente a nova env var no README.
2. Swagger público (src/api/main.py): desabilite /docs, /redoc e /openapi.json por padrão,
   controlado por env var (ex. ENABLE_API_DOCS=true habilita; ausente ou "false" desabilita —
   padrão é desabilitado). Use docs_url=None, redoc_url=None, openapi_url=None
   condicionalmente na criação do FastAPI(...).
3. Rate limiting no Worker (cloudflare/src/index.ts), antes de proxiar para o Container, nos
   caminhos que começam com /auth/login, /auth/register, /search/vibe e /chat. Use o binding
   nativo de Rate Limiting do Cloudflare Workers se a versão instalada do wrangler suportar
   (adicione o binding em cloudflare/wrangler.jsonc); se não tiver certeza da API atual desse
   binding, implemente uma alternativa simples: tabela no D1 (ex. rate_limit_hits: chave
   IP+rota, timestamp) checada antes de proxiar. Limites: 10 req/min por IP em
   /auth/login e /auth/register; 30 req/min por usuário autenticado em /search/vibe e /chat
   (use o identificador do usuário se já estiver disponível nesse ponto do código; senão, IP).
   Acima do limite: HTTP 429.

Não faça: não desligue os docs no ambiente local (respeite a env var). Não faça deploy.

Critério de pronto: build do Worker sem erros, CORS restrito por variável, /docs inacessível
por padrão, as 4 rotas sensíveis com limite de taxa implementado.
```

---

## Prompt 3 — Remover infraestrutura legada MySQL/Aiven e código morto

```
Você vai remover a infraestrutura legada de banco MySQL (Aiven) do repositório
lianes-library — não é mais usada em produção, a API usa exclusivamente Cloudflare D1.

Leia primeiro: src/db/connection.py, src/core/config.py. Depois rode:
grep -rn "from src.db.connection import get_engine" src/
para confirmar a lista atual de dependentes (na última auditoria eram:
src/scripts/adicionar_coluna.py, src/scripts/import_csv.py, src/scripts/verificar_capas.py,
src/scripts/atualizar_capas.py, src/scripts/setup_db.py).

Faça:
1. Para cada script encontrado no grep: reescreva para usar src/db/d1_client.py
   (d1_query/d1_one/d1_exec/d1_batch) em vez de SQLAlchemy/MySQL, preservando a função de
   cada um. setup_db.py hoje cria schema — isso já é feito pelas migrations em
   cloudflare/migrations/, então avalie se ele deve virar só um wrapper fino que roda
   `wrangler d1 migrations apply`, ou ser removido se não tiver mais utilidade real.
2. Delete src/db/connection.py.
3. Remova DATABASE_URL, DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME de src/core/config.py e
   as linhas correspondentes do .env (só as linhas — não precisa versionar segredo nenhum).
4. Remova sqlalchemy/pymysql (ou equivalente) de requirements.txt e requirements-api.txt se
   não forem mais usados por nada.
5. Atualize o README.md removendo o MySQL/Aiven do stack atual (pode ficar como nota
   histórica, mas deixe claro que D1 é a única fonte de dados hoje).

Não faça: não tente revogar a credencial da Aiven — isso é ação manual minha no painel deles,
fora do seu alcance. Não apague o .env inteiro, só as linhas do MySQL.

Critério de pronto: grep confirma zero referências a get_engine/SQLAlchemy/pymysql/
DATABASE_URL no repositório; os scripts em src/scripts/ continuam compilando
(python -m py_compile); o FastAPI sobe normalmente.
```

---

## Prompt 4 — Medir uso por conta nas chamadas pagas de IA/e-mail

```
Você vai instrumentar contagem de uso por usuário (owner_id) nas chamadas a APIs pagas de
terceiros no repositório lianes-library — só medição, sem cobrança/bloqueio ainda (isso é o
Prompt 8, separado).

Leia primeiro: cloudflare/migrations/ (para seguir o padrão de migration), src/db/d1_client.py,
src/ai/vectorstore.py, src/ai/agents/librarian.py, cloudflare/src/email.ts,
cloudflare/src/wishlist.ts, cloudflare/src/events.ts.

Faça:
1. Nova migration em cloudflare/migrations/ (próximo número sequencial) criando
   usage_events: usage_event_id INTEGER PK AUTOINCREMENT, owner_id INTEGER NOT NULL
   REFERENCES users(user_id), event_type TEXT NOT NULL
   (valores: 'vibe_search','chat','email_send','google_books_lookup','ticketmaster_lookup'),
   created_at TEXT NOT NULL DEFAULT (datetime('now')). Índice em
   (owner_id, event_type, created_at).
2. Backend Python: crie src/db/crud/usage.py com record_usage(owner_id: int, event_type: str)
   -> None (INSERT simples; envolva em try/except e apenas logue erro, não pode derrubar a
   requisição principal). Chame em vibe_search() (src/ai/vectorstore.py, 'vibe_search') e em
   chat_with_librarian() (src/ai/agents/librarian.py, 'chat'), logo após a chamada externa
   ter sucesso.
3. Worker (cloudflare/src/): registre uso também para envios via Resend (email.ts,
   'email_send'), consultas ao Google Books (wishlist.ts, 'google_books_lookup') e
   Ticketmaster (events.ts, 'ticketmaster_lookup'), inserindo direto em usage_events via
   env.DB (o Worker já tem binding direto ao D1).
4. Adicione GET /analytics/usage (novo router ou dentro de analytics.py) que devolve, para o
   owner_id autenticado, a contagem de eventos por tipo nos últimos 30 dias.

Não faça: nenhuma lógica de limite/bloqueio ainda. Não faça deploy.

Critério de pronto: migration aplica localmente sem erro; os 5 pontos de chamada registram
evento; GET /analytics/usage retorna dados corretos para um usuário de teste.
```

---

## Prompt 5 — Migrar autenticação para Clerk (backend: Worker + FastAPI)

```
Você vai substituir o sistema de autenticação próprio (JWT emitido por nós + bcrypt) por
Clerk (clerk.com) no backend do repositório lianes-library. Este prompt cobre só backend
(Worker + FastAPI) — não mexa em web/, isso é o próximo prompt.

Contexto que você não precisa re-descobrir: toda rota autenticada depende de
get_current_user_id (src/api/deps.py), que hoje decodifica um JWT nosso (src/core/security.py,
HS256, segredo JWT_SECRET) e devolve o user_id interno (inteiro, PK da tabela users no D1).
Cadastro/login vivem em src/api/routers/auth.py com bcrypt. Mantenha o mesmo contrato de
retorno de get_current_user_id (um int) para não precisar tocar nos outros routers — só a
fonte da verificação muda.

Leia primeiro: src/api/deps.py, src/core/security.py, src/api/routers/auth.py,
src/db/crud/users.py, src/schemas/user.py, cloudflare/src/index.ts (a função fetch, onde as
env vars são passadas ao container), cloudflare/wrangler.jsonc.

Faça:
1. Nova migration em cloudflare/migrations/ adicionando clerk_user_id TEXT UNIQUE à tabela
   users. Mantenha password_hash por enquanto (não escreva mais nela, mas não apague a
   coluna); comente no arquivo da migration que está deprecated pós-Clerk.
2. Em src/db/crud/users.py: adicione get_user_by_clerk_id(clerk_user_id) e
   get_or_create_user_by_clerk(clerk_user_id, email, first_name, last_name) — esta última:
   (a) tenta achar por clerk_user_id; (b) se não achar, tenta achar por email (linka contas
   que já existiam antes do Clerk, preservando histórico) e atualiza o clerk_user_id; (c) se
   não achar nenhuma, cria uma linha nova.
3. Escolha como verificar o token do Clerk no Python: prefira o SDK oficial do Clerk para
   Python se houver um pacote bem mantido no PyPI (confirme o nome atual antes de instalar);
   senão, verifique manualmente via PyJWT contra o JWKS do Clerk (cacheie as chaves, não
   busque a cada requisição). A config necessária (CLERK_SECRET_KEY e/ou CLERK_JWKS_URL) vem
   de variável de ambiente.
4. Reescreva get_current_user_id (src/api/deps.py): extrai o Bearer token, verifica como
   token do Clerk (passo 3), extrai claims (clerk user id, email, nome), chama
   get_or_create_user_by_clerk(...), devolve o user_id interno (int) — mesma assinatura de
   hoje.
5. Remova (ou esvazie) /auth/register e /auth/login em src/api/routers/auth.py — Clerk cuida
   disso no frontend agora. Mantenha /auth/me (já funciona com o novo get_current_user_id).
6. Remova hash_password/verify_password/create_access_token/decode_access_token de
   src/core/security.py se não forem mais usados em lugar nenhum (confirme com grep antes);
   remova JWT_SECRET dali.
7. Em cloudflare/src/index.ts e wrangler.jsonc: troque a env var injetada no container de
   JWT_SECRET para a escolhida no passo 3, como secret do Worker (documente
   `wrangler secret put` no README, sem valor real no wrangler.jsonc).
8. Crie um endpoint de webhook no Worker (ex. POST /__webhooks/clerk) que verifica a
   assinatura do webhook do Clerk (Clerk usa Svix — confirme a biblioteca/método atual) e, por
   enquanto, só loga o tipo de evento recebido (user.created, user.deleted, etc.) — a reação a
   user.deleted fica para o Prompt 7 (exclusão de conta em cascata).

Não faça: não mexa em nada dentro de web/. Não apague a coluna password_hash. Não faça
deploy nem configure o webhook de verdade no painel do Clerk.

Critério de pronto: API sobe localmente; uma requisição com token de Clerk válido (de teste)
autentica e cria/vincula a linha em users; nenhum router além de auth.py foi alterado.
```

---

## Prompt 6 — Migrar autenticação para Clerk (frontend)

```
Você vai trocar a autenticação do frontend (web/) do repositório lianes-library para Clerk
(@clerk/clerk-react), substituindo o fluxo caseiro que guarda token em localStorage.
Pré-requisito: backend já migrado para verificar tokens do Clerk (Prompt 5) — não repita esse
trabalho aqui, e não mexa em src/ nem cloudflare/.

Leia primeiro: web/src/lib/AuthContext.tsx, web/src/lib/api.ts, web/src/pages/Login.tsx,
web/src/App.tsx, web/index.html, web/package.json.

Faça:
1. Instale @clerk/clerk-react em web/. A chave publicável vem de
   import.meta.env.VITE_CLERK_PUBLISHABLE_KEY (adicione a um web/.env.example; não coloque
   chave real no repo).
2. Envolva a aplicação com <ClerkProvider> (web/src/main.tsx ou App.tsx, o que fizer mais
   sentido na estrutura atual), publishableKey vindo da env var acima.
3. Substitua web/src/pages/Login.tsx pelos componentes do Clerk (<SignIn/>/<SignUp/> ou a API
   atual da versão instalada), habilitando login social — Google e Apple são configurados no
   painel do Clerk, não no código; confirme na documentação da versão instalada se falta algo
   no lado do componente. Reaproveite o quanto der da identidade visual atual (Card, cores do
   tema) via appearance prop, sem brigar com o que o Clerk já resolve.
4. Reescreva web/src/lib/AuthContext.tsx: remova login/register/setSession/clearSession
   baseados em localStorage; use useAuth()/useUser() do Clerk como fonte de verdade. Se outras
   partes do app esperam o formato atual de useAuth() (ex. user.first_name), crie um adaptador
   fino por cima de useUser() para minimizar mudanças no resto do app.
5. Em web/src/lib/api.ts: troque request() para obter o token via Clerk (getToken(), chamado
   antes de cada chamada) em vez de localStorage.getItem(TOKEN_KEY). Escolha a forma mais
   simples de acessar o token fora de componentes React dado como o Clerk expõe isso na versão
   instalada. Remova as funções de auth baseadas em localStorage.
6. Em web/src/App.tsx: troque o if (!user) return <Navigate to="/login" /> manual pelos
   componentes/hooks do Clerk (<SignedIn>/<SignedOut> ou isSignedIn), mantendo o mesmo
   comportamento (não autenticado cai em /login).
7. Remova telas/campos de registro caseiro (senha custom etc.) que não fazem mais sentido com
   o Clerk.

Não faça: não mexa em src/ nem cloudflare/. Não faça deploy.

Critério de pronto: npm run build em web/ conclui sem erro de tipo; o login via componentes
do Clerk renderiza; grep confirma zero referências a "lianes_token"/"lianes_user" no código.
```

---

## Prompt 7 — Exclusão de conta em cascata

```
Você vai implementar exclusão de conta em cascata no repositório lianes-library (fecha um
gap de LGPD: hoje não existe forma de apagar uma conta e os dados associados). Pré-requisito:
Prompts 5 e 6 (Clerk) já aplicados.

Leia primeiro: todas as migrations em cloudflare/migrations/ (para relembrar o schema
completo e quais tabelas têm owner_id), src/db/d1_client.py (d1_batch), o endpoint de webhook
do Clerk criado no Prompt 5 (POST /__webhooks/clerk, em cloudflare/src/index.ts ou arquivo
correlato).

Faça:
1. Nova migration em cloudflare/migrations/: primeiro confira se existem linhas com
   owner_id NULL em books/borrowers/transactions (query de verificação); trate essas linhas
   (associe a um usuário ou remova, conforme fizer sentido) e então aplique NOT NULL nas três
   colunas. No D1/SQLite isso normalmente exige recriar a tabela (CREATE TABLE nova +
   INSERT SELECT + DROP + RENAME, preservando dados e índices) — siga esse padrão.
2. Crie src/db/crud/account.py com delete_user_cascade(owner_id: int) que apaga, numa única
   transação (d1_batch), todas as linhas do usuário nesta ordem: journal_entries,
   reading_log, borrow_records, notifications, wishlist_price_snapshots (via join com
   wishlist_items), wishlist_items, recommendation_cache, recommendation_dismissals,
   user_event_preferences, transactions, borrowers, books, e por fim users. Não dependa de
   ON DELETE CASCADE — delete explicitamente tabela por tabela.
3. Exponha DELETE /users/me (endpoint autenticado, mesmo padrão Depends(get_current_user_id)
   dos outros routers) que chama delete_user_cascade(owner_id) e também chama a API do Clerk
   para apagar o usuário lá (o mesmo SDK/método usado no Prompt 5). Se a chamada ao Clerk
   falhar, garanta mesmo assim que os dados no D1 foram apagados, e logue o erro.
4. No handler de webhook do Clerk (Prompt 5): implemente a reação a user.deleted — ache o
   user_id interno pelo clerk_user_id e rode a mesma lógica de cascade. Evite duplicar a
   lista de tabelas em dois lugares — extraia a lógica para uma função compartilhada se dois
   pontos realmente precisarem dela (ex. reimplementada em D1 direto via env.DB.batch() no
   Worker, já que ele tem acesso direto). Isso cobre o caso do usuário apagar a conta direto
   pelo painel do Clerk, não só pelo nosso app.
5. Frontend (web/): adicione uma seção "Conta" com botão "Excluir minha conta", com
   confirmação clara (ex. digitar o e-mail ou modal de confirmação), que chama DELETE
   /users/me e desloga (Clerk signOut()).

Não faça: não implemente exportação de dados aqui (fora de escopo). Não faça deploy.

Critério de pronto: migration aplica localmente sem erro; delete_user_cascade testado contra
um usuário com dados em várias tabelas não deixa nenhuma linha órfã (query de verificação);
endpoint e botão funcionam ponta a ponta em ambiente local.
```

---

## Prompt 8 — Cobrança com Stripe

```
Você vai integrar cobrança via Stripe (free tier da própria Stripe, sem custo fixo) no
repositório lianes-library, usando a medição de uso por owner_id do Prompt 4 (tabela
usage_events). Pré-requisito: Prompts 4, 5 e 6 já aplicados.

Leia primeiro: cloudflare/src/index.ts, cloudflare/src/email.ts (padrão de "integração
externa opcional, degrada graciosamente" a seguir), cloudflare/migrations/ (inclusive a
migration de usage_events do Prompt 4), cloudflare/wrangler.jsonc.

Faça:
1. Nova migration: tabela subscriptions (owner_id INTEGER PRIMARY KEY REFERENCES
   users(user_id), stripe_customer_id TEXT, stripe_subscription_id TEXT, plan TEXT NOT NULL
   DEFAULT 'free' CHECK(plan IN ('free','pro')), status TEXT NOT NULL DEFAULT 'active',
   current_period_end TEXT, updated_at TEXT NOT NULL DEFAULT (datetime('now'))).
2. Defina os limites do plano free (ex.: 20 buscas semânticas/mês, 20 mensagens de chat/mês —
   ajuste para algo razoável) num único lugar (ex. cloudflare/src/plans.ts), usando a
   contagem de usage_events dos últimos 30 dias. Não espalhe esses números pelo código.
3. Crie cloudflare/src/stripe.ts com: (a) criação de Stripe Checkout Session para o plano
   "pro" (chave via env.STRIPE_SECRET_KEY, secret do Worker); (b) POST /billing/checkout
   (autenticado) que devolve a URL da Checkout Session; (c) POST /webhooks/stripe que
   verifica a assinatura (STRIPE_WEBHOOK_SECRET) e trata pelo menos checkout.session.completed
   (grava/atualiza subscriptions com plan='pro'), customer.subscription.updated e
   customer.subscription.deleted (volta para plan='free' em cancelamento/falha de pagamento).
4. Antes de processar /search/vibe ou /chat: verifique subscriptions.plan e a contagem de uso
   do mês; se o usuário está no free e passou do limite, devolva HTTP 402 com mensagem clara
   em vez de processar — implemente essa checagem no Worker, antes de proxiar para o
   container (mais barato que checar só depois de chegar no container).
5. Frontend (web/): página "Assinatura" mostrando plano atual e uso do mês (consumindo GET
   /analytics/usage do Prompt 4), com botão "Fazer upgrade" que chama POST /billing/checkout
   e redireciona para a URL retornada.

Não faça: não implemente planos além de free/pro. Não crie produtos/preços reais no painel
da Stripe nem faça deploy — vou criar o produto "Pro" na Stripe e te passar o price_id
depois; deixe-o como env var STRIPE_PRICE_ID_PRO.

Critério de pronto: migration aplica sem erro; com chave de teste da Stripe, o fluxo de
checkout ponta a ponta funciona localmente; a checagem de limite do plano free bloqueia
corretamente após o limite configurado.
```
