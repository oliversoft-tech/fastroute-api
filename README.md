# FastRoute API (Node.js + Supabase) 

API gerada a partir da collection Postman `FastRouteApp.postman_collection.json`, usando
`@oliverbill/fastroute-domain` para executar regras de negócio.

## Setup

```bash 
npm install
cp .env.example .env
npm run dev
```

## CI Scripts

```bash
npm run lint
npm run typecheck
npm test
npm run build
```

## Deploy Secrets (GitHub Actions)

Configure os secrets abaixo no repositório:

- `GHCR_USERNAME`: usuário dono do pacote no GHCR (ex.: `oliverbill`)
- `GHCR_PAT`: token para o VPS fazer pull do GHCR (`read:packages` e `repo` se repo privado)
- `VPS_HOST`
- `VPS_USER`
- `VPS_SSH_KEY`

## Endpoints (collection)

- `POST /login`
- `GET /route`
- `POST /route/import` (multipart, campo `file`)
- `PATCH /route/start`
- `PATCH /route/finish`
- `PATCH /waypoint/finish` (multipart, campo `image_base64`)
- `GET /waypoint/photo`
- `PATCH /waypoint/reorder`

## Endpoints auxiliares

- `POST /sync/push`
- `POST /sync/pull`
- `GET /health`
