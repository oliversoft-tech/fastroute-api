# fastroute-domain

Single source of truth for FastRoute business rules, shared across:
- React Native (Android/iOS)
- Backend (Node/Nest)
- Orchestrators (n8n) indirectly via an API that imports this package

This repository implements the rules extracted from your n8n flows:
- Validate JWT (structural + result normalization)
- Add Route (DBSCAN preconditions + clustering params defaults + waypoint generation)
- Finish Route (route status + no pending waypoints)
- Finish Waypoint (allowed transitions + mandatory photo metadata + upload/record preconditions)

## Install

```bash
npm i fastroute-domain
```

## Usage (examples)

```ts
import {
  validateJwtInput,
  dbscanDefaults,
  validateDbscanPoints,
  generateWaypointsForCluster,
  canFinishRoute,
  validateFinishWaypoint
} from "fastroute-domain";
```

## Development

```bash
npm i
npm run test
npm run build
```

## Notes

- This library is **pure**: no network calls, no Supabase SDK calls.
- JWT verification is represented as:
  1) structural validation (token presence)
  2) normalization of a provider response (valid/invalid)

Your backend (or edge function) should perform the actual Supabase `/auth/v1/user` call and then feed the response to these helpers.
