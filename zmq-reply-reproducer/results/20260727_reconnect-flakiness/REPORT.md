# Rapport — Série #1 : intermittence des reconnects

**Session** : `20260727T234621Z_seed42_reconnect-flakiness`  
**Date** : 2026-07-27  
**Seed** : 42 (reproductible)  
**Politique broker** : `job-id`  
**Payload RESULT** : ~20 Mo (moyenne 20 971 520 B, jitter ±15 %)

## Commande

```bash
python3 run_scenarios.py --runs 20 --seed 42 \
  --scenarios job_gt_timeout_reconnect_then_ready_ack \
  job_gt_timeout_reconnect_full_immediate \
  job_gt_timeout_reconnect_then_heartbeat \
  --result-payload-bytes 20971520 --result-policy job-id \
  --no-monitor --save-traces --trace-label reconnect-flakiness
```

## Contexte

Après les tests unitaires (1 run par scénario), nous avons observé :

- `no-reconnect + strict` → CASE2 (rejet applicatif)
- `no-reconnect + job-id` → SUCCESS (transport OK)
- `ready-ack + strict` → CASE2 (`worker_not_busy_state=READY`)
- `ready-ack + job-id` → CASE1 intermittent (broker ne reçoit rien)

**Objectif de cette série** : quantifier l'intermittence des stratégies de reconnect avec une politique broker permissive (`job-id`), pour isoler les problèmes de transport.

## Paramètres fixes

| Paramètre | Valeur |
|-----------|--------|
| `HEARTBEAT_INTERVAL` | 0.5 s |
| `WORKER_TIMEOUT` | 1.5 s |
| `JOB_DURATION` | 5.0 s |
| `threading_mode` | `blocking-network-loop` |
| Runs par stratégie | 20 |
| Total runs | 60 |

## Résultats globaux

| Métrique | Valeur |
|----------|--------|
| Succès bout en bout | **20 / 60 (33 %)** |
| CASE1 — send OK, broker ne reçoit rien | **40 / 60 (67 %)** |
| CASE2 — reçu puis rejeté | **0** |
| CASE3 — accepté, ACK perdu | **0** |

Avec `job-id`, **toutes les pertes sont du transport** (CASE1). Aucun rejet applicatif.

## Résultats par stratégie

| Stratégie | Runs | SUCCESS | CASE1 | Taux succès |
|-----------|------|---------|-------|-------------|
| `manual-reconnect-immediate` | 20 | 4 | 16 | **20 %** |
| `manual-reconnect-heartbeat` | 20 | 9 | 11 | **45 %** |
| `manual-reconnect-ready-ack` | 20 | 7 | 13 | **35 %** |

**Classement** : heartbeat > ready-ack > immediate.

## Corrélation READY_ACK (stratégie `ready-ack`)

Analyse des 20 runs `manual-reconnect-ready-ack` :

| Signal post-reconnect | Runs | Résultat |
|-----------------------|------|----------|
| `READY_ACK` reçu (nouvelle session) | 7 | **7 × SUCCESS** |
| `ready_ack_timeout` (3 s) | 13 | **13 × CASE1** |

**Corrélation 100 %** : quand le handshake applicatif post-reconnect réussit, le RESULT est toujours accepté. Quand il échoue, le broker ne reçoit jamais le message.

## Schéma du phénomène

```
Job long (5s) → worker non responsive → EXPIRED à T+1.5s
        ↓
Reconnect FULL (nouvelle socket + session_id)
        ↓
   ┌────┴────┬──────────────┐
   ▼         ▼              ▼
immediate  heartbeat    ready-ack
(envoi sec) (attend HB)  (attend ACK 3s)
   │         │              │
  20%       45%            35%
  SUCCESS   SUCCESS        SUCCESS*
                             *100% si ACK reçu
```

## Conclusions

1. **La politique `job-id` élimine les rejets applicatifs (CASE2)** — le problème résiduel est purement transport/routing ZMQ après expiration.

2. **Le reconnect FULL est intermittent** — entre 55 % et 80 % d'échec selon la stratégie d'attente post-reconnect.

3. **`manual-reconnect-immediate` est la plus fragile** (20 %) : envoyer le RESULT sans attendre que la pipe soit opérationnelle échoue souvent.

4. **`manual-reconnect-heartbeat` est la plus fiable** (45 %) parmi les trois, mais sans garantie forte.

5. **`manual-reconnect-ready-ack` est binaire** : fiable à 100 % quand l'ACK arrive, mais 65 % des runs timeout sur l'ACK (broker ne répond pas au READY post-reconnect).

6. **La taille du payload (~20 Mo) n'est pas le facteur limitant** quand le message arrive : les SUCCESS transfèrent le blob complet sans CASE3.

## Pistes de suite

| # | Test | Hypothèse |
|---|------|-----------|
| 2 | `separate-job-thread` | Sans expiration worker, le reconnect devient inutile |
| 3 | Augmenter timeout READY_ACK (3s → 10s) | Les 13 CASE1 ready-ack deviennent des SUCCESS |
| 4 | Reconnect light vs full | Le reconnect léger change le taux CASE1 |
| 5 | `ZMQ_IMMEDIATE=1` | Le buffer local masque ou aggrave les pertes |

## Fichiers de cette session

| Fichier | Contenu |
|---------|---------|
| `session.json` | Métadonnées et commande exacte |
| `aggregate.json` | Statistiques agrégées |
| `outcomes.json` | Résultat par run (champs essentiels) |
| `REPORT.md` | Ce rapport |

Logs complets et timelines détaillées : voir `traces/20260727T234621Z_seed42_reconnect-flakiness/` en local (`traces/` est gitignored).
