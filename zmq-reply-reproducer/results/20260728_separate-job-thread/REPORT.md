# Rapport — Série #2 : `separate-job-thread`

**Session** : `20260728T062024Z_seed42_separate-job-thread`  
**Date** : 2026-07-28  
**Seed** : 42 (identique à la Série #1)  
**Politique broker** : `job-id`  
**Payload RESULT** : ~20 Mo (moyenne 20 971 520 B, jitter ±15 %)

## Commande

```bash
python3 run_scenarios.py --runs 20 --seed 42 \
  --scenarios job_gt_timeout_reconnect_then_ready_ack \
  job_gt_timeout_reconnect_full_immediate \
  job_gt_timeout_reconnect_then_heartbeat \
  --result-payload-bytes 20971520 --result-policy job-id \
  --threading-mode separate-job-thread \
  --no-monitor --save-traces --trace-label separate-job-thread
```

## Hypothèse testée

> Avec `separate-job-thread`, le worker reste joignable pendant le job (thread réseau dédiée) → pas d'expiration → le reconnect devient inutile.

## Paramètres (vs Série #1)

| Paramètre | Série #1 | Série #2 |
|-----------|----------|----------|
| `threading_mode` | `blocking-network-loop` | **`separate-job-thread`** |
| Autres | identiques | identiques |

## Résultats globaux

| Métrique | Série #1 | Série #2 | Δ |
|----------|----------|----------|---|
| Broker accepte le RESULT | 20 / 60 (33 %) | **27 / 60 (45 %)** | **+12 pts** |
| CASE1 — send OK, broker ne reçoit rien | 40 (67 %) | 33 (55 %) | −12 pts |
| CASE2 — reçu puis rejeté | 0 | 0 | = |
| CASE3 — accepté, ACK worker non reçu | 0 | 27 (45 %) | nouveau |
| Worker expiré | 60 / 60 | **60 / 60** | = |

**Note CASE3** : en mode `separate-job-thread`, le thread principal ne reste pas en attente du `RESULT_ACK` (contrairement au mode blocking). Le broker accepte le RESULT mais le worker ne confirme pas la réception de l'ACK — ce n'est pas une perte transport côté broker.

## Résultats par stratégie

| Stratégie | Série #1 | Série #2 | Δ |
|-----------|----------|----------|---|
| `manual-reconnect-immediate` | 4/20 (20 %) | **7/20 (35 %)** | +15 pts |
| `manual-reconnect-heartbeat` | 9/20 (45 %) | **13/20 (65 %)** | +20 pts |
| `manual-reconnect-ready-ack` | 7/20 (35 %) | 7/20 (35 %) | = |

**Classement inchangé** : heartbeat > ready-ack = immediate.

## Verdict sur l'hypothèse

**Hypothèse partiellement infirmée.**

1. **Le worker expire toujours** (60/60 runs) : le broker mesure la vivacité via `last_heartbeat`, mis à jour uniquement quand le broker **reçoit** un HEARTBEAT du worker. Or le worker ne renvoie pas de heartbeat au broker — il se contente de recevoir ceux du broker. Le fait que la thread réseau reste active pendant le job ne change donc pas le timeout de vivacité.

2. **Le reconnect reste nécessaire** : tous les scénarios testés incluent un reconnect post-job, et l'intermittence persiste.

3. **Amélioration malgré tout** : la thread réseau dédiée améliore les taux de succès (+12 pts global), probablement parce que le reconnect et l'envoi du RESULT s'exécutent sur une thread qui n'a jamais bloqué la socket ZMQ pendant le job.

## Observation READY_ACK

En Série #1, la corrélation READY_ACK → SUCCESS était de 100 % (7/7). En Série #2, les logs montrent des `READY_ACK` reçus côté worker, mais le flag `session_validated_ready_ack` n'est pas propagé dans les stats (limitation du mode `separate-job-thread`). Les 7 succès ready-ack correspondent aux runs où le broker a bien reçu le RESULT après reconnect.

## Schéma comparatif

```
                    Série #1 (blocking)     Série #2 (separate-job-thread)
                    ───────────────────     ──────────────────────────────
Worker expiré       60/60                   60/60  ← inchangé
Reconnect requis    oui                     oui
Taux acceptation    33 %                    45 %   ← mieux
CASE1               67 %                    55 %
```

## Pistes de suite (mises à jour)

| # | Test | Statut |
|---|------|--------|
| 2 | `separate-job-thread` | **Fait** — améliore le transport mais n'empêche pas l'expiration |
| 2b | `separate-job-thread` + worker renvoie HEARTBEAT au broker | Empêcherait l'expiration → reconnect inutile |
| 3 | Augmenter timeout READY_ACK (3s → 10s) | À faire |
| 4 | Reconnect light vs full | À faire |
| 5 | `ZMQ_IMMEDIATE=1` | À faire |

## Fichiers de cette session

| Fichier | Contenu |
|---------|---------|
| `session.json` | Métadonnées et commande exacte |
| `aggregate.json` | Statistiques agrégées |
| `outcomes.json` | Résultat par run (champs essentiels) |
| `REPORT.md` | Ce rapport |

Logs complets : `traces/20260728T062024Z_seed42_separate-job-thread/` en local.
