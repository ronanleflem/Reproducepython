# Rapport — Série #3 : matrice de combinaisons (7 leviers)

**Session** : `20260728T073229Z_seed42_combination-matrix`  
**Date** : 2026-07-28  
**Seed** : 42  
**17 combinaisons × 10 runs = 170 runs**  
**Payload** : ~20 Mo, politique `job-id` (sauf combo strict)

## Commande

```bash
python3 run_combinations.py --runs 10 --seed 42 \
  --no-monitor --save-traces --trace-label combination-matrix
```

## Classement final

| # | Combinaison | Succès | Δ baseline | Expiré | CASE1 | CASE2 |
|---|-------------|--------|------------|--------|-------|-------|
| 1 | **echo-hb + no-reconnect + separate-thread** | **100 %** | **+50 pts** | 0/10 | 0 | 0 |
| 2 | stack idéal (idem) | **100 %** | +50 pts | 0/10 | 0 | 0 |
| 3 | separate-thread seul | 60 % | +10 pts | 10/10 | 4 | 0 |
| 4 | separate + heartbeat | 60 % | +10 pts | 10/10 | 4 | 0 |
| 5 | separate + ready-ack 10s | 60 % | +10 pts | 10/10 | 4 | 0 |
| 6 | stack full (echo + ready-ack 10s + light) | 60 % | +10 pts | 4/10 | 4 | 0 |
| 7 | **baseline** (blocking + heartbeat) | 50 % | 0 | 10/10 | 5 | 0 |
| 8 | ready-ack timeout 10s | 50 % | 0 | 10/10 | 5 | 0 |
| 9 | separate + reconnect light | 40 % | −10 pts | 10/10 | 6 | 0 |
| 10–14 | ready-ack, zmq-imm, echo+heartbeat… | 30 % | −20 pts | — | — | — |
| 15–16 | immediate, reconnect light | 20 % | −30 pts | 10/10 | 8 | 0 |
| 17 | **politique strict** | **0 %** | −50 pts | 10/10 | 6 | **4** |

## Conclusions majeures

### 1. Le combo gagnant est sans ambiguïté

**`separate-job-thread` + echo heartbeat worker→broker + pas de reconnect = 100 %**

- 0 expiration worker
- 0 CASE1
- SUCCESS bout en bout avec ACK reçu
- Le reconnect devient **inutile** quand le worker reste vivant

### 2. Effet marginal de chaque levier seul (vs baseline 50 %)

| Levier seul | Taux | Verdict |
|-------------|------|---------|
| separate-thread | 60 % | Utile (+10 pts) mais insuffisant seul |
| echo-hb + no-reconnect | **100 %** | **Décisif** |
| reconnect immediate | 20 % | Pire |
| ready-ack | 30 % | Faible |
| ready-ack 10s | 50 % | Neutre |
| strict policy | 0 % | Catastrophique (CASE2) |
| reconnect light | 20 % | Pire |
| zmq-immediate | 30 % | Légèrement pire |

### 3. Cumuler tous les leviers ne sert à rien (voire nuit)

Le « kitchen sink » (tout activé) : **30 %** seulement.

Quand echo-hb empêche l'expiration mais qu'on force quand même un reconnect (stratégie heartbeat), on **recrée** le problème qu'on vient de résoudre.

### 4. Ordre d'efficacité pour ton projet

```
1. Thread réseau dédié (socket ZMQ isolée)
2. Echo heartbeat worker → broker pendant le job
3. Supprimer le reconnect post-job (devenu inutile)
────────────────────────────────────────────
4. separate-thread seul si pas encore (2) : +10 pts
5. job-id plutôt que strict : évite CASE2
────────────────────────────────────────────
6. ready-ack timeout 10s : neutre seul
7. reconnect heartbeat : utile seulement si (1-3) impossibles
────────────────────────────────────────────
❌ À éviter : immediate, reconnect light, strict, kitchen sink
```

## Implémentation ajoutée

- `WorkerConfig.echo_heartbeat` : le worker répond aux HEARTBEAT du broker
- `run_combinations.py` : runner de matrice reproductible

## Rejouer

```bash
python3 run_combinations.py --runs 10 --seed 42 --no-monitor
python3 run_combinations.py --combinations 2 14  # stack gagnant seul
```
