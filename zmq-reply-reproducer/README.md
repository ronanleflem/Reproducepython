# zmq-reply-reproducer

Projet Python minimal pour **reproduire et diagnostiquer** une perte intermittente de reply ZeroMQ après un job long.

Simule uniquement l'architecture **Broker ROUTER ↔ Worker DEALER** : le broker envoie un job, le worker dépasse la fenêtre de liveness, le broker retire le worker de sa liste, puis le worker tente d'envoyer son `RESULT`.

## Installation

```bash
cd zmq-reply-reproducer
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Python 3.10 ou 3.11 recommandé.

## Configuration par défaut (accélérée)

| Paramètre | Valeur | Effet |
|-----------|--------|-------|
| `HEARTBEAT_INTERVAL` | 0.5 s | Cadence des heartbeats broker |
| `LIVENESS_MULTIPLIER` | 3 | Multiplicateur de liveness |
| `WORKER_TIMEOUT` | 1.5 s | Expiration worker (`interval × multiplier`) |
| `JOB_DURATION` | 5.0 s | Job plus long que la liveness → expiration |

### Scénario production

```bash
python3 broker.py --production
python3 worker.py --production --strategy no-reconnect
```

Valeurs : heartbeat 5 s, multiplier 12 (timeout 60 s), job 540 s.

## Lancement manuel

**Terminal 1 — Broker**

```bash
python3 broker.py --endpoint tcp://127.0.0.1:5555 --log-level INFO
```

**Terminal 2 — Worker**

```bash
python3 worker.py \
  --broker tcp://127.0.0.1:5555 \
  --strategy manual-reconnect-ready-ack \
  --reconnect-mode full \
  --threading-mode blocking-network-loop \
  --log-level INFO
```

### Stratégies worker (`--strategy`)

| Stratégie | Comportement après le job long |
|-----------|-------------------------------|
| `no-reconnect` | Envoie `RESULT` sur la socket existante |
| `manual-reconnect-immediate` | `reconnect_socket()` puis `RESULT` immédiat |
| `manual-reconnect-heartbeat` | Reconnect, attend le 1er `HEARTBEAT`, puis `RESULT` |
| `manual-reconnect-ready-ack` | Reconnect complet, `READY` + attend `READY_ACK`, puis `RESULT` |
| `auto-reconnect-only` | Aucun reconnect manuel, ZMQ gère seul |
| `reconnect-then-delay` | Reconnect + délai (`--reconnect-delay`) avant `RESULT` |

### Modes de reconnect (`--reconnect-mode`)

- **`light`** : `disconnect()` + `connect()` sur la même socket (session inchangée)
- **`full`** : fermeture, nouvelle socket, nouvelles options, nouvelle `session_id`, `socket_generation++`

### Modes de threading (`--threading-mode`)

- **`blocking-network-loop`** : le `sleep` du job bloque la boucle réseau → aucun heartbeat traité pendant le job
- **`separate-job-thread`** : thread réseau dédiée ; le job tourne dans une autre thread via `queue.Queue` (la thread job ne touche jamais la socket ZMQ)

### Politique de résultat broker (`--result-policy`)

- **`strict`** (défaut) : refuse si worker absent, session différente, ou état ≠ `BUSY`
- **`job-id`** : accepte si le `job_id` est connu et non terminé, même après reconnexion

### Options ZeroMQ configurables

```bash
--zmq-immediate 0|1
--zmq-linger <ms>
--zmq-reconnect-ivl <ms>
--zmq-reconnect-ivl-max <ms>
--zmq-sndtimeo <ms>    # -1 = bloquant
--send-mode blocking|dontwait   # côté worker
```

Toutes les valeurs effectives sont loguées au démarrage (`ZMQ_OPTIONS`).

## Scénarios automatiques

```bash
# 20 exécutions par scénario (défaut)
python3 run_scenarios.py

# 50 runs avec jitter ±20 % sur durées/délais
python3 run_scenarios.py --runs 50 --jitter 0.2

# Politique job-id
python3 run_scenarios.py --runs 20 --result-policy job-id

# Sous-ensemble de scénarios
python3 run_scenarios.py --runs 20 --scenarios job_gt_timeout_no_reconnect job_gt_timeout_reconnect_then_ready_ack

# Logs détaillés
python3 run_scenarios.py --runs 5 --verbose
```

### Scénarios inclus

1. `job_lt_timeout` — job court (< timeout), baseline
2. `job_gt_timeout_no_reconnect`
3. `job_gt_timeout_reconnect_light_immediate`
4. `job_gt_timeout_reconnect_full_immediate`
5. `job_gt_timeout_reconnect_then_heartbeat`
6. `job_gt_timeout_reconnect_then_ready_ack`
7. `job_gt_timeout_auto_reconnect`
8. `job_gt_timeout_reconnect_then_delay`

### Tableau de synthèse

```
strategy                                       runs  expired  send_ok  raw_rx  accepted  rejected  missing  success%
job_gt_timeout_no_reconnect                      20       20       20      20         0        20         0      0.0%
...
```

| Colonne | Signification |
|---------|---------------|
| `expired` | Le broker a expiré le worker pendant le job |
| `send_ok` | `send_multipart()` a réussi côté worker |
| `raw_rx` | Le broker a reçu physiquement un message `RESULT` (niveau transport) |
| `accepted` | Le broker a accepté et envoyé `RESULT_ACK` |
| `rejected` | Message reçu mais rejeté (policy / état / session) |
| `missing` | Aucun `RESULT` reçu par le broker |

## Structure des logs

### Niveau transport (`TRANSPORT_RECV`)

Logué **immédiatement** après chaque `recv_multipart()`, avant tout parsing :

```
TRANSPORT_RECV component=broker mono_ts=... wall_ts=... routing_id=776f... frame_count=2 frames=['RESULT', '{...}']
```

### Niveau applicatif (`APP`)

```
APP result_received ... worker_known=False worker_state=N/A ... policy=strict
APP result_rejected ... reason=worker_not_in_registry
APP result_accepted job_id=...
APP worker_expired ... last_heartbeat_age=1.502s timeout=1.500s
```

### Envoi (`SEND_RESULT`)

Chaque `send_multipart()` est tracé avec durée, `socket_generation`, `session_id`, succès/erreur/`errno`. **Un envoi réussi ne prouve pas la réception.**

### Socket monitor (`SOCKET_MONITOR`)

Thread séparée, événements ZMTP :

`EVENT_CONNECTED`, `EVENT_CONNECT_DELAYED`, `EVENT_CONNECT_RETRIED`, `EVENT_DISCONNECTED`, `EVENT_ACCEPTED`, `EVENT_HANDSHAKE_SUCCEEDED`, `EVENT_HANDSHAKE_FAILED`, `EVENT_CLOSED`, …

### Heartbeats bufferisés

Chaque `HEARTBEAT` broker porte un `heartbeat_counter` et un `session_id`. Le worker logue la classification :

- `current_session` — correspond à la session active
- `old_session` — heartbeat d'une session précédente (potentiellement bufferisé)
- `unknown_session` — session inconnue

Après reconnect complet, seul un `READY_ACK` contenant le **nouveau** `session_id` valide la session.

## Interprétation : `send()` réussi mais broker ne reçoit rien

1. **Route ZMQ supprimée** : le broker a retiré le worker ; la pipe TCP peut subsister mais le ROUTER ne route plus vers cette identité de façon fiable.
2. **Reconnect léger vs complet** : `disconnect/connect` peut réutiliser une pipe half-open ; une nouvelle socket peut créer une nouvelle route.
3. **File d'attente locale** : `send()` met le message dans le buffer sortant ZMQ ; sans peer actif (`ZMQ_IMMEDIATE=1`), l'envoi peut échouer ; avec `IMMEDIATE=0`, il peut réussir localement sans livraison.
4. **Concurrence auto-reconnect / manuel** : double reconnexion, race sur la pipe.
5. **Messages bufferisés** : anciens `HEARTBEAT` livrés après reconnect ≠ signal de session valide.

## Handshake ZMTP vs heartbeat vs READY/READY_ACK

| Couche | Rôle |
|--------|------|
| **ZMTP** (`EVENT_HANDSHAKE_SUCCEEDED`) | Connexion TCP + négociation protocole ZMQ |
| **Heartbeat applicatif** (`HEARTBEAT`) | Keepalive logique broker → worker ; peut être bufferisé ; **ne restaure pas** une session applicative |
| **READY / READY_ACK** | Handshake applicatif : enregistrement explicite du worker, `session_id`, état `READY` |

Un reconnect ZMTP réussi **ne garantit pas** que le broker considère le worker comme `BUSY` pour le job en cours.

## Machine d'état broker

```
READY ──(job)──► BUSY ──(liveness expirée)──► EXPIRED (retiré du registre)
  ▲                │
  └──(RESULT accepté)┘
```

## Fichiers

```
zmq-reply-reproducer/
├── broker.py           # Broker ROUTER
├── worker.py           # Worker DEALER + stratégies
├── protocol.py         # Messages multipart
├── config.py           # Constantes et dataclasses
├── logging_setup.py    # Logging structuré
├── socket_monitor.py   # Monitor ZMQ en thread
├── run_scenarios.py    # Batterie de tests
├── requirements.txt
└── README.md
```

## Questions diagnostiques couvertes

1. La suppression du worker entraîne-t-elle la disparition de la route ZMQ ?
2. Un reply peut-il être envoyé avec succès sans jamais être reçu ?
3. Le reconnect léger détruit-il ou remplace-t-il la pipe ?
4. Le premier heartbeat après reconnect est-il un mauvais signal ?
5. `READY_ACK` est-il un meilleur signal de session restaurée ?
6. Le broker reçoit-il le reply mais le rejette (worker plus `BUSY`) ?
7. `ZMQ_IMMEDIATE` modifie-t-il le comportement ?
8. Reconnexion manuelle en concurrence avec l'auto-reconnect ?
9. Intermittence avec jitter (race conditions) ?
10. La politique `job-id` + `RESULT/RESULT_ACK` corrige-t-elle le problème ?
