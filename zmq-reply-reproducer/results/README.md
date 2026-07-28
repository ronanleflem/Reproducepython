# Résultats d'investigation

Ce dossier contient les **rapports de synthèse** des campagnes de test exécutées avec le laboratoire ZMQ. Les logs complets restent en local dans `traces/` (gitignored).

Chaque sous-dossier correspond à une session archivée :

| Dossier | Description |
|---------|-------------|
| [20260727_reconnect-flakiness](20260727_reconnect-flakiness/REPORT.md) | Série #1 — intermittence des stratégies de reconnect (60 runs) |
| [20260728_separate-job-thread](20260728_separate-job-thread/REPORT.md) | Série #2 — même campagne avec `separate-job-thread` (60 runs) |

## Rejouer une campagne

Voir la commande dans `session.json` de chaque dossier, ou :

```bash
python3 run_scenarios.py --runs 20 --seed 42 \
  --scenarios job_gt_timeout_reconnect_then_ready_ack \
  job_gt_timeout_reconnect_full_immediate \
  job_gt_timeout_reconnect_then_heartbeat \
  --result-payload-bytes 20971520 --result-policy job-id \
  --save-traces --trace-label reconnect-flakiness
```
