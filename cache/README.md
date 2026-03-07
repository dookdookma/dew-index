# DEW Retention and Rollup Jobs

Cron-friendly commands:

- Daily snapshot touch (or replace with real writer job):
  - `python scripts/rollup_and_prune.py daily-touch`
- Weekly rollup:
  - `python scripts/rollup_and_prune.py weekly`
- Monthly rollup:
  - `python scripts/rollup_and_prune.py monthly`
- Prune expired covered artifacts:
  - `python scripts/rollup_and_prune.py prune`
- All-in-one:
  - `python scripts/rollup_and_prune.py all`

Suggested scheduler:
- 23:55 daily: daily-touch (or your real snapshot writer)
- Sunday 00:10: weekly
- 1st of month 00:20: monthly
- Daily 00:40: prune
