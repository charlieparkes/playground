1. `fetch.py` - Create sqlite database of every outcome document ID
2. `rehash.py` - Send `UPDATE_OUTCOME_HASH` messages for all documents. Don't resend any that have `Outcome.queued == True`

The second step, rehash, will check at the end that there are no outcomes left to be queued.
```python
Outcome.select(Outcome.doc_id).where(Outcome.queued==False).count()
```

Migrations are for my use only. If you have a fresh database, don't use them.
