# Celery delayed tasks via RabbitMQ DLX + TTL

Schedule any message — with a **payload**, a **destination queue** (`q1` or `q2`),
and a **dynamic delay** — using RabbitMQ's Dead-Letter-Exchange + per-queue TTL.
No plugins, no `countdown` polling.

## Key design points
- **One delay queue per delay value** (`delay.5s`, `delay.10s`, `delay.15s`,
  `delay.20s`, `delay.30s`), **shared** by every destination — not duplicated per
  target queue.
- **Destination chosen per message.** The delay queue sets `x-dead-letter-exchange`
  at the *queue* level but **no** `x-dead-letter-routing-key`, so an expired
  message keeps its **own** routing key (`q1`/`q2`), and the DLX routes on that.
- **Fanout input exchange per delay** (`delay.in.5s` → `delay.5s`). Publishing via
  fanout lets the routing key carry the *target* (instead of selecting the queue),
  so it survives onto the dead-lettered message.
- **Arbitrary delays are composed.** 35s → `delay.30s` then `delay.5s`. The task
  re-checks the clock on each hop and only runs when it is truly due.

## Flow
```
schedule(task_1.s(payload=...), queue="q2", delay_seconds=35)
        │  publish(exchange="delay.in.30s", routing_key="q2")
        ▼
   delay.30s   TTL 30s, DLX=delay.dlx (no dlx routing key)   ── no consumer
        │  TTL expires → kept routing key "q2"
        ▼
   delay.dlx (direct)  "q2" → q2
        ▼
       q2  ──►  worker: 5s still left → re-queue via delay.5s ──► … → EXECUTE
```

## Files
- `celery_app.py` — app, exchanges, queues, `declare_topology()`,
  `best_fit_delay()`, `route_to_target()`, `schedule()`
- `tasks.py` — self-rescheduling `task_1` task (payload + target + target time)
- `produce.py` — pushes sample messages to q1/q2 with varied payloads & delays

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run
1. Start the worker. **Consume only the main queues** — the delay queues must
   stay consumer-less or their messages would deliver immediately instead of
   waiting for the TTL:
   ```bash
   celery -A celery_app worker --loglevel=INFO -Q q1,q2
   ```

2. In another terminal (same venv), declare the topology and send the messages:
   ```bash
   python produce.py
   ```

You'll see each message execute on its destination queue at its target time.

## Scheduling your own message
```python
from celery_app import declare_topology, schedule
from tasks import task_1

declare_topology()  # once, to ensure exchanges/queues exist

# run on q2, ~15s from now, with a payload
schedule(task_1.s(label="job", payload={"x": 1}), queue="q2", delay_seconds=15)
```

## Tuning
- **Delay values:** edit `DELAYS` in `celery_app.py`.
- **Destination queues:** edit `MAIN_QUEUES`.
- **Timing behavior:** `FIT_TOLERANCE` (default 0.5s) lets a near-boundary target
  pick the larger queue instead of splitting into extra hops; raising it trades a
  little more potential lateness for fewer hops. The scheme never fires *early*;
  non-multiples of 5 round **up** to the next 5s.

> Changing a delay value alters that queue's `x-message-ttl`. `declare_topology()`
> deletes and recreates the delay queues, so it handles the otherwise-fatal
> "redeclare with different arguments" error for you.
