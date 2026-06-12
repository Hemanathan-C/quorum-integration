"""Celery app: delayed task execution via RabbitMQ DLX + per-queue TTL.

Any message (with a payload) can be scheduled to ANY main queue (q1, q2) with a
dynamic delay. There is ONE delay queue per delay value, SHARED by all targets;
the destination is chosen per-message (message-level), not pinned on the queue.

How the per-message destination works
-------------------------------------
The dead-letter EXCHANGE is set at the queue level (x-dead-letter-exchange), but
NO x-dead-letter-routing-key is set. So an expired message keeps its OWN routing
key, and that key (q1 / q2) is what the DLX uses to deliver it.

A message's routing key normally also selects the queue it enters -- so to free
the key to carry the target, each delay queue has a FANOUT input exchange. We
publish to the fanout with routing_key = target; fanout ignores the key for
routing-in, but the key stays on the message for the eventual dead-letter.

  publish(exchange="delay.in.5s", routing_key="q2")
        |  fanout -> delay.5s   (ignores key)
        v
  delay.5s  TTL=5s, x-dead-letter-exchange="delay.dlx", (no dlx routing key)
        |  expires -> keeps routing key "q2"
        v
  delay.dlx (direct)  "q1"->q1, "q2"->q2  --> worker runs the task

Broker: amqp://guest:guest@localhost:5672//
"""

import functools
import time

from celery import Celery
from kombu import Exchange, Queue

BROKER_URL = "amqp://guest:guest@localhost:5672//"

# Main processing queues that tasks can be routed to.
MAIN_QUEUES = ["q1", "q2"]

# Supported delay values, in SECONDS. Add/remove values here to change the set.
DELAYS = [5, 10, 15, 20, 30, 120]

app = Celery(
    "celery_delay_queue",
    broker=BROKER_URL,
    backend="rpc://",
    include=["tasks"],
)

# Direct exchange that delivers expired messages to a main queue by routing key.
dlx = Exchange("delay.dlx", type="direct", durable=True)

# Main queues: bound to the DLX by their own name, so expired messages land here.
MAIN_QUEUE_OBJS = [
    Queue(name, exchange=dlx, routing_key=name, durable=True) for name in MAIN_QUEUES
]


def delay_in_exchange(seconds: int) -> Exchange:
    """Fanout input exchange for the `seconds` delay queue (one per delay value)."""
    return Exchange(f"delay.in.{seconds}s", type="fanout", durable=True)


def _delay_queue(seconds: int) -> Queue:
    """Single shared delay queue for `seconds`; dead-letters via the message's key."""
    return Queue(
        f"delay.{seconds}s",
        exchange=delay_in_exchange(seconds),
        routing_key="",                                  # fanout ignores the key
        durable=True,
        queue_arguments={
            "x-message-ttl": seconds * 1000,             # TTL in milliseconds
            "x-dead-letter-exchange": dlx.name,          # set at QUEUE level
            # NO x-dead-letter-routing-key: keep the message's own key (the target)
        },
    )


DELAY_QUEUE_OBJS = [_delay_queue(d) for d in DELAYS]

# The worker consumes only the main queues. Delay queues MUST stay consumer-less
# (declared via declare_topology) so their TTL can elapse before delivery.
app.conf.task_queues = MAIN_QUEUE_OBJS
app.conf.task_default_queue = "q1"
app.conf.task_default_exchange = "delay.dlx"
app.conf.task_default_routing_key = "q1"

app.conf.task_serializer = "json"
app.conf.result_serializer = "json"
app.conf.accept_content = ["json"]
app.conf.timezone = "UTC"


def declare_topology():
    """(Re)create exchanges, delay queues and main queues on the broker.

    Delete-then-declare the delay queues so changing their arguments (e.g. an old
    x-dead-letter-routing-key) can't trip a PRECONDITION_FAILED on redeclare.
    """
    with app.connection() as conn:
        for q in DELAY_QUEUE_OBJS:
            ch = conn.channel()
            try:
                q.bind(ch).delete()        # fresh channel: a delete error won't cascade
            except Exception:
                pass
            finally:
                ch.close()
        ch = conn.channel()
        for q in DELAY_QUEUE_OBJS + MAIN_QUEUE_OBJS:
            q.bind(ch).declare()           # declares exchange + queue + binding
        ch.close()


# Tolerance so a target measured at e.g. 9.999s still picks the 10s queue instead
# of splitting into smaller hops. Worst case fires up to this many seconds late.
FIT_TOLERANCE = 0.5


def best_fit_delay(remaining: float) -> int:
    """Pick the delay-queue TTL to use for `remaining` seconds of delay.

    Selection rule (never fires early, granularity rounds up to 5s):
      * remaining >= smallest queue    -> largest queue TTL that fits (no overshoot)
      * 0 < remaining < smallest queue -> one final smallest-queue hop
      * remaining <= 0                 -> 0, meaning "run now"
    """
    if remaining <= 0:
        return 0
    fits = [d for d in DELAYS if d <= remaining + FIT_TOLERANCE]
    return max(fits) if fits else min(DELAYS)


def route_to_target(signature, target_ts: float, queue: str):
    """Send `signature` toward `target_ts`: best-fit delay queue, or `queue` if due.

    The target queue rides in the message's routing key so the DLX can deliver it.
    """
    if queue not in MAIN_QUEUES:
        raise ValueError(f"unknown queue {queue!r}; choose from {MAIN_QUEUES}")
    remaining = target_ts - time.time()
    d = best_fit_delay(remaining)
    if d == 0:
        # Due now: publish straight to the main queue via the DLX.
        return signature.apply_async(exchange=dlx.name, routing_key=queue)
    # Not due: into the shared delay queue, carrying the target in the routing key.
    return signature.apply_async(exchange=f"delay.in.{d}s", routing_key=queue)


def ts(epoch: float = None) -> str:
    """HH:MM:SS for an epoch time (or now) -- shared for trace logging."""
    return time.strftime("%H:%M:%S", time.localtime(epoch))


def due_now_or_reschedule(task, *, target_ts, target_queue, resend) -> bool:
    """Shared delay gate: 'is it finally time, or do I bounce myself onward?'

    Any task can call this. It:
      * traces the pickup + delay check,
      * returns True if the target time has arrived (caller does its real work),
      * otherwise re-queues `resend()` (a fresh signature of the calling task,
        carrying the SAME target_ts/target_queue) onto the best-fit delay queue
        and returns False.

    `task`   : the bound task instance (`self`), used for request metadata.
    `resend` : a zero-arg callable returning the signature to re-enqueue.
    """
    now = time.time()
    remaining = 0 if target_ts is None else target_ts - now
    delivery = getattr(task.request, "delivery_info", {}) or {}
    arrived_via = delivery.get("routing_key", "?")

    print(f"[{ts()}] >>>>>>> {task.name} PICKED UP  id={task.request.id}  "
          f"arrived_rk={arrived_via!r}  target_queue={target_queue}")
    if target_ts is None:
        print(f"[{ts()}] DELAY CHECK: no target_ts -> due now")
    else:
        print(f"[{ts()}] DELAY CHECK: now={ts(now)} target={ts(target_ts)} "
              f"remaining={remaining:.2f}s")

    if remaining <= 0:
        print(f"[{ts()}] === DUE -> executing {task.name} ===")
        return True

    d = best_fit_delay(remaining)
    print(f"[{ts()}] NOT DUE: {remaining:.2f}s left -> re-queue via delay.{d}s "
          f"(dead-letters back to {target_queue})")
    route_to_target(resend(), target_ts, target_queue)
    return False


def delayable(fn):
    """Decorator: run the shared delay gate before the task body.

    Apply it UNDER @app.task(bind=True). It reads target_ts/target_queue from the
    task's kwargs and runs due_now_or_reschedule; if not due it re-queues the same
    call and returns, if due it runs the wrapped body.

        @app.task(bind=True)
        @delayable
        def task_1(self, label=None, payload=None, target_ts=None, target_queue="q1"):
            ...  # only the real work

    The task must accept `target_ts` and `target_queue` kwargs (schedule()/the pika
    producer set them); `self.s(*args, **kwargs)` rebuilds the call for the next hop.
    """
    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        if not due_now_or_reschedule(
            self,
            target_ts=kwargs.get("target_ts"),
            target_queue=kwargs.get("target_queue", "q1"),
            resend=lambda: self.s(*args, **kwargs),
        ):
            return f"requeued label={kwargs.get('label')!r}"
        return fn(self, *args, **kwargs)

    return wrapper


def schedule(signature, queue: str, delay_seconds: float):
    """Run `signature` on `queue` after ~`delay_seconds`, composing delay queues.

    Stamps the destination queue and an absolute target timestamp into the task's
    kwargs, then kicks off the first hop. The task re-checks the clock on every
    hop and re-queues itself until the target time is reached (see tasks.task_1).
    """
    target_ts = time.time() + delay_seconds
    signature.kwargs["target_ts"] = target_ts
    signature.kwargs["target_queue"] = queue
    return route_to_target(signature, target_ts, queue)


if __name__ == "__main__":
    app.start()
