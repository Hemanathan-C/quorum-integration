"""Push messages to q1 and q2 via raw pika, each with a payload and dynamic delay.

Topology is declared with declare_topology() (single source of truth); the
messages themselves are produced with pika (see pika_producer). Delays no single
queue covers are composed automatically by the worker on each hop.
"""

import time

from celery_app import declare_topology
from pika_producer import connect, publish_delayed

# (task, label, destination queue, delay seconds, payload)
MESSAGES = [
    ("tasks.task_1", "order-confirm", "q1", 164, {"order_id": 1001, "amount": 49.9}),
    ("tasks.task_2", "welcome-email", "q2", 144, {"to": "user@example.com"}),
    ("tasks.task_1", "sms-otp",       "q1", 0,  {"phone": "+15550001", "code": "8421"}),
    # ("tasks.task_2", "push-reminder", "q2", 20, {"device": "abc", "text": "Don't forget!"}),
    # ("tasks.task_1", "retry-webhook", "q1", 15, {"url": "https://hook.test/x", "attempt": 2}),
]

if __name__ == "__main__":
    declare_topology()
    conn = connect()
    channel = conn.channel()
    print(f"[{time.strftime('%H:%M:%S')}] pushing {len(MESSAGES)} messages via pika")
    for task, label, queue, delay, payload in MESSAGES:
        task_id, first_hop = publish_delayed(
            channel, label=label, payload=payload, queue=queue,
            delay_seconds=delay, task_name=task,
        )
        hop = f"delay.{first_hop}s" if first_hop else queue
        print(f"  -> {task.split('.')[-1]:<7} {label:<14} queue={queue} "
              f"delay={delay:>2}s first_hop={hop} id={task_id}")
    conn.close()
    print("Sent. Worker (-Q q1,q2) will execute each at its target time.")
