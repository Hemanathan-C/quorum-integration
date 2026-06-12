"""Produce delayed tasks using raw pika (AMQP) instead of Celery's producer.

Any AMQP client can inject work as long as it speaks Celery's message protocol
(v2). This module builds such a message and publishes the FIRST hop:
  * if already due  -> straight to the main queue via delay.dlx
  * otherwise       -> the best-fit fanout delay exchange, target in the routing key

The worker (Celery) consumes q1/q2 and, on each hop, re-queues via Celery's own
producer (see celery_app.due_now_or_reschedule). So: external producer = pika,
internal rescheduling = Celery.
"""

import json
import time
import uuid
from urllib.parse import unquote, urlparse

import pika

from celery_app import BROKER_URL, DELAYS, MAIN_QUEUES, best_fit_delay, dlx

EMBED = {"callbacks": None, "errbacks": None, "chain": None, "chord": None}


def _pika_params():
    """Pika ConnectionParameters from BROKER_URL (Celery's trailing // = vhost '/')."""
    u = urlparse(BROKER_URL)
    vhost = unquote(u.path[1:]) or "/"
    creds = pika.PlainCredentials(u.username or "guest", u.password or "guest")
    return pika.ConnectionParameters(
        host=u.hostname or "localhost", port=u.port or 5672,
        virtual_host=vhost, credentials=creds,
    )


def connect():
    """Open a blocking pika connection to the broker."""
    return pika.BlockingConnection(_pika_params())


def _celery_message(task_name, kwargs):
    """Build a Celery protocol-v2 (body, properties) pair for a kwargs-only call."""
    task_id = str(uuid.uuid4())
    body = json.dumps([[], kwargs, EMBED])              # (args, kwargs, embed)
    props = pika.BasicProperties(
        content_type="application/json",
        content_encoding="utf-8",
        correlation_id=task_id,
        delivery_mode=2,                                # persistent
        headers={
            "lang": "py",
            "task": task_name,
            "id": task_id,
            "root_id": task_id,
            "parent_id": None,
            "group": None,
            "argsrepr": "()",
            "kwargsrepr": repr(kwargs),
            "origin": "pika-producer",
            "retries": 0,
            "eta": None,
            "expires": None,
        },
    )
    return task_id, body, props


def publish_now(channel, *, label, payload, queue, task_name="tasks.task_1"):
    """Publish straight to a main queue with NO delay. Returns task_id.

    Sends via the DLX exchange to `queue` with target_ts=None, so the worker's
    delay gate treats it as due immediately and runs the body on first pickup.
    """
    if queue not in MAIN_QUEUES:
        raise ValueError(f"unknown queue {queue!r}; choose from {MAIN_QUEUES}")
    kwargs = {"label": label, "payload": payload,
              "target_ts": None, "target_queue": queue}
    task_id, body, props = _celery_message(task_name, kwargs)
    channel.basic_publish(
        exchange=dlx.name, routing_key=queue, body=body, properties=props,
    )
    return task_id


def publish_delayed(channel, *, label, payload, queue, delay_seconds,
                    task_name="tasks.task_1"):
    """Publish the first hop of a delayed task via pika. Returns (task_id, first_hop)."""
    if queue not in MAIN_QUEUES:
        raise ValueError(f"unknown queue {queue!r}; choose from {MAIN_QUEUES}")

    target_ts = time.time() + delay_seconds
    kwargs = {"label": label, "payload": payload,
              "target_ts": target_ts, "target_queue": queue}
    task_id, body, props = _celery_message(task_name, kwargs)

    d = best_fit_delay(target_ts - time.time())
    if d == 0:
        exchange, routing_key = dlx.name, queue          # due now: straight to main queue
    else:
        exchange, routing_key = f"delay.in.{d}s", queue  # delay queue; target in the key

    channel.basic_publish(
        exchange=exchange, routing_key=routing_key, body=body, properties=props,
    )
    return task_id, d
