"""Tasks. The delay logic lives in the @delayable decorator (built on
celery_app.due_now_or_reschedule), so each task body is ONLY its real work.

A message carries a payload, a destination queue (q1/q2) and an absolute target
time. The decorator runs the shared gate first: if it isn't time yet, the task
bounces through another delay queue; if it is, the body below runs.
"""

from celery_app import app, delayable, ts


@app.task(bind=True)
@delayable
def task_1(self, label=None, payload=None, target_ts=None, target_queue="q1"):
    # ---- task_1's real work goes here ----
    print(">>>>>>>>>> task_1 <<<<<<<<<<<<<<")
    print(f"[{ts()}] task_1 WORK: label={label!r} payload={payload!r} on {target_queue}")
    return f"task_1 processed label={label!r} payload={payload!r} on {target_queue}"


@app.task(bind=True)
@delayable
def task_2(self, label=None, payload=None, target_ts=None, target_queue="q1"):
    # ---- task_2's real work goes here ----
    print(">>>>>>>>>> task_2 <<<<<<<<<<<<<<")
    print(f"[{ts()}] task_2 WORK: label={label!r} payload={payload!r} on {target_queue}")
    return f"task_2 processed label={label!r} payload={payload!r} on {target_queue}"
