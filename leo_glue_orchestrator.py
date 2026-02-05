import os
import time
import boto3
from botocore.exceptions import ClientError

glue = boto3.client("glue")

BRONZE_TO_SILVER_JOB = os.environ["BRONZE_TO_SILVER_JOB"]
SILVER_TO_GOLD_JOB = os.environ["SILVER_TO_GOLD_JOB"]

POLL_SECONDS = 20
TIMEOUT_SECONDS = 60 * 60  # 60 minutes safety
ACTIVE_STATES = {"STARTING", "RUNNING", "STOPPING"}

def wait_for_job(job_name: str, job_run_id: str) -> str:
    start = time.time()
    while True:
        resp = glue.get_job_run(JobName=job_name, RunId=job_run_id, PredecessorsIncluded=False)
        state = resp["JobRun"]["JobRunState"]

        if state in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"):
            return state

        if time.time() - start > TIMEOUT_SECONDS:
            raise TimeoutError(f"{job_name} run {job_run_id} exceeded timeout")

        time.sleep(POLL_SECONDS)

def has_active_run(job_name: str) -> bool:
    runs = glue.get_job_runs(JobName=job_name, MaxResults=10).get("JobRuns", [])
    return any(r.get("JobRunState") in ACTIVE_STATES for r in runs)

def start_job_with_retry(job_name: str, max_wait_seconds: int = 15 * 60) -> str:
    start = time.time()
    while True:
        # If Glue says job is busy, just wait
        if has_active_run(job_name):
            if time.time() - start > max_wait_seconds:
                raise TimeoutError(f"Job {job_name} stayed active too long")
            time.sleep(POLL_SECONDS)
            continue

        try:
            return glue.start_job_run(JobName=job_name)["JobRunId"]
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code == "ConcurrentRunsExceededException":
                # Another run started between our check and the start call
                time.sleep(POLL_SECONDS)
                continue
            raise

def lambda_handler(event, context):
    run1 = start_job_with_retry(BRONZE_TO_SILVER_JOB)
    state1 = wait_for_job(BRONZE_TO_SILVER_JOB, run1)
    if state1 != "SUCCEEDED":
        raise Exception(f"{BRONZE_TO_SILVER_JOB} failed with state={state1}")

    run2 = start_job_with_retry(SILVER_TO_GOLD_JOB)
    state2 = wait_for_job(SILVER_TO_GOLD_JOB, run2)
    if state2 != "SUCCEEDED":
        raise Exception(f"{SILVER_TO_GOLD_JOB} failed with state={state2}")

    return {"status": "OK", "bronze_to_silver_run": run1, "silver_to_gold_run": run2}
