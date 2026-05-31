from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

import sys
sys.path.insert(0, "/opt/airflow")

default_args = {
    "owner":             "data-engineering",
    "depends_on_past":   False,
    "email_on_failure":  False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "sla":               timedelta(minutes=30),
}

dag = DAG(
    dag_id="fraud_detection_pipeline",
    default_args=default_args,
    description="Fraud detection: Kafka ingestion → GE validation → dbt scoring",
    schedule_interval="*/15 * * * *",  # every 15 minutes
    start_date=days_ago(1),
    catchup=False,
    tags=["fraud", "kafka", "great-expectations", "reliability"],
    max_active_runs=1,
)


def produce_transactions(**context):
    import uuid, random
    from producer.transaction_producer import generate_batch, produce_to_kafka

    merchant_ids = [str(uuid.uuid4()) for _ in range(20)]
    customer_ids = [str(uuid.uuid4()) for _ in range(100)]

    bad_data_pct = round(random.uniform(0.0, 0.10), 4)

    batch = generate_batch(
        merchant_ids, customer_ids,
        n=200, fraud_pct=0.08, bad_data_pct=bad_data_pct
    )
    result = produce_to_kafka(batch)

    context["ti"].xcom_push(key="produced_count", value=result["sent"])
    context["ti"].xcom_push(key="bad_data_pct", value=bad_data_pct)
    print(f"Produced: {result} | bad_data_pct={bad_data_pct:.1%}")


def consume_and_validate(**context):
    from consumer.transaction_consumer import consume_and_process

    result = consume_and_process(batch_size=100, timeout_ms=15_000)

    context["ti"].xcom_push(key="written_count",  value=result["total_written"])
    context["ti"].xcom_push(key="failure_rate",   value=result["overall_failure_rate"])
    context["ti"].xcom_push(key="pipeline_healthy", value=result["healthy"])

    print(f"Consumer result: {result}")


def check_pipeline_health(**context):
    failure_rate = context["ti"].xcom_pull(
        key="failure_rate", task_ids="consume_validate"
    ) or 0.0

    if failure_rate > 0.05:
        print(f"DQ ALERT: failure_rate={failure_rate:.1%} — above 5% threshold, routing to alert")
        return "trigger_dq_alert"
    print(f"DQ OK: failure_rate={failure_rate:.1%} — below 5% threshold, proceeding to dbt")
    return "run_dbt"


def trigger_dq_alert(**context):
    import os, urllib.request, json as _json

    failure_rate = context["ti"].xcom_pull(
        key="failure_rate", task_ids="consume_validate"
    ) or 0.0
    written = context["ti"].xcom_pull(
        key="written_count", task_ids="consume_validate"
    ) or 0
    bad_data_pct = context["ti"].xcom_pull(
        key="bad_data_pct", task_ids="produce_transactions"
    ) or 0.0
    run_id = context["run_id"]

    msg = (
        f"\n{'='*50}\n"
        f"DQ ALERT — fraud_detection_pipeline\n"
        f"Run:          {run_id}\n"
        f"Bad data pct: {bad_data_pct:.1%}  (produced)\n"
        f"Failure rate: {failure_rate:.1%}  (threshold: 5%)\n"
        f"Valid rows written: {written}\n"
        f"Check dead_letter/ for rejected rows\n"
        f"{'='*50}"
    )
    print(msg)

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if webhook_url:
        payload = _json.dumps({
            "text": (
                f":rotating_light: *DQ ALERT — fraud_detection_pipeline*\n"
                f"*Run:* `{run_id}`\n"
                f"*Bad data produced:* {bad_data_pct:.1%}\n"
                f"*Validation failure rate:* {failure_rate:.1%} (threshold: 5%)\n"
                f"*Valid rows written:* {written}\n"
                f"Check `dead_letter/` for rejected rows."
            )
        }).encode("utf-8")
        try:
            req = urllib.request.Request(
                webhook_url,
                data=payload,
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                print(f"Slack notification sent (HTTP {resp.status})")
        except Exception as e:
            print(f"Slack notification failed (non-fatal): {e}")
    else:
        print("SLACK_WEBHOOK_URL not set — skipping Slack notification")
    return "run_dbt"


def replay_dead_letter(**context):
    from consumer.dead_letter_replay import replay_dead_letter_files
    result = replay_dead_letter_files()
    context["ti"].xcom_push(key="replay_result", value=result)
    print(
        f"Dead letter replay — "
        f"files: {result['files_processed']} | "
        f"recovered: {result['records_recovered']} | "
        f"still invalid: {result['records_still_invalid']}"
    )


produce_task = PythonOperator(
    task_id="produce_transactions",
    python_callable=produce_transactions,
    dag=dag,
)

consume_task = PythonOperator(
    task_id="consume_validate",
    python_callable=consume_and_validate,
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)

health_check_task = BranchPythonOperator(
    task_id="health_check",
    python_callable=check_pipeline_health,
    dag=dag,
)

alert_task = PythonOperator(
    task_id="trigger_dq_alert",
    python_callable=trigger_dq_alert,
    dag=dag,
)

run_dbt_task = BashOperator(
    task_id="run_dbt",
    bash_command=(
        "cd /opt/airflow/dbt_project && "
        "dbt run --profiles-dir /opt/airflow/dbt_project --target prod"
    ),
    trigger_rule="none_failed_min_one_success",
    dag=dag,
)

dbt_tests_task = BashOperator(
    task_id="dbt_tests",
    bash_command=(
        "cd /opt/airflow/dbt_project && "
        "dbt test --profiles-dir /opt/airflow/dbt_project --target prod"
    ),
    dag=dag,
)

replay_task = PythonOperator(
    task_id="replay_dead_letter",
    python_callable=replay_dead_letter,
    trigger_rule="all_done",  # always runs, even if dbt failed
    dag=dag,
)

produce_task >> consume_task >> health_check_task
health_check_task >> [alert_task, run_dbt_task]
alert_task >> run_dbt_task >> dbt_tests_task >> replay_task
