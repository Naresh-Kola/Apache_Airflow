# Apache Airflow — Complete Guide

## What is Apache Airflow?

Apache Airflow is an **open-source workflow orchestration platform** used to programmatically author, schedule, and monitor data pipelines.

- Created at **Airbnb in 2014** by Maxime Beauchemin
- Donated to the **Apache Software Foundation in 2016**
- Became a **top-level Apache project in 2019**
- Written entirely in **Python**
- Workflows are defined as **DAGs (Directed Acyclic Graphs)**

> In simple terms: Airflow is the **control tower** for your data pipelines — it decides *what* runs, *when* it runs, *in what order*, and *what happens if it fails*.

---

## Why Do We Need Airflow? (The Problem)

Modern data pipelines are not simple. A typical pipeline might look like:

1. Extract data from 3 different APIs at 6 AM
2. Validate and clean the raw data
3. Transform and join with warehouse data
4. Run data quality checks
5. Load into production tables
6. Trigger downstream ML model training
7. Send a Slack alert when done

Managing this with **cron jobs + shell scripts** causes:

- ❌ No visibility into what's running or failed
- ❌ No retry logic when a step fails at 3 AM
- ❌ No dependency management between steps
- ❌ No way to backfill historical data
- ❌ Silent failures that corrupt downstream data
- ❌ Impossible to scale across teams

**Airflow solves all of these.**

---

## What Problems Does Airflow Solve?

| Problem | Without Airflow | With Airflow |
|---|---|---|
| **Task Dependencies** | Manual, fragile scripts | DAG defines dependencies explicitly |
| **Scheduling** | Cron jobs, hard to manage | Flexible scheduler with complex intervals |
| **Failure Handling** | Silent failures | Automatic retries, alerts, failure hooks |
| **Monitoring** | No visibility | Rich UI with logs, status, run history |
| **Backfilling** | Painful manual re-runs | One-click backfill for any date range |
| **Scalability** | Hard to parallelize | Distributed execution across workers |
| **Auditability** | No history | Full run history and task logs |
| **Reusability** | Copy-paste scripts | Modular operators and reusable DAGs |

---

## Core Concepts

### DAG (Directed Acyclic Graph)
A workflow defined as a Python file. Each **node** is a task, each **edge** is a dependency.

```
extract → transform → validate → load → notify
```

- **Directed** — tasks flow in one direction
- **Acyclic** — no circular dependencies (no infinite loops)

### Operator
Defines what a task *does*. Airflow has 600+ built-in operators:

| Operator | Purpose |
|---|---|
| `PythonOperator` | Run a Python function |
| `BashOperator` | Run a shell command |
| `PostgresOperator` | Run a SQL query |
| `S3ToRedshiftOperator` | Move data from S3 to Redshift |
| `HttpOperator` | Call a REST API |
| `EmailOperator` | Send an email |
| `DockerOperator` | Run a Docker container |

### Scheduler
The brain of Airflow — continuously scans DAG files and triggers task instances based on schedule and dependencies.

### Executor
Determines *how* tasks are run:

| Executor | Best For |
|---|---|
| `SequentialExecutor` | Local dev/testing only |
| `LocalExecutor` | Small teams, single machine |
| `CeleryExecutor` | Large scale, multiple workers |
| `KubernetesExecutor` | Cloud-native, dynamic scaling |

### Web UI
A dashboard to monitor, trigger, debug, and manage all DAGs and task runs.

### XCom (Cross Communication)
Mechanism for tasks to share small pieces of data with each other.

---

## Simple DAG Example

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define task functions
def extract():
    print("Extracting data from API...")

def transform():
    print("Cleaning and transforming data...")

def load():
    print("Loading data to warehouse...")

# Define the DAG
with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",        # Run every day
    catchup=False,            # Don't backfill old runs
    tags=["etl", "example"]
) as dag:

    t1 = PythonOperator(task_id="extract",   python_callable=extract)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="load",      python_callable=load)

    # Set execution order
    t1 >> t2 >> t3
```

---

## Key Features of Apache Airflow

### 1. Dynamic Pipeline Generation
Pipelines are defined in Python — you can use loops, conditionals, and functions to generate DAGs dynamically.

### 2. Rich Web UI
- Visual DAG graph view
- Gantt charts for performance analysis
- Task log viewer
- Manual trigger and backfill buttons
- Variable and connection manager

### 3. Extensible with Providers
600+ pre-built integrations (providers) for:
- Cloud: AWS, GCP, Azure
- Databases: Postgres, MySQL, BigQuery, Snowflake
- Tools: Spark, dbt, Kubernetes, Docker
- Communication: Slack, Email, PagerDuty

### 4. Backfilling
Re-run pipelines for any historical date range:
```bash
airflow dags backfill --start-date 2024-01-01 --end-date 2024-06-01 my_dag
```

### 5. SLAs and Alerting
Define SLA (Service Level Agreement) deadlines for tasks. Get alerts if a task misses its deadline.

### 6. Connections & Variables
Centrally manage credentials (DB passwords, API keys) and config values — no hardcoding secrets in code.

### 7. Task Retry & Timeout
```python
PythonOperator(
    task_id="my_task",
    retries=3,                         # Retry 3 times on failure
    retry_delay=timedelta(minutes=5),  # Wait 5 mins between retries
    execution_timeout=timedelta(hours=1)
)
```

### 8. Sensors
Special operators that **wait** for a condition to be true before proceeding:
- `FileSensor` — wait for a file to appear
- `SqlSensor` — wait for a SQL query to return rows
- `HttpSensor` — wait for an API to return 200

---

## Advantages of Apache Airflow

| Advantage | Description |
|---|---|
| ✅ **Python-native** | Pipelines are pure Python — flexible and familiar |
| ✅ **Huge ecosystem** | 600+ providers, massive community |
| ✅ **Excellent UI** | Rich, intuitive dashboard for monitoring |
| ✅ **Scalable** | From single machine to thousands of workers |
| ✅ **Open source & free** | No licensing costs |
| ✅ **Backfilling support** | Built-in historical reprocessing |
| ✅ **Battle-tested** | Used by thousands of companies worldwide |
| ✅ **Highly extensible** | Build custom operators, hooks, and plugins |
| ✅ **Strong community** | Active development, frequent releases |
| ✅ **Cloud managed options** | MWAA (AWS), Cloud Composer (GCP), Astro (Astronomer) |

---

## Disadvantages of Apache Airflow

| Disadvantage | Description |
|---|---|
| ❌ **Complex setup** | Requires metadata DB, scheduler, workers, and UI to be running |
| ❌ **Not for streaming** | Designed for batch workloads; poor fit for real-time/streaming data |
| ❌ **DAG parsing overhead** | All DAG files are parsed repeatedly — can slow down with 1000s of DAGs |
| ❌ **Steep learning curve** | Takes time to understand DAGs, executors, and best practices |
| ❌ **No native data lineage** | Doesn't track data assets or column-level lineage out of the box |
| ❌ **Dynamic DAGs are tricky** | Generating DAGs at runtime is possible but complex |
| ❌ **Scheduler is a SPOF** | Traditional scheduler is a single point of failure (improved in Airflow 2.x) |
| ❌ **Heavy for simple tasks** | Overkill for a single cron job |
| ❌ **XCom limitations** | Not suitable for passing large data between tasks |

---

## Airflow vs Alternatives

| Tool | Best For | Key Difference |
|---|---|---|
| **Apache Airflow** | General batch orchestration | Most mature, largest ecosystem |
| **Prefect** | Modern Python workflows | Easier setup, dynamic tasks |
| **Dagster** | Data-asset centric pipelines | Strong typing, built-in lineage |
| **Luigi** | Simple pipelines | Older, simpler, less features |
| **Temporal** | General workflow automation | Not data-specific, more general |
| **AWS Step Functions** | AWS-native workflows | No coding needed, vendor lock-in |
| **dbt** | SQL transformations only | Not a full orchestrator |

---

## When to Use Airflow

### ✅ Good Use Cases
- ETL/ELT data pipelines
- ML model training and retraining pipelines
- Scheduled reporting and aggregations
- Data quality monitoring workflows
- Complex multi-system data integrations
- Batch processing jobs

### ❌ Avoid Airflow When
- You need **real-time/streaming** processing → use Kafka, Flink, Spark Streaming
- You have **only simple cron jobs** → plain cron is fine
- You need **microservice communication** → use message queues (RabbitMQ, SQS)
- You want **zero infrastructure** → consider managed services like AWS Glue

---

## Summary

```
Apache Airflow = Scheduler + Dependency Manager + Monitor + Alerter
                 all in one platform for data pipelines
```

Airflow is the **industry standard** for batch data orchestration. Its combination of Python-based DAGs, a powerful UI, backfilling, retry logic, and a massive provider ecosystem makes it the go-to choice for data engineering teams worldwide — despite its complexity in setup and maintenance.
