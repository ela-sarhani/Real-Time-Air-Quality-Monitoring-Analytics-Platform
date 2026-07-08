# Real-Time Air Quality Monitoring Platform

A full data engineering pipeline that simulates IoT air-quality sensors and processes streaming data in real time.

```
Python Producer (IoT Simulation)
         ↓
       Kafka
         ↓
  Spark Structured Streaming
         ↓
      PostgreSQL
         ↓
       Grafana
```

---

## Table of Contents

- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Step-by-Step Run Guide](#step-by-step-run-guide)
- [Grafana Queries](#grafana-queries)

---

## Architecture

| Layer | Technology | Role |
|-------|-----------|------|
| **Producer** | Python + `kafka-python` | Simulates IoT sensor readings |
| **Message Broker** | Apache Kafka | Buffers and streams sensor events |
| **Stream Processor** | Apache Spark Structured Streaming | Consumes, transforms, and writes data |
| **Storage** | PostgreSQL | Persists processed sensor records |
| **Visualization** | Grafana | Dashboards and real-time alerts |

---

## ⚙️ Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)
- Python 3.10+

Install the required Python dependency:

```bash
pip install kafka-python
```

---

## Step-by-Step Run Guide

### 1. Start All Containers

From the project root, start all services:

```bash
docker compose up -d
```

Verify all containers are running:

```bash
docker ps
```

---

### 2. Create PostgreSQL Table

Enter the PostgreSQL container:
```bash
docker exec -it airquality-db psql -U spark -d airquality
```


Create the sensor_data table:
```sql
CREATE TABLE sensor_data (
    id SERIAL PRIMARY KEY,
    sensor_id INT,
    pm25 FLOAT,
    no2 FLOAT,
    timestamp TIMESTAMP
);
```
Verify the table:
```bash
\dt
```
Check structure:
```bash
\d sensor_data
```

### 3. Create Kafka Topic

Enter the Kafka container:

```bash
docker exec -it real-time-air-quality-monitoring-analytics-platform_kafka_1 bash
```

Create the `air-quality` topic:

```bash
kafka-topics \
  --create \
  --topic air-quality \
  --bootstrap-server kafka:9092
```

Confirm the topic was created:

```bash
kafka-topics --list --bootstrap-server kafka:29092
```

Consumer launch:
```bash
kafka-console-consumer --topic air-quality --from-beginning --bootstrap-server kafka:9092
```
---

### 4. Run the Spark Streaming Job

Enter the Spark master container:

```bash
docker exec -it spark-master bash
```

Copy the python spark job (stream.py) into the spark environment (from your host):

```bash
docker cp stream.py spark-master:/opt/spark/work-dir/
```

Back to the spark home (inside the spark-master container):

```bash
cd ./../../..
```

Run the namenode container:
```bash
docker exec -it namenode bash
```

Create the necessary folders and give permissions:
```bash
hdfs dfs -mkdir -p /data/airquality
hdfs dfs -mkdir -p /checkpoints/airquality_pipeline
hdfs dfs -chmod -R 777 /data
hdfs dfs -chmod -R 777 /checkpoints 
```

Submit the streaming job:

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3 \
  /opt/spark/work-dir/stream.py
```

---

### 5. Run the Kafka Producer

On your **local machine**, and inside the venv that has kafka-python, start the sensor simulator:

```bash
python producer.py
```

The producer generates and sends JSON messages like:

```json
{
  "sensor_id": 2,
  "pm25": 45.3,
  "no2": 21.7,
  "timestamp": "2026-05-02T16:30:00+00:00"
}
```

| Field | Description |
|-------|------------|
| `sensor_id` | Unique identifier for the IoT sensor |
| `pm25` | Fine particulate matter concentration (µg/m³) |
| `no2` | Nitrogen dioxide concentration (µg/m³) |
| `timestamp` | ISO 8601 UTC timestamp of the reading |

---

### 6. Verify Data in PostgreSQL

Connect to the database:

```bash
docker exec -it airquality-db psql -U spark -d airquality
```

Query the latest sensor records:

```sql
SELECT * FROM sensor_data ORDER BY timestamp DESC;
```

---

### 7. Open Grafana

Navigate to [http://localhost:3000](http://localhost:3000) in your browser.

**Default credentials:**

| Field | Value |
|-------|-------|
| Username | `admin` |
| Password | `admin` |

---

### 8. Add PostgreSQL Data Source in Grafana

Go to **Connections → Add Data Source → PostgreSQL** and fill in the following:

| Field | Value |
|-------|-------|
| Host | `postgres:5432` |
| Database | `airquality` |
| User | `spark` |
| Password | `spark` |

Click **Save & Test** to confirm the connection.

---

## 📊 Grafana Queries

### Live Air Quality Data

Visualize PM2.5 and NO2 readings over time.

```sql
SELECT
  timestamp AS time,
  pm25,
  no2
FROM sensor_data
WHERE $__timeFilter(timestamp)
ORDER BY timestamp
```

---

### Pollution Alerts

Identify readings that exceed safe thresholds (PM2.5 > 50 or NO2 > 70).

```sql
SELECT
  timestamp AS time,
  pm25,
  no2,
  CASE
    WHEN pm25 > 50 AND no2 > 70 THEN 'PM2.5 + NO2 high'
    WHEN pm25 > 50 THEN 'High PM2.5'
    WHEN no2 > 70 THEN 'High NO2'
    ELSE 'Normal'
  END AS alert_cause
FROM sensor_data
WHERE $__timeFilter(timestamp)
  AND (pm25 > 50 OR no2 > 70)
ORDER BY timestamp DESC;
```

---

