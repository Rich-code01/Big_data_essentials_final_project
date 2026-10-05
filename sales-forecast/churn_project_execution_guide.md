# Churn Prediction Big Data Pipeline — Execution Guide

## Architecture Overview

```
customer_data.csv (local)
        |
        v
   HDFS (Hadoop)  ---->  train_model.py (MLlib RandomForest) ---->  Model saved to HDFS
        |
        v
Kafka Producer (hdfs_kafka_producer.py)
        |
        v
   Kafka Topic: churn-events
        |
        v
PySpark Structured Streaming (churn_streaming.py)
   - Loads trained MLlib model from HDFS
   - Consumes Kafka stream
   - Predicts churn per micro-batch
        |
        v
   MySQL (churn_db.churn_predictions)
        |
        v
   Django Dashboard (visualizes predictions) [in progress]
```

## Environment Details

| Component | Location / Version |
|---|---|
| Hadoop | 3.3.6, installed at `~/bigdata/hadoop-3.3.6` |
| Kafka | 2.13-3.6.1, installed at `~/bigdata/kafka` |
| Spark | 3.5.1 (Hadoop3 build), installed at `~/bigdata/spark` |
| Python venv | `~/projects/churn_prediction_project/venv` (pandas 2.2.2, numpy 1.26.4, pyspark 3.5.1, kafka-python 2.0.2, mysql-connector-python 8.4.0, django 5.0.6) |
| MySQL | 8.0.46, database `churn_db`, table `churn_predictions` |
| Dataset | `customer_data.csv`, 25M rows, 1.3GB, uploaded to `hdfs:///user/rimwe/churn/data/customer_data.csv` |
| Model | RandomForestClassifier via MLlib Pipeline, saved to `hdfs:///user/rimwe/churn/models/churn_rf_model` |

---

## One-Time Setup (already completed — for reference in report)

1. Downloaded and extracted Hadoop, Kafka, Spark tarballs to `~/bigdata/`
2. Configured environment variables in `~/.bashrc` (`JAVA_HOME`, `HADOOP_HOME`, `KAFKA_HOME`, `SPARK_HOME`)
3. Configured Hadoop pseudo-distributed mode (`core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`, `yarn-site.xml`) and enabled passwordless SSH to localhost
4. Formatted the NameNode (`hdfs namenode -format`)
5. Created Python virtual environment and installed pinned dependencies
6. Generated the 25M-row synthetic churn dataset (`generate_dataset.py`)
7. Uploaded dataset to HDFS
8. Trained the MLlib RandomForest model (`train_model.py`) — achieved AUC 0.9999, Accuracy 0.997, F1 0.997
9. Set up MySQL database, table, and dedicated `churn_user` account
10. Built the corrected Kafka producer and PySpark structured streaming consumer with MySQL write-back

---

## Startup Sequence (run every time you demo/test the project)

Open **4 separate terminal windows/tabs**, all in the same WSL Ubuntu-22.04 session.

### Terminal 1 — Hadoop HDFS
```bash
start-dfs.sh
jps
```
Confirm you see: `NameNode`, `DataNode`, `SecondaryNameNode`

### Terminal 2 — Zookeeper (leave running)
```bash
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
```

### Terminal 3 — Kafka Broker (leave running)
```bash
kafka-server-start.sh $KAFKA_HOME/config/server.properties
```

### Verify all daemons (any terminal)
```bash
jps
```
Should show: `NameNode`, `DataNode`, `SecondaryNameNode`, `QuorumPeerMain`, `Kafka`

### MySQL
```bash
sudo service mysql start
mysql -u churn_user -pchurnpass123 churn_db -e "SHOW TABLES;"
```

---

## Running the Pipeline

### Terminal 4 — Start the PySpark Structured Streaming job
```bash
cd ~/projects/churn_prediction_project
source venv/bin/activate
spark-submit \
  --master local[*] \
  --driver-memory 4g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark/churn_streaming.py
```
Wait for `Model loaded from: ...` in the output before proceeding.

### Terminal 5 — Run the Kafka Producer
```bash
cd ~/projects/churn_prediction_project
source venv/bin/activate
python3 kafka/hdfs_kafka_producer.py
```
This reads `customer_data.csv` from HDFS and streams records into the `churn-events` Kafka topic. Let it run, or stop early with `Ctrl+C` — the streaming job continues consuming whatever has been sent.

### Verify predictions are being written
```bash
mysql -u churn_user -pchurnpass123 churn_db -e "SELECT COUNT(*) FROM churn_predictions;"
mysql -u churn_user -pchurnpass123 churn_db -e "SELECT * FROM churn_predictions ORDER BY id DESC LIMIT 10;"
```

---

## Shutdown Sequence

```bash
# Stop the Spark streaming job (Ctrl+C in Terminal 4)
# Stop the Kafka broker (Ctrl+C in Terminal 3)
# Stop Zookeeper (Ctrl+C in Terminal 2)

stop-dfs.sh
sudo service mysql stop
```

---

## Retraining the Model (if dataset changes)

```bash
cd ~/projects/churn_prediction_project
source venv/bin/activate
spark-submit --master local[*] --driver-memory 4g spark/train_model.py
```

---

## Known Issues Encountered & Resolutions (useful for report's "Challenges" section)

| Issue | Cause | Resolution |
|---|---|---|
| Kafka schema mismatch | Streaming script's schema didn't match actual CSV columns | Rewrote schema to match `generate_dataset.py` output exactly |
| NameNode/DataNode failed to start | Port 9870/9864 held by stale WSL2 network sockets | `wsl --shutdown` from PowerShell to reset WSL2 networking |
| `Failed to find data source: kafka` | Spark doesn't bundle the Kafka connector | Added `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` to `spark-submit` |
| MySQL root access denied | `auth_socket` plugin requires `sudo mysql`, not password login | Used `sudo mysql` for admin tasks; created dedicated `churn_user` with `mysql_native_password` |
| OutOfMemoryError during training | Default Spark local-mode driver heap too small for 25M rows | Added `--driver-memory 4g`/`6g` to `spark-submit` |
| `BlockMissingException` reading model from HDFS | DataNode process had stopped between sessions | Restarted with `start-dfs.sh` before running the streaming job |

---

## Still To Do

- [ ] Build Django dashboard (models.py, views, templates) to visualize `churn_predictions` table
- [ ] Testing and deployment (Step 7 of course guidelines)
- [ ] Final report writeup with architecture diagram, findings, and version documentation
