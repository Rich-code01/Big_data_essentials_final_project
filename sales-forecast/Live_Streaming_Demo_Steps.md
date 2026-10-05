# Live Streaming Demo Step-by-Step Notes

Use this checklist to verify everything works before your presentation, and
as your script during the live demo itself.

---

## PART A  Prerequisites (do this first, before the demo)

### A1. Start HDFS
New Command Prompt:
```cmd
%HADOOP_HOME%\sbin\start-dfs.cmd
```
✅ Check: two new windows open (NameNode, DataNode) and stay running.
✅ Verify:
```cmd
hdfs dfs -ls /project/sales_data
```
Should list `sales_data.csv` (~1.1 GB).

### A2. Start Kafka
New Command Prompt:
```cmd
cd C:\kafka
bin\windows\kafka-server-start.bat config\kraft\server.properties
```
✅ Check: log ends with `[KafkaRaftServer nodeId=1] Kafka Server started`
✅ Leave this window open don't close it.

### A3. Confirm the topic exists
New Command Prompt (separate window, Kafka still running):
```cmd
cd C:\kafka
bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```
✅ Check: `sales-stream` appears in the list.

**If you want a clean demo with an empty topic** (so old test messages don't
clutter the live view), delete and recreate it:
```cmd
bin\windows\kafka-topics.bat --delete --topic sales-stream --bootstrap-server localhost:9092
bin\windows\kafka-topics.bat --create --topic sales-stream --bootstrap-server localhost:9092
```

---

## PART B Start the Consumer (do this BEFORE the producer)

Open a new terminal (VS Code terminal or Command Prompt):
```cmd
cd C:\projects\sales-forecast
bigdata-env\Scripts\activate
python spark_streaming_consumer.py
```

✅ What you'll see, in order:
1. `Starting Spark session...` (may take ~10-20 sec first time, faster after)
2. `Listening to topic 'sales-stream'...`
3. A box saying "Two live views will print below"
4. `Streaming started. Press Ctrl+C to stop.`
5. Then it goes quiet and waits **this is correct**, it's idle until new
   messages arrive.

**Leave this terminal window visible during your demo** — this is where the
live output will appear.

---

## PART C Start the Producer (this triggers the live demo)

Open a **second, separate** terminal:
```cmd
cd C:\projects\sales-forecast
bigdata-env\Scripts\activate
python kafka_producer.py --rate 20 --limit 500
```

Notes on the flags:
- `--rate 20` = sends 20 rows/second (slow enough to visibly watch, fast
  enough not to bore your audience the whole run takes ~25 seconds)
- `--limit 500` = only streams 500 rows (a manageable demo chunk, not all
  17.5 million)
- Adjust `--rate` up (e.g. `--rate 100`) if you want it to finish faster, or
  down (e.g. `--rate 5`) if you want more time to narrate while it runs

✅ What you'll see in THIS terminal:
```
Starting producer -> topic 'sales-stream' at 20.0 rows/sec
Starting Spark session to read from HDFS...
Dataset loaded from HDFS: 500 rows. Beginning stream...
Sent 100 / 500 rows (actual rate: 19.8 rows/sec)
...
Done. Sent 500 rows in 25.1 seconds (19.9 rows/sec average).
```

✅ What you'll see in the CONSUMER terminal (Part B), within a few seconds:
- A batch table of raw records scrolling in (10 rows shown per batch)
- Followed by a batch table of running totals by region + category, updating
  as more data streams in

**This side-by-side view (producer sending on one screen, consumer reacting
on another) is the actual "real-time" demo moment** — have both terminal
windows visible/side-by-side on screen when you show this part.

---

## PART D Show the Dashboard

New terminal:
```cmd
cd C:\projects\sales-forecast\sales_dashboard
..\bigdata-env\Scripts\activate
python manage.py runserver
```
✅ Open browser to **http://127.0.0.1:8000/**
- **Overview** tab KPI summary (days evaluated, MAPE, best/worst prediction)
- **Predictions** tab full table, paginated
- **Charts** tab 4 charts (line, day-of-week bar, error bar, accuracy scatter)

---

## PART E — Clean shutdown after the demo (optional)

In each terminal, press `Ctrl+C` to stop:
- The consumer (`spark_streaming_consumer.py`)
- The Django server (`manage.py runserver`)

Kafka and HDFS windows can stay open or be closed — closing them just means
you'll need to restart them (Part A) next time.

---

## Quick troubleshooting during the demo

| Symptom | Fix |
|---|---|
| Producer errors "Unable to bootstrap from localhost:9092" | Kafka isn't running — check the Part A2 window is still open |
| Producer errors reading from HDFS | HDFS isn't running — check Part A1 windows are still open |
| Consumer never prints anything after producer finishes | Give it a few seconds — Spark micro-batches aren't instant. If nothing after 15-20 sec, check the consumer terminal for errors |
| "Address already in use" on Kafka start | A leftover Kafka process from earlier — find and kill it: `netstat -ano \| findstr :9093` then `taskkill /PID <number> /F`, then retry |
| Dashboard shows old error data | That's fine — the dashboard reads `daily_sales_predictions` (from `train_and_predict.py`), which is separate from the live streaming demo. You don't need to rerun training for the streaming demo. |

---

## Suggested presentation order

1. Start with **Part A + B** already running before your audience arrives
   (so there's no dead air waiting for Spark to boot)
2. Show the **Dashboard** first (Part D) familiar, visual, easy to explain
3. Then switch to the **live streaming demo** (Part C) as the technical
   highlight narrate what's happening as the producer sends and the
   consumer reacts
4. Wrap up by pointing back at the dashboard's Charts tab, tying the
   real-time pipeline back to the trained model's predictions
