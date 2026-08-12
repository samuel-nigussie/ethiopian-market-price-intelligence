

# 🇪🇹 Ethiopian Market Price Intelligence System

A Big Data platform designed to collect, process, store, analyze, and visualize Ethiopian commodity market-price data using a modern data engineering stack.

This project demonstrates an end-to-end data pipeline, combining **Apache Kafka** for real-time event streaming and **Hadoop HDFS & Apache Spark** for large-scale historical data processing. Insights are served through an interactive **Streamlit** dashboard.

## 🎯 Project Objectives

* Monitor commodity prices, inflation indicators, and trust scores across Ethiopian regions.
* Architect a dual-pipeline system (Batch + Streaming).
* Visualize price trends, geographic market distributions, and real-time analytics.
* Demonstrate scalable Big Data technologies in a localized economic context.

## 🏗️ System Architecture

The system operates on two parallel data flows:

1. **Historical Pipeline (Batch):**
`Raw CSV` ➔ `Hadoop HDFS` ➔ `Apache Spark` ➔ `Batch Analytics`
2. **Real-Time Pipeline (Streaming):**
`Replay Producer` ➔ `Apache Kafka (Topic: market-prices)` ➔ `Streamlit Dashboard`

## 🧰 Tech Stack

* **Data Processing & Storage:** Apache Hadoop (HDFS), Apache Spark (PySpark), Pandas
* **Real-time Streaming:** Apache Kafka
* **Visualization:** Streamlit, Plotly
* **Language:** Python 3.x

---

## 🚀 Quickstart Guide

### 1. Environment Setup

Clone the repository and set up a Python virtual environment:

```bash
git clone https://github.com/samuel-nigussie/ethiopian-market-price-intelligence.git
cd ethiopian-market-price-intelligence
python3 -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
pip install -r requirements.txt

```

### 2. Start Infrastructure (Hadoop & Kafka)

Ensure your Hadoop and Kafka clusters are running locally.

```bash
# Start HDFS
start-dfs.sh

# Create Kafka topic
kafka-topics.sh --create --topic market-prices --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

```

### 3. Load Historical Data

```bash
hdfs dfs -mkdir -p /user/market-data/raw
hdfs dfs -put data/raw/market_prices.csv /user/market-data/raw/

```

### 4. Run the Pipelines

Open separate terminal windows for the following processes (ensure the `venv` is activated in each):

**Run Spark Batch Analysis:**

```bash
spark-submit spark/historical_analysis.py

```

**Start the Kafka Producer (Simulates live market data):**

```bash
python producer/replay_producer.py

```

**Launch the Dashboard:**

```bash
streamlit run dashboard/app.py

```

---

## 🔮 Future Roadmap

* **Live API Integration:** Transition from simulated replay data to a live Ethiopian market API.
* **Machine Learning:** Implement a Spark MLlib pipeline to forecast future prices for staple commodities (Teff, Wheat, Maize).
* **Automated Alerts:** Anomaly detection for sudden market spikes or crashes.
* **Cloud Deployment:** Containerize with Docker and deploy to AWS/GCP.

## 👥 Project Team

* Amanuel Alemu Zewdu
* Merhawit Kahsay Gidey
* Samuel Nigussie Chanie
* Ana Boset Wakeyo
* Sofonias Berhane Kelet

## 📜 License

Developed for educational and research purposes as part of a Big Data Engineering initiative.
