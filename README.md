# 🇪🇹 Ethiopian Market Price Intelligence System

A Big Data platform for collecting, streaming, storing, processing, and visualizing Ethiopian commodity market-price data.

The project combines **Apache Kafka**, **Hadoop HDFS**, **Apache Spark/PySpark**, and **Streamlit** to demonstrate an end-to-end Big Data pipeline.

## 🎯 Objectives

* 📊 Analyze historical Ethiopian commodity prices.
* ⚡ Simulate real-time price streaming with Kafka.
* 💾 Store streaming data in HDFS.
* 🔥 Process historical data using Spark.
* 📈 Visualize market-price trends through an interactive dashboard.

---

## 🏗️ Architecture

### Streaming Pipeline

```text
Historical Data
      │
      ▼
Replay Producer
      │
      ▼
Apache Kafka
      │
      ├──────────────► HDFS Consumer ──► HDFS
      │
      └──────────────► Analytics Consumer
                              │
                              ▼
                       Streamlit Dashboard
```

### Batch Pipeline

```text
Historical Data
      │
      ▼
     HDFS
      │
      ▼
Apache Spark
      │
      ▼
Historical Analytics
```

---

## 🧰 Tech Stack

* **Python** — Programming language
* **Apache Kafka** — Real-time streaming
* **Hadoop HDFS** — Distributed storage
* **Apache Spark / PySpark** — Big Data processing
* **Pandas** — Data processing
* **Streamlit** — Dashboard
* **Plotly** — Visualization
* **Jupyter Notebook** — Data exploration

---

## 📁 Project Structure

```text
ethiopian-market-price-intelligence/
│
├── consumers/
│   ├── analytics_dashboard.py
│   └── hdfs_writer.py
├── dashboard/
│   └── app.py
├── producer/
│   └── replay_producer.py
├── spark_jobs/
│   └── analyze_prices.py
├── notebooks/
│   └── ethiopia_prices_2007_2026.ipynb
├── data/
├── output/
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/samuel-nigussie/ethiopian-market-price-intelligence.git
cd ethiopian-market-price-intelligence
```

### 2. Install Dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Start Hadoop

```bash
start-dfs.sh
```

### 4. Start Kafka

Create the project topic:

```bash
kafka-topics.sh \
--create \
--topic market-prices \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1
```

### 5. Run the Pipeline

Start the HDFS consumer:

```bash
python consumers/hdfs_writer.py
```

Start the analytics consumer:

```bash
python consumers/analytics_dashboard.py
```

Start the producer:

```bash
python producer/replay_producer.py
```

Run Spark analysis:

```bash
spark-submit spark_jobs/analyze_prices.py
```

Launch the dashboard:

```bash
streamlit run dashboard/app.py
```

---

## 📊 Key Features

* Historical Ethiopian market-price analysis
* Real-time Kafka data streaming
* HDFS distributed storage
* Spark batch processing
* Interactive Streamlit dashboard
* Market and commodity price analytics

---

## 🔮 Future Improvements

* 🌐 Live market-price API integration
* 🤖 Price forecasting using Machine Learning
* 🚨 Automated price anomaly detection
* ☁️ Docker and cloud deployment
* 📡 Advanced real-time monitoring

---

## 👥 Project Team

| Name                       |
| -------------------------- |
| **Amanuel Alemu Zewdu**    |
| **Merhawit Kahsay Gidey**  |
| **Samuel Nigussie Chanie** |
| **Ana Boset Wakeyo**       |
| **Sofonias Berhane Kelet** |

---

## 📜 License

Developed for educational and research purposes as part of a Big Data Engineering initiative.
