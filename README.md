# 🇪🇹 Ethiopian Market Price Intelligence System

## 📌 Project Overview

The **Ethiopian Market Price Intelligence System** is a Big Data platform designed to collect, process, store, analyze, and visualize Ethiopian commodity market-price data.

The system combines **Apache Kafka, Hadoop HDFS, Apache Spark, Python, Pandas, and Streamlit** to demonstrate a complete Big Data pipeline.

Historical market-price data is stored for large-scale analysis, while Kafka is used to simulate a real-time stream of market events. A Streamlit dashboard consumes live Kafka events and presents market prices, inflation indicators, regional comparisons, trends, and geographic information.

The project demonstrates how Big Data technologies can be combined to transform raw market data into useful information for monitoring and decision-making.

---

# 🎯 Objectives

The main objectives of this project are:

* Collect and process Ethiopian commodity market-price data.
* Store large historical datasets using Hadoop HDFS.
* Use Apache Kafka for real-time data streaming.
* Process historical data using Apache Spark.
* Build real-time market analytics.
* Monitor commodity prices across Ethiopian markets and regions.
* Visualize price trends and regional differences.
* Demonstrate an end-to-end Big Data architecture.

---

# 🏗️ System Architecture

```text
                         ETHIOPIAN MARKET DATA
                                  │
                                  ▼
                       ┌─────────────────────┐
                       │ Historical Dataset  │
                       │ CSV / Market Data   │
                       └──────────┬──────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
                    ▼                           ▼
              ┌───────────┐              ┌──────────────┐
              │   HDFS    │              │ Kafka        │
              │ Historical│              │ Producer     │
              │ Storage   │              │ Replay       │
              └─────┬─────┘              └──────┬───────┘
                    │                           │
                    ▼                           ▼
              ┌───────────┐              ┌──────────────┐
              │  Spark    │              │ Kafka Topic  │
              │  Batch    │              │ market-prices│
              │ Analytics │              └──────┬───────┘
              └─────┬─────┘                     │
                    │                           ▼
                    │                    ┌──────────────┐
                    │                    │  Streamlit   │
                    │                    │  Dashboard   │
                    │                    └──────┬───────┘
                    │                           │
                    ▼                           ▼
             Historical Analysis        Real-Time Analytics
```

---

# 🔄 Data Flow

The system has two major data paths.

## 1. Historical Data Pipeline

Historical market data is loaded into Hadoop HDFS.

```text
Historical Dataset
       ↓
      HDFS
       ↓
     Spark
       ↓
Batch Processing
       ↓
Historical Market Analysis
```

HDFS provides distributed storage for large datasets, while Spark is used to process and analyze the stored data.

---

## 2. Real-Time Streaming Pipeline

Historical records can also be replayed through Kafka to simulate live market-price events.

```text
Historical Dataset
       ↓
Replay Producer
       ↓
Apache Kafka
       ↓
market-prices topic
       ↓
Streamlit Consumer
       ↓
Real-Time Dashboard
```

The replay producer allows the project to demonstrate real-time streaming even when a real live market API is not available.

---

# 🧰 Technologies Used

| Technology   | Purpose                             |
| ------------ | ----------------------------------- |
| Python       | Main programming language           |
| Apache Kafka | Real-time data streaming            |
| Hadoop HDFS  | Distributed historical data storage |
| Apache Spark | Large-scale data processing         |
| PySpark      | Python interface for Spark          |
| Pandas       | Data manipulation                   |
| Streamlit    | Interactive dashboard               |
| Plotly       | Interactive visualizations          |
| JSON         | Event/message format                |
| Git & GitHub | Version control                     |

---

# 📂 Project Structure

```text
ethiopian-market-price-intelligence/
│
├── data/
│   ├── raw/
│   │   └── market_prices.csv
│   │
│   └── processed/
│
├── producer/
│   └── replay_producer.py
│
├── kafka/
│   └── kafka_config.md
│
├── spark/
│   ├── historical_analysis.py
│   └── spark_processing.py
│
├── dashboard/
│   └── app.py
│
├── hdfs/
│   └── hdfs_commands.md
│
├── notebooks/
│   └── analysis.ipynb
│
├── requirements.txt
│
├── .gitignore
│
└── README.md
```

> Adjust the folder names to match the actual folders in your repository.

---

# 📊 Dataset

The project uses Ethiopian market-price data containing information such as:

* Market
* Region
* Zone
* Commodity
* Date
* Closing price
* High price
* Low price
* Currency
* Inflation percentage
* Trust score
* Latitude
* Longitude

Example event:

```json
{
    "market": "Addis Ababa",
    "region": "Addis Ababa",
    "zone": "Addis Ababa",
    "commodity": "teff",
    "price_date": "2013-04-01",
    "currency": "ETB",
    "price_close": 2450.50,
    "price_high": 2500.00,
    "price_low": 2400.00,
    "inflation_pct": 8.4,
    "trust_score": 0.91,
    "lat": 9.03,
    "lon": 38.74
}
```

---

# 📨 Kafka Streaming

Apache Kafka is responsible for transporting market-price events between the producer and the dashboard.

The main Kafka topic is:

```text
market-prices
```

The producer reads historical market records and publishes them as JSON messages.

```text
Producer
   │
   ▼
Kafka Broker
   │
   ▼
market-prices
   │
   ▼
Dashboard Consumer
```

### Example Kafka message

```json
{
    "market": "Bahir Dar",
    "region": "Amhara",
    "commodity": "wheat",
    "price_close": 1200.50,
    "price_date": "2013-04-01"
}
```

---

# ⚡ Real-Time Simulation

Because real-time market data is not continuously available for the project, historical data is replayed through Kafka.

For example:

```text
Historical Record 1
        ↓
      Kafka
        ↓
    Dashboard

Historical Record 2
        ↓
      Kafka
        ↓
    Dashboard

Historical Record 3
        ↓
      Kafka
        ↓
    Dashboard
```

This allows the project to simulate a real-time market data environment.

The architecture can later be connected to a real market API or data collection system without changing the Kafka-to-dashboard design significantly.

---

# 🗄️ Hadoop HDFS

Hadoop HDFS is used as the distributed storage layer for historical market data.

The purpose of HDFS is to provide scalable storage for datasets that may become too large for a traditional local filesystem.

Example structure:

```text
/user/market-data/
│
├── raw/
│
├── processed/
│
└── analytics/
```

Example command:

```bash
hdfs dfs -mkdir -p /user/market-data/raw
```

Upload data:

```bash
hdfs dfs -put data/raw/market_prices.csv /user/market-data/raw/
```

List files:

```bash
hdfs dfs -ls /user/market-data/raw
```

---

# 🔥 Apache Spark

Apache Spark is used for processing and analyzing historical market data.

Spark is particularly useful when the dataset becomes too large for traditional single-machine processing.

Example Spark workflow:

```text
HDFS
 ↓
Spark
 ↓
Data Cleaning
 ↓
Transformation
 ↓
Aggregation
 ↓
Analysis
```

Possible Spark analyses include:

* Average commodity price
* Average price by region
* Price changes over time
* Commodity comparisons
* Regional price differences
* Inflation analysis
* Market-level statistics

---

# 📈 Streamlit Dashboard

The Streamlit dashboard provides an interactive interface for monitoring market conditions.

The dashboard includes:

### Market Overview

Displays:

* Total records
* Average price
* Average inflation
* Average trust score

### Commodity Analysis

Shows average prices by commodity.

### Regional Analysis

Compares average prices between regions.

### Price Trends

Displays commodity prices over time.

### Geographic Market Map

Shows market locations using latitude and longitude.

### Latest Market Records

Displays the most recently received market events.

---

# 🖥️ Dashboard Preview

The dashboard is designed around the following layout:

```text
┌─────────────────────────────────────────────────────┐
│ 🇪🇹 Ethiopian Market Price Intelligence             │
│ Real-time market analytics powered by Kafka         │
├─────────────────────────────────────────────────────┤
│                                                     │
│ Total Records │ Avg Price │ Inflation │ Trust      │
│                                                     │
├─────────────────────────────────────────────────────┤
│                                                     │
│ Average Price by Commodity │ Price by Region       │
│                                                     │
├─────────────────────────────────────────────────────┤
│                                                     │
│              Market Price Trend                    │
│                                                     │
├─────────────────────────────────────────────────────┤
│                                                     │
│              Market Locations Map                  │
│                                                     │
├─────────────────────────────────────────────────────┤
│                                                     │
│              Latest Market Records                 │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

# 🚀 Installation

## 1. Clone the Repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
```

Move into the project:

```bash
cd ethiopian-market-price-intelligence
```

---

# 🐍 2. Create a Python Virtual Environment

```bash
python3 -m venv venv
```

Activate it:

### Linux / Ubuntu

```bash
source venv/bin/activate
```

### Windows

```bash
venv\Scripts\activate
```

---

# 📦 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

If you don't have a `requirements.txt` yet, the main Python packages are:

```text
kafka-python
pandas
streamlit
plotly
pyspark
```

---

# 📨 4. Start Kafka

Make sure your Kafka broker is running.

The project expects Kafka at:

```text
localhost:9092
```

Create the topic:

```bash
kafka-topics.sh \
--create \
--topic market-prices \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1
```

Check the topic:

```bash
kafka-topics.sh \
--list \
--bootstrap-server localhost:9092
```

---

# 🗄️ 5. Start Hadoop

Make sure HDFS is running:

```bash
start-dfs.sh
```

Check Java/Hadoop processes:

```bash
jps
```

You should see Hadoop services such as:

```text
NameNode
DataNode
SecondaryNameNode
```

---

# 📤 6. Upload Historical Data to HDFS

Create the directory:

```bash
hdfs dfs -mkdir -p /user/market-data/raw
```

Upload the dataset:

```bash
hdfs dfs -put data/raw/market_prices.csv /user/market-data/raw/
```

Verify:

```bash
hdfs dfs -ls /user/market-data/raw
```

---

# ⚡ 7. Start the Kafka Producer

Run:

```bash
python producer/replay_producer.py
```

The producer will read market-price records and publish them to:

```text
market-prices
```

You should see output similar to:

```text
Sent 100 events...
Sent 200 events...
Sent 300 events...
```

---

# 📊 8. Start the Dashboard

Open another terminal.

Activate the environment:

```bash
source venv/bin/activate
```

Run:

```bash
streamlit run dashboard/app.py
```

Streamlit will provide a local address where the dashboard can be opened in a browser.

---

# 🔄 Running the Complete System

For a complete demonstration, run the components in separate terminals.

### Terminal 1 — Hadoop

```bash
start-dfs.sh
```

### Terminal 2 — Kafka

Start your Kafka services.

### Terminal 3 — Producer

```bash
python producer/replay_producer.py
```

### Terminal 4 — Spark

Run the historical analysis:

```bash
spark-submit spark/historical_analysis.py
```

### Terminal 5 — Dashboard

```bash
streamlit run dashboard/app.py
```

---

# 🧠 Example End-to-End Flow

```text
                 ┌──────────────────────┐
                 │ Ethiopian Market Data│
                 └──────────┬───────────┘
                            │
             ┌──────────────┴──────────────┐
             │                             │
             ▼                             ▼
        Historical                    Replay Producer
             │                             │
             ▼                             ▼
           HDFS                          Kafka
             │                             │
             ▼                             ▼
          Spark                      Streamlit
             │                             │
             ▼                             ▼
      Batch Analytics                Live Dashboard
```

---

# 📌 Key Features

* 🇪🇹 Ethiopian commodity market monitoring
* ⚡ Kafka-based event streaming
* 🗄️ HDFS distributed storage
* 🔥 Spark-based Big Data processing
* 📊 Interactive Streamlit dashboard
* 📈 Price trend analysis
* 🌍 Regional market comparison
* 🗺️ Geographic visualization
* 📉 Inflation analysis
* 🔎 Commodity filtering
* 📡 Simulated real-time market streaming

---

# 🔐 Data Quality

The dashboard performs basic data cleaning before analysis.

Numeric fields such as:

```text
price_close
price_high
price_low
inflation_pct
trust_score
```

are converted to numeric values.

Invalid values are converted to missing values instead of causing the dashboard to crash.

Dates are converted using:

```python
pd.to_datetime()
```

Kafka messages are also safely decoded from JSON.

---

# 📊 Example Analytics

The system can answer questions such as:

### Commodity

* Which commodity has the highest average price?
* How does teff compare with wheat?
* Which commodity shows the largest price variation?

### Region

* Which region has the highest average market price?
* How do prices differ between regions?
* Which markets have unusually high prices?

### Time

* How are commodity prices changing over time?
* Are prices increasing or decreasing?
* What periods show significant price changes?

### Market

* Which markets have the highest prices?
* Where are particular commodities concentrated?
* What markets have high or low trust scores?

---

# 🎯 Why These Technologies?

## Apache Kafka

Kafka provides a reliable way to transport market events between data producers and consumers.

```text
Producer → Kafka → Consumer
```

It allows the system to handle continuous streams of data.

---

## Hadoop HDFS

HDFS provides distributed storage for historical data.

It is useful when datasets become too large to efficiently manage on a single machine.

---

## Apache Spark

Spark provides fast distributed processing for historical and large-scale datasets.

Instead of manually processing millions of records with Python loops, Spark can distribute computation across a cluster.

---

## Streamlit

Streamlit provides a simple way to turn Python analytics into an interactive dashboard.

---

# ⚙️ Scalability

The architecture is designed so individual components can be scaled independently.

For example:

```text
More data
   ↓
More Kafka partitions
   ↓
More Kafka consumers
   ↓
More Spark workers
   ↓
More processing capacity
```

Kafka partitions can allow multiple consumers to process different portions of a stream.

Spark can distribute large analytical workloads across multiple worker nodes.

---

# 🔮 Future Improvements

Possible future improvements include:

* Connect to live Ethiopian market APIs.
* Add machine-learning price forecasting.
* Add anomaly detection.
* Add commodity price alerts.
* Add historical price comparison.
* Add user authentication.
* Add database storage for dashboard results.
* Deploy Kafka and Spark on multiple nodes.
* Deploy the dashboard to a cloud platform.
* Add automated data ingestion.
* Add more Ethiopian commodities.
* Add more market locations.
* Add predictive analytics for future prices.

---

# 🤖 Future Machine Learning Extension

A future version could add a machine-learning pipeline:

```text
Historical Market Data
        ↓
HDFS
        ↓
Spark
        ↓
Feature Engineering
        ↓
Machine Learning
        ↓
Price Prediction
        ↓
Streamlit Dashboard
```

For example, the system could predict the future price of:

```text
Teff
Wheat
Maize
Coffee
```

This would transform the project from a monitoring system into a **market intelligence and prediction platform**.

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

# 🧪 Project Status

### Current Implementation

* [x] Historical market dataset
* [x] Kafka topic
* [x] Kafka producer
* [x] JSON event streaming
* [x] Kafka consumer
* [x] Streamlit dashboard
* [x] Commodity filtering
* [x] Regional filtering
* [x] Price analytics
* [x] Inflation metrics
* [x] Trust score metrics
* [x] Price trend visualization
* [x] Geographic market map
* [x] Latest market records
* [x] HDFS storage
* [x] Spark processing

### Future Work

* [ ] Live external market API
* [ ] Machine-learning price prediction
* [ ] Anomaly detection
* [ ] Automated alerts
* [ ] Cloud deployment
* [ ] Multi-node deployment

---

# 📜 License

This project was developed for educational and research purposes as part of a Big Data engineering project.

---

# 🙏 Acknowledgements

We acknowledge the open-source communities behind:

* Apache Kafka
* Apache Hadoop
* Apache Spark
* Python
* Pandas
* Streamlit
* Plotly

These technologies made it possible to build the end-to-end Big Data pipeline demonstrated in this project.

---

# ⭐ Conclusion

The **Ethiopian Market Price Intelligence System** demonstrates how modern Big Data technologies can work together to build a scalable market-monitoring platform.

The project combines:

```text
HDFS
  +
Spark
  +
Kafka
  +
Python
  +
Streamlit
      ↓
Big Data Market Intelligence
```

The architecture separates **historical data processing** from **real-time event streaming**, allowing the system to support both large-scale historical analytics and real-time market monitoring.

This architecture can be extended in the future with machine learning, anomaly detection, live data sources, and cloud deployment.
