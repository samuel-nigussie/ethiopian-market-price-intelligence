import json
from collections import deque

import pandas as pd
import plotly.express as px
import streamlit as st
from kafka import KafkaConsumer


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BROKER = "localhost:9092"
TOPIC = "market-prices"
GROUP_ID = "streamlit-dashboard-group"


# ============================================================
# KAFKA JSON DECODER
# ============================================================

def safe_json_decode(raw_bytes):
    try:
        return json.loads(raw_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Ethiopian Market Price Intelligence",
    page_icon="🇪🇹",
    layout="wide"
)

st.title("🇪🇹 Ethiopian Market Price Intelligence")
st.caption("Real-time market analytics powered by Kafka and Streamlit")


# ============================================================
# SESSION STATE
# ============================================================

if "events" not in st.session_state:
    st.session_state.events = deque(maxlen=5000)


# ============================================================
# KAFKA CONSUMER
# ============================================================

@st.cache_resource
def create_consumer():

    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        value_deserializer=safe_json_decode,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
    )


consumer = create_consumer()


# ============================================================
# READ NEW EVENTS
# ============================================================

try:

    new_events = 0

    for message in consumer:

        event = message.value

        if event is not None:
            st.session_state.events.append(event)
            new_events += 1

except Exception as e:

    st.error(f"Kafka connection error: {e}")


# ============================================================
# CONVERT TO DATAFRAME
# ============================================================

if len(st.session_state.events) == 0:

    st.warning("Waiting for market-price events from Kafka...")

    st.info(
        "Start replay_producer.py in another terminal "
        "to send market data."
    )

    st.stop()


df = pd.DataFrame(list(st.session_state.events))


# ============================================================
# DATA CLEANING
# ============================================================

df["price_close"] = pd.to_numeric(
    df["price_close"],
    errors="coerce"
)

df["price_high"] = pd.to_numeric(
    df["price_high"],
    errors="coerce"
)

df["price_low"] = pd.to_numeric(
    df["price_low"],
    errors="coerce"
)

df["inflation_pct"] = pd.to_numeric(
    df["inflation_pct"],
    errors="coerce"
)

df["trust_score"] = pd.to_numeric(
    df["trust_score"],
    errors="coerce"
)

df["price_date"] = pd.to_datetime(
    df["price_date"],
    errors="coerce"
)


# ============================================================
# SIDEBAR FILTERS
# ============================================================

st.sidebar.header("Filters")

commodities = sorted(
    df["commodity"].dropna().unique()
)

regions = sorted(
    df["region"].dropna().unique()
)

selected_commodities = st.sidebar.multiselect(
    "Commodity",
    commodities,
    default=commodities
)

selected_regions = st.sidebar.multiselect(
    "Region",
    regions,
    default=regions
)


filtered_df = df[
    df["commodity"].isin(selected_commodities)
    & df["region"].isin(selected_regions)
]


# ============================================================
# KEY METRICS
# ============================================================

st.subheader("Market Overview")

col1, col2, col3, col4 = st.columns(4)


with col1:
    st.metric(
        "Total Records",
        f"{len(filtered_df):,}"
    )


with col2:

    avg_price = filtered_df["price_close"].mean()

    st.metric(
        "Average Price",
        f"{avg_price:,.2f} ETB"
    )


with col3:

    avg_inflation = filtered_df["inflation_pct"].mean()

    st.metric(
        "Average Inflation",
        f"{avg_inflation:,.2f}%"
    )


with col4:

    avg_trust = filtered_df["trust_score"].mean()

    st.metric(
        "Average Trust Score",
        f"{avg_trust:,.2f}"
    )


# ============================================================
# PRICE ANALYSIS
# ============================================================

st.subheader("Price Analytics")

col1, col2 = st.columns(2)


with col1:

    commodity_prices = (
        filtered_df
        .groupby("commodity")["price_close"]
        .mean()
        .sort_values(ascending=False)
        .reset_index()
    )

    fig = px.bar(
        commodity_prices,
        x="commodity",
        y="price_close",
        title="Average Closing Price by Commodity"
    )

    fig.update_layout(
        xaxis_title="Commodity",
        yaxis_title="Average Price (ETB)"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


with col2:

    region_prices = (
        filtered_df
        .groupby("region")["price_close"]
        .mean()
        .sort_values(ascending=False)
        .reset_index()
    )

    fig = px.bar(
        region_prices,
        x="region",
        y="price_close",
        title="Average Closing Price by Region"
    )

    fig.update_layout(
        xaxis_title="Region",
        yaxis_title="Average Price (ETB)"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


# ============================================================
# PRICE TREND
# ============================================================

st.subheader("Price Trend")

trend = (
    filtered_df
    .groupby(["price_date", "commodity"])["price_close"]
    .mean()
    .reset_index()
)

fig = px.line(
    trend,
    x="price_date",
    y="price_close",
    color="commodity",
    markers=True,
    title="Market Price Trend"
)

fig.update_layout(
    xaxis_title="Date",
    yaxis_title="Closing Price (ETB)"
)

st.plotly_chart(
    fig,
    use_container_width=True
)


# ============================================================
# GEOGRAPHIC MAP
# ============================================================

st.subheader("Market Locations")

map_df = filtered_df.dropna(
    subset=["lat", "lon"]
).copy()

if len(map_df) > 0:

    fig = px.scatter_mapbox(
        map_df,
        lat="lat",
        lon="lon",
        color="region",
        size="price_close",
        hover_name="market",
        hover_data=[
            "commodity",
            "price_close",
            "inflation_pct",
            "trust_score"
        ],
        zoom=5,
        height=550
    )

    fig.update_layout(
        mapbox_style="open-street-map"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


# ============================================================
# LATEST MARKET DATA
# ============================================================

st.subheader("Latest Market Records")

display_columns = [
    "market",
    "region",
    "zone",
    "commodity",
    "price_date",
    "currency",
    "price_close",
    "inflation_pct",
    "trust_score"
]

available_columns = [
    col for col in display_columns
    if col in filtered_df.columns
]

st.dataframe(
    filtered_df[
        available_columns
    ].tail(20),
    use_container_width=True
)


