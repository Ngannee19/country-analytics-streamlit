import streamlit as st
import pandas as pd
import vaex

st.set_page_config(page_title="Country Analytics", layout="wide")

st.title("🌍 Country Analytics Dashboard")
st.caption("Dataset: REST Countries • Engine: Vaex • UI: Streamlit")

@st.cache_data
def load_data():
    vx = vaex.open("countries.parquet")
    df = vx.to_pandas_df()
    df["density"] = df["population"] / df["area"]
    return df

df = load_data()

# ===== Sidebar =====
st.sidebar.header("Bộ lọc")
region = st.sidebar.selectbox(
    "Region",
    ["All"] + sorted(df["region"].dropna().unique().tolist())
)

min_pop = st.sidebar.slider(
    "Population tối thiểu",
    min_value=0,
    max_value=int(df["population"].max()),
    value=0,
    step=1_000_000
)

if region != "All":
    df = df[df["region"] == region]

df = df[df["population"] >= min_pop]

# ===== Metrics =====
c1, c2, c3 = st.columns(3)
c1.metric("Số quốc gia", len(df))
c2.metric("Tổng dân số", f"{int(df.population.sum()):,}")
c3.metric("Dân số TB", f"{int(df.population.mean()):,}")

# ===== Table =====
st.subheader("📋 Danh sách quốc gia")
st.dataframe(
    df.sort_values("population", ascending=False),
    use_container_width=True
)

# ===== Chart =====
st.subheader("📊 Tổng dân số theo Region")
chart = (
    df.groupby("region")["population"]
      .sum()
      .sort_values(ascending=False)
)
st.bar_chart(chart)
