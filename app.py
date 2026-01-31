import streamlit as st
import pandas as pd

st.set_page_config(page_title="Countries Analytics", layout="wide")

@st.cache_data
def load_data():
    df = pd.read_parquet("countries_eda.parquet")
    df["density"] = df["population"] / df["area"]
    return df

df = load_data()

# Sidebar
region = st.sidebar.multiselect(
    "Region",
    options=sorted(df["region"].dropna().unique())
)
min_pop = st.sidebar.number_input("Min population", value=0, step=1_000_000)

if region:
    df = df[df["region"].isin(region)]
df = df[df["population"] >= min_pop]

st.metric("Countries", len(df))
st.metric("Total population", int(df.population.sum()))

st.dataframe(df.sort_values("population", ascending=False))
