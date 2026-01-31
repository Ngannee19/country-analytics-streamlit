import os
import streamlit as st
import pandas as pd

from analysis_vaex import (
    load_vaex_df,
    clean_countries,
    filter_countries,
    top_population,
    top_density,
    agg_by_region,
    descriptive_stats,
)

st.set_page_config(page_title="Countries Analytics (Vaex)", layout="wide")

st.title("🌍 Countries Analytics Dashboard")
st.caption(
    "Data: REST Countries → MongoDB Atlas (Câu 1) → Vaex EDA (Câu 2) → Streamlit Cloud (Câu 3)"
)

# =========================
# 0) Locate dataset file
# =========================
CANDIDATE_FILES = ["countries_eda.parquet", "countries.parquet"]
DATA_PATH = next((f for f in CANDIDATE_FILES if os.path.exists(f)), None)

if DATA_PATH is None:
    st.error(
        "Không tìm thấy file dữ liệu Parquet trong repo.\n\n"
        "Hãy đảm bảo bạn đã push `countries_eda.parquet` (hoặc `countries.parquet`) lên GitHub cùng thư mục với app.py."
    )
    st.stop()

# =========================
# 1) Load + Clean (cache)
# =========================
@st.cache_resource(show_spinner=True)
def get_vx(path: str):
    vx = load_vaex_df(path)
    vx = clean_countries(vx)
    return vx

vx = get_vx(DATA_PATH)

# =========================
# 2) Sidebar filters
# =========================
with st.sidebar:
    st.header("🔎 Bộ lọc")

    # Region options
    regions = []
    if "region" in vx.get_column_names():
        try:
            regions = sorted([r for r in vx.region.unique().tolist() if r and str(r).strip()])
        except Exception:
            regions = []
    region_pick = st.multiselect("Region", options=regions, default=[])

    # Population controls
    min_pop = st.number_input("Population tối thiểu", min_value=0, value=0, step=1_000_000)

    enable_max = st.toggle("Bật population tối đa", value=False)
    max_pop = None
    if enable_max:
        # nếu có population thì set max slider theo max thực tế
        pop_max = int(float(vx.population.max())) if "population" in vx.get_column_names() else 1_000_000_000
        max_pop = st.number_input("Population tối đa", min_value=0, value=min(500_000_000, pop_max), step=1_000_000)

    top_n = st.slider("Top N", min_value=5, max_value=50, value=10)

    if st.button("🔄 Refresh cache"):
        st.cache_resource.clear()
        st.cache_data.clear()
        st.rerun()

# region arg for function
region_arg = None
if len(region_pick) == 1:
    region_arg = region_pick[0]
elif len(region_pick) > 1:
    region_arg = region_pick

vx_f = filter_countries(vx, region=region_arg, min_pop=min_pop, max_pop=max_pop)

# =========================
# 3) Metrics
# =========================
n_rows = len(vx_f)
pop_sum = float(vx_f.population.sum()) if "population" in vx_f.get_column_names() else 0.0
pop_mean = float(vx_f.population.mean()) if "population" in vx_f.get_column_names() else 0.0

c1, c2, c3 = st.columns(3)
c1.metric("Số quốc gia", f"{n_rows:,}")
c2.metric("Tổng dân số", f"{int(pop_sum):,}")
c3.metric("Dân số trung bình", f"{int(pop_mean):,}")

st.divider()

# =========================
# 4) Tabs
# =========================
tab1, tab2, tab3, tab4 = st.tabs(["📋 Preview", "🏆 Top N", "📊 Aggregation", "🧾 Stats"])

def vx_to_pandas(vxdf, cols=None, n=200):
    if cols is None:
        return vxdf.head(n).to_pandas_df()
    return vxdf[cols].head(n).to_pandas_df()

with tab1:
    st.subheader("Preview dữ liệu sau lọc")

    cols = [c for c in ["cca3", "name_common", "capital", "region", "subregion", "population", "area", "density"]
            if c in vx_f.get_column_names()]
    df_preview = vx_to_pandas(vx_f, cols=cols, n=200)
    st.dataframe(df_preview, use_container_width=True)

    st.download_button(
        "⬇️ Download preview CSV",
        data=df_preview.to_csv(index=False).encode("utf-8"),
        file_name="countries_preview.csv",
        mime="text/csv",
    )

with tab2:
    st.subheader(f"Top {top_n} theo Population")
    df_top_pop = top_population(vx_f, n=top_n).to_pandas_df()
    st.dataframe(df_top_pop, use_container_width=True)

    st.download_button(
        "⬇️ Download Top Population CSV",
        data=df_top_pop.to_csv(index=False).encode("utf-8"),
        file_name="top_population.csv",
        mime="text/csv",
    )

    st.subheader(f"Top {top_n} theo Density")
    if "density" in vx_f.get_column_names():
        df_top_den = top_density(vx_f, n=top_n).to_pandas_df()
        st.dataframe(df_top_den, use_container_width=True)

        st.download_button(
            "⬇️ Download Top Density CSV",
            data=df_top_den.to_csv(index=False).encode("utf-8"),
            file_name="top_density.csv",
            mime="text/csv",
        )
    else:
        st.info("Không có cột `density`. Hãy đảm bảo dataset có `population` và `area` để tạo density.")

with tab3:
    st.subheader("Aggregation theo Region")
    df_agg = agg_by_region(vx_f).to_pandas_df()
    st.dataframe(df_agg, use_container_width=True)

    # chart tổng dân số theo region nếu có
    if "region" in df_agg.columns and "total_population" in df_agg.columns:
        st.bar_chart(df_agg.set_index("region")[["total_population"]])

    st.download_button(
        "⬇️ Download Aggregation CSV",
        data=df_agg.to_csv(index=False).encode("utf-8"),
        file_name="agg_by_region.csv",
        mime="text/csv",
    )

with tab4:
    st.subheader("Thống kê mô tả")
    stats = descriptive_stats(vx_f)

    if not stats:
        st.info("Không có đủ cột numeric để thống kê.")
    else:
        rows = []
        for k, v in stats.items():
            rows.append({"metric": k, "min": v.get("min"), "mean": v.get("mean"), "max": v.get("max")})
        df_stats = pd.DataFrame(rows)
        st.dataframe(df_stats, use_container_width=True)

        st.download_button(
            "⬇️ Download Stats CSV",
            data=df_stats.to_csv(index=False).encode("utf-8"),
            file_name="descriptive_stats.csv",
            mime="text/csv",
        )

st.caption(f"✅ Data file đang dùng: `{DATA_PATH}`")
