# =========================
# Vaex EDA utilities
# =========================
from __future__ import annotations

from typing import List, Optional, Union, Dict, Any
import vaex


# -------------------------
# 1) Load
# -------------------------
def load_vaex_df(path: str) -> vaex.dataframe.DataFrame:
    """
    Load dataset into a Vaex DataFrame.
    Supports: .parquet, .hdf5, .csv (csv may be slower for big files)
    """
    p = path.lower()
    if p.endswith(".parquet") or p.endswith(".hdf5"):
        return vaex.open(path)
    elif p.endswith(".csv"):
        # CSV -> Vaex can read, but for large file Parquet is recommended
        return vaex.from_csv(path, convert=False, copy_index=False)
    else:
        raise ValueError("Unsupported file type. Use .parquet / .hdf5 / .csv")


# -------------------------
# 2) Cleaning
# -------------------------
def clean_countries(vx: vaex.dataframe.DataFrame) -> vaex.dataframe.DataFrame:
    """
    Minimal cleaning:
    - Fill missing capital -> 'Unknown'
    - Ensure population & area are valid
    - Create density column
    """
    vx2 = vx.copy()

    # Fill null capital
    if "capital" in vx2.get_column_names():
        vx2["capital"] = vx2.capital.fillna("Unknown")

    # Filter invalid numeric
    if "population" in vx2.get_column_names():
        vx2 = vx2[vx2.population > 0]
    if "area" in vx2.get_column_names():
        vx2 = vx2[vx2.area > 0]

    # Create density
    if "population" in vx2.get_column_names() and "area" in vx2.get_column_names():
        vx2["density"] = vx2.population / vx2.area

    return vx2


# -------------------------
# 3) Filter
# -------------------------
def filter_countries(
    vx: vaex.dataframe.DataFrame,
    region: Optional[Union[str, List[str]]] = None,
    min_pop: int = 0,
    max_pop: Optional[int] = None,
) -> vaex.dataframe.DataFrame:
    """
    Filter by region and population threshold(s).
    region: None | "Asia" | ["Asia","Europe"]
    min_pop: minimum population
    max_pop: optional maximum population
    """
    vx_f = vx

    # Region filter
    if region is not None and "region" in vx_f.get_column_names():
        if isinstance(region, str):
            region_list = [region]
        else:
            region_list = region
        vx_f = vx_f[vx_f.region.isin(region_list)]

    # Population filter
    if "population" in vx_f.get_column_names():
        vx_f = vx_f[vx_f.population >= min_pop]
        if max_pop is not None:
            vx_f = vx_f[vx_f.population <= max_pop]

    return vx_f


# -------------------------
# 4) Top N
# -------------------------
def top_population(
    vx: vaex.dataframe.DataFrame,
    n: int = 10,
    cols: Optional[List[str]] = None,
) -> vaex.dataframe.DataFrame:
    """
    Top N countries by population.
    """
    if cols is None:
        cols = ["cca3", "name_common", "region", "population", "area"]
        if "density" in vx.get_column_names():
            cols.append("density")
return vx.sort("population", ascending=False)[cols].head(n)


def top_density(
    vx: vaex.dataframe.DataFrame,
    n: int = 10,
    cols: Optional[List[str]] = None,
) -> vaex.dataframe.DataFrame:
    """
    Top N countries by density.
    """
    if "density" not in vx.get_column_names():
        raise ValueError("density column not found. Run clean_countries() first or create vx['density'].")

    if cols is None:
        cols = ["cca3", "name_common", "region", "population", "area", "density"]

    return vx.sort("density", ascending=False)[cols].head(n)


# -------------------------
# 5) GroupBy/Aggregation
# -------------------------
def agg_by_region(vx: vaex.dataframe.DataFrame) -> vaex.dataframe.DataFrame:
    """
    Aggregations by region:
    - total population
    - avg area
    - number of countries
    """
    if "region" not in vx.get_column_names():
        raise ValueError("region column not found")

    agg_map = {
        "country_count": vaex.agg.count(),
    }
    if "population" in vx.get_column_names():
        agg_map["total_population"] = vaex.agg.sum("population")
        agg_map["avg_population"] = vaex.agg.mean("population")
    if "area" in vx.get_column_names():
        agg_map["avg_area"] = vaex.agg.mean("area")

    if "density" in vx.get_column_names():
        agg_map["avg_density"] = vaex.agg.mean("density")

    return vx.groupby(by="region", agg=agg_map).sort("country_count", ascending=False)


# -------------------------
# 6) Descriptive stats
# -------------------------
def descriptive_stats(vx: vaex.dataframe.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Return min/max/mean for population & density (if exist).
    Outputs as Python scalars for easy printing/report.
    """
    out: Dict[str, Dict[str, Any]] = {}

    if "population" in vx.get_column_names():
        out["population"] = {
            "min": vx.population.min().item(),
            "mean": vx.population.mean().item(),
            "max": vx.population.max().item(),
        }

    if "density" in vx.get_column_names():
        out["density"] = {
            "min": vx.density.min().item(),
            "mean": vx.density.mean().item(),
            "max": vx.density.max().item(),
        }

    if "area" in vx.get_column_names():
        out["area"] = {
            "min": vx.area.min().item(),
            "mean": vx.area.mean().item(),
            "max": vx.area.max().item(),
        }

    return out
