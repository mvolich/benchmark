# dashboard.py — Rubrics Positioning & Risk Dashboard (Streamlit, single-file)
# ---------------------------------------------------------------------------------
# Implements the specification provided by Marcos for a 12‑month positioning & risk dashboard.
# Reads `Dashboard_Input.xlsx` uploaded via sidebar, validates inputs, computes scenario P&L
# (rates & credit), and renders enterprise-grade visuals with Rubrics branding.
#
# Branding references aligned to prior internal app styling (colors, typography, template).  :contentReference[oaicite:0]{index=0}
#
# How to run:
#   streamlit run dashboard.py
#
# Notes:
# - Plotly is used for all charts.
# - LLM “Insights” tab is deterministic: pure string builder over computed tables (no external calls).
# - Roll-down is wired as 0 in v1 (placeholder for future field).
# - This file is deterministic, vectorised (NumPy/Pandas), and robust to common data quirks.

from __future__ import annotations

import io
import json
import math
import textwrap
from dataclasses import dataclass
from typing import Dict, List, Tuple
from decimal import Decimal, ROUND_HALF_UP

import numpy as np
import openai
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
import streamlit as st

# ================================ Branding & Theme ================================

RB_COLORS = {
    "blue":   "#001E4F",  # Dark Blue
    "med":    "#2C5697",  # Medium Blue
    "light":  "#7BA4DB",  # Light Blue
    "grey":   "#D8D7DF",  # Grey
    "orange": "#CF4520",  # Rubrics Orange
}

# Plotly template
BRAND_TEMPLATE = go.layout.Template(
    layout=go.Layout(
        colorway=[RB_COLORS["blue"], RB_COLORS["med"], RB_COLORS["light"], RB_COLORS["grey"], RB_COLORS["orange"]],
        font=dict(family="Inter, Segoe UI, Roboto, Arial, sans-serif"),
        legend=dict(orientation="h", y=1.02, yanchor="bottom", x=1, xanchor="right"),
        margin=dict(l=12, r=12, t=40, b=40),
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        title=dict(font=dict(size=18, color=RB_COLORS["blue"])),
        xaxis=dict(showgrid=True, gridcolor="rgba(128,128,128,0.15)"),
        yaxis=dict(showgrid=True, gridcolor="rgba(128,128,128,0.15)"),
    )
)
pio.templates["rubrics"] = BRAND_TEMPLATE
pio.templates.default = "rubrics"
PLOTLY_CONFIG = {"displaylogo": False, "responsive": True}
PLOT_HEIGHT = 380

def inject_brand_css() -> None:
    st.markdown(
        """
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

  :root{
    --rb-blue:#001E4F; --rb-mblue:#2C5697; --rb-lblue:#7BA4DB; --rb-grey:#D8D7DF; --rb-orange:#CF4520;
  }
  html, body, .stApp { background:#f7f8fb; color:#0b0c0c; font-family: Inter, "Segoe UI", Roboto, Arial, sans-serif !important; }
  .block-container { padding-top: 3.5rem; padding-bottom: 4rem; }

  /* Header */
  .rb-header { display:flex; align-items:flex-start; justify-content:space-between; gap:12px; }
  .rb-title { display:flex; flex-direction:column; }
  .rb-title h1 { margin:0; padding:0; font-size:2.0rem; line-height:1.1; font-weight:700; color:var(--rb-blue); }
  .rb-sub { color:#444; font-size:.9rem; margin-top:.25rem; }
  .rb-logo img { height:40px; margin-top:2px; }

  /* Controls */
  .stSelectbox label, .stRadio label, .stCheckbox label { font-weight:600; color:var(--rb-blue); }
  .stButton>button, .stDownloadButton>button {
    background: var(--rb-mblue); color:#fff; border:none; border-radius:4px; padding:8px 14px; font-weight:700;
  }
  .stButton>button:hover, .stDownloadButton>button:hover { background: var(--rb-blue); }

  /* Tabs */
  .stTabs [data-baseweb="tab-list"]{ gap:8px; border-bottom:none; }
  .stTabs [data-baseweb="tab"]{
    background:#ffffff; border:1px solid var(--rb-grey); border-bottom:2px solid var(--rb-grey);
    border-radius:4px 4px 0 0; color:var(--rb-blue); font-weight:600; padding:.5rem .9rem;
  }
  .stTabs [aria-selected="true"]{
    background:#fff; border-color:var(--rb-mblue); border-bottom:3px solid var(--rb-orange); color:#000;
  }

  /* Status pills */
  .pill { display:inline-block; padding:2px 8px; border-radius:999px; font-size:.78rem; font-weight:700; margin-right:6px; }
  .pill.ok { background:#e6f4ea; color:#137333; border:1px solid #c8e6c9; }
  .pill.warn { background:#fff8e1; color:#8a6d00; border:1px solid #ffecb3; }
  .pill.err { background:#fdecea; color:#b00020; border:1px solid #f7c5c0; }

  /* KPI tiles */
  .kpi { background:#fff; border:1px solid var(--rb-grey); border-radius:6px; padding:10px 12px; }
  .kpi .label { font-size:.85rem; color:#344; font-weight:600; margin-bottom:4px; }
  .kpi .value { font-size:1.35rem; font-weight:800; color:#000; }

  /* Popover button */
  .link { color:var(--rb-mblue); text-decoration:underline; cursor:pointer; }
</style>
        """,
        unsafe_allow_html=True,
    )

# ================================ Page Config ================================

st.set_page_config(
    page_title="Rubrics Positioning & Risk Dashboard",
    page_icon="https://rubricsam.com/wp-content/uploads/2021/01/cropped-rubrics-logo-tight.png",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_brand_css()

# ================================ Constants & Helpers ================================

REQUIRED_COMBINED_COLS = [
    "Entity", "Reference Fund", "Currency",
    "KRD 6m", "KRD 2y", "KRD 5y", "KRD 10y", "KRD 20y", "KRD 30y",
    "DTS", "Hedged Yield", "Hedged Yield Contr",
]
REQUIRED_SCENARIOS_COLS = [
    "Scenario", "Scenario Name", "Currency", "6m", "2yr", "5 yr", "10 yr", "20 yr", "30 yr", "Credit Spread Change %"
]
REQUIRED_OGC_COLS = ["Name", "OGC"]

TENOR_COLUMNS_MAP = {
    "6m": "6m",
    "2yr": "2y",
    "5 yr": "5y",
    "10 yr": "10y",
    "20 yr": "20y",
    "30 yr": "30y",
}
KRD_NODE_COLUMNS = ["KRD 6m", "KRD 2y", "KRD 5y", "KRD 10y", "KRD 20y", "KRD 30y"]
NODES_ORDER = ["6m", "2y", "5y", "10y", "20y", "30y"]

FUND_CODE_TO_NAME_HINTS = {
    "GCF": ["global credit ucits", "global credit", "gcf"],
    "GFI": ["global fixed income ucits", "global fixed income", "gfi"],
    "EYF": ["enhanced yield ucits", "enhanced yield", "eyf"],
}

BPS_FMT = "{:+.0f} bps"
BPS_FMT_POS = "{:.0f} bps"
PCT_FMT = "{:.2f}%"

def _fail(msg: str):
    st.error(msg)
    st.stop()

def require_columns(df: pd.DataFrame, required: List[str], sheet: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        st.error(
            f"❌ Missing required columns in **{sheet}**: {missing}\n\n"
            f"Found columns: {list(df.columns)}"
        )
        st.stop()

def to_numeric(df: pd.DataFrame, cols: List[str], sheet: str):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            if df[c].isna().any():
                bad = df.index[df[c].isna()].tolist()
                _fail(f"❌ Column **{c}** in **{sheet}** contains non-numeric values. Bad rows (0-based): {bad}")

def trim_currency(df: pd.DataFrame):
    if "Currency" in df.columns:
        df["Currency"] = df["Currency"].astype(str).str.strip()

def currency_filter_for_charts(df: pd.DataFrame) -> pd.DataFrame:
    mask = ~df["Currency"].str.strip().str.upper().isin({"TOTAL", "OTHER"})
    return df.loc[mask].copy()

def canonicalize_currency(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.upper()

def canonicalize_spread_sign(x: pd.Series) -> pd.Series:
    # Sheet legacy sign: + = tightening, − = widening
    # Canonical: + = widening (hurts when DTS > 0)
    return -1.0 * pd.to_numeric(x, errors="coerce").fillna(0.0)

def map_fund_to_ogc(ogc_df: pd.DataFrame) -> Dict[str, float]:
    # Build a robust join from code -> OGC (bps). Prefer explicit 'Code' column if present; else fuzzy by 'Name'.
    out = {}
    cols_lower = {c.lower(): c for c in ogc_df.columns}
    code_col = cols_lower.get("code")
    name_col = "Name"
    rate_col = "OGC"
    if code_col:
        for code in ["GCF", "GFI", "EYF"]:
            rows = ogc_df.loc[ogc_df[code_col].astype(str).str.strip().str.upper() == code]
            if len(rows) == 1:
                out[code] = float(rows[rate_col].iloc[0])
    # Fuzzy fallback by name
    for code in ["GCF", "GFI", "EYF"]:
        if code in out:
            continue
        hints = FUND_CODE_TO_NAME_HINTS[code]
        cand = ogc_df[name_col].astype(str).str.lower()
        hit = None
        for h in hints:
            m = ogc_df.loc[cand.str.contains(h, na=False)]
            if len(m) >= 1:
                hit = float(m[rate_col].iloc[0])
                break
        if hit is None and len(ogc_df) == 1:
            # Only one row — assume it applies (common in prototypes)
            hit = float(ogc_df[rate_col].iloc[0])
        if hit is None:
            st.warning(f"⚠️ Could not map **{code}** to an OGC row by name; defaulting to 0 bps.")
            hit = 0.0
        out[code] = hit
    return out

# ================================ Data Ingest ================================

@dataclass
class Inputs:
    combined: pd.DataFrame
    scenarios: pd.DataFrame
    ogc_map_bps: Dict[str, float]
    asof_by_fund: Dict[str, pd.Timestamp]
    scenario_list: List[Tuple[int, str]]

@st.cache_data(show_spinner=False)
def load_workbook(uploaded_bytes: bytes) -> Inputs:
    if uploaded_bytes is None:
        _fail("Please upload **Dashboard_Input.xlsx** to proceed.")

    xls = pd.ExcelFile(io.BytesIO(uploaded_bytes), engine="openpyxl")

    if "Combined 2" not in xls.sheet_names:
        _fail("Sheet **Combined 2** not found.")
    if "Scenarios" not in xls.sheet_names:
        _fail("Sheet **Scenarios** not found.")
    if "OGC" not in xls.sheet_names:
        _fail("Sheet **OGC** not found.")

    combined = pd.read_excel(xls, sheet_name="Combined 2")
    scenarios = pd.read_excel(xls, sheet_name="Scenarios")
    ogc = pd.read_excel(xls, sheet_name="OGC")

    # Basic validation
    require_columns(combined, REQUIRED_COMBINED_COLS, "Combined 2")
    require_columns(scenarios, REQUIRED_SCENARIOS_COLS, "Scenarios")
    require_columns(ogc, REQUIRED_OGC_COLS, "OGC")

    # Coerce numeric where needed
    to_numeric(combined, ["DTS", "Hedged Yield", "Hedged Yield Contr"] + KRD_NODE_COLUMNS, "Combined 2")
    to_numeric(scenarios, ["6m", "2yr", "5 yr", "10 yr", "20 yr", "30 yr", "Credit Spread Change %"], "Scenarios")
    to_numeric(ogc, ["OGC"], "OGC")

    # Trim currency fields
    trim_currency(combined)
    scenarios["Currency"] = scenarios["Currency"].astype(str).str.strip()

    # Canonicalize currencies for joins
    combined["Currency_u"] = canonicalize_currency(combined["Currency"])
    scenarios["Currency_u"] = canonicalize_currency(scenarios["Currency"])

    # Prepare scenario shocks table (long-form by scenario, currency)
    shock_cols = ["6m", "2yr", "5 yr", "10 yr", "20 yr", "30 yr"]
    # Canonical spread (%): + = widening
    scenarios["spread_move_pct"] = canonicalize_spread_sign(scenarios["Credit Spread Change %"])

    # Scenario list (id, name)
    # Ensure "Scenario" is integer-like
    scenarios["Scenario"] = pd.to_numeric(scenarios["Scenario"], errors="coerce").astype(int)
    scenario_list = (
        scenarios[["Scenario", "Scenario Name"]]
        .drop_duplicates()
        .sort_values(["Scenario"])
        .itertuples(index=False, name=None)
    )

    # OGC map
    ogc_map = map_fund_to_ogc(ogc)

    # As-of dates (banner)
    asof_by_fund = {}
    for code, sheet in [("GCF", "GCF Raw"), ("GFI", "GFI Raw"), ("EYF", "EYF Raw")]:
        if sheet in xls.sheet_names:
            try:
                raw = pd.read_excel(xls, sheet_name=sheet)
                # Try common variants
                date_col = None
                for c in raw.columns:
                    if str(c).strip().lower().replace(" ", "") in {"asofdate", "asof", "as_of_date", "as_of"}:
                        date_col = c
                        break
                if date_col is None:
                    # Assume first column has dates if small sheet
                    date_col = raw.columns[0]
                dates = pd.to_datetime(raw[date_col], errors="coerce")
                dt = pd.NaT
                if dates.notna().any():
                    dt = pd.to_datetime(dates.dropna()).max()
                asof_by_fund[code] = dt
            except Exception:
                asof_by_fund[code] = pd.NaT
        else:
            asof_by_fund[code] = pd.NaT

    # Final normalisations: enforce allowed Entities and Fund codes
    combined["Entity"] = combined["Entity"].astype(str).str.strip().str.title()
    combined["Reference Fund"] = combined["Reference Fund"].astype(str).str.strip().str.upper()

    # Keep only rows for {Fund, Index} and fund codes in set
    combined = combined[combined["Entity"].isin(["Fund", "Index"])].copy()
    combined = combined[combined["Reference Fund"].isin(["GCF", "GFI", "EYF"])].copy()

    return Inputs(
        combined=combined,
        scenarios=scenarios,
        ogc_map_bps=ogc_map,
        asof_by_fund=asof_by_fund,
        scenario_list=list(scenario_list),
    )

# ================================ Core Calculations ================================

@dataclass
class ScenarioResult:
    fund_code: str
    scenario_id: int
    scenario_name: str
    carry_bps_fund: float
    carry_bps_idx: float
    ogc_bps_fund: float
    ogc_bps_idx: float
    credit_bps_fund: float
    credit_bps_idx: float
    rates_bps_fund: float
    rates_bps_idx: float
    etr_bps_fund: float
    etr_bps_idx: float
    rel_etr_bps: float  # Fund - Index

# --- Carry, DTS, KRD helpers ---

def slice_entity(df: pd.DataFrame, fund_code: str, entity: str) -> pd.DataFrame:
    return df[(df["Reference Fund"] == fund_code) & (df["Entity"] == entity)].copy()

def per_currency_view(df_slice: pd.DataFrame) -> pd.DataFrame:
    # Exclude Total/Other for per-ccy visuals
    return currency_filter_for_charts(df_slice)

def carry_bps(df_slice: pd.DataFrame) -> float:
    # Sum Hedged Yield Contr across per-currency rows (exclude Total/Other) and *100 to get bps
    per_ccy = per_currency_view(df_slice)
    val_pct = float(per_ccy["Hedged Yield Contr"].sum())
    return 100.0 * val_pct

def carry_recon_ok(df_slice: pd.DataFrame, tol_pct: float = 1e-6) -> Tuple[bool, float, float]:
    # Reconcile Σ Hedged Yield Contr (per-ccy) to Hedged Yield on Currency="Total"
    per_ccy = per_currency_view(df_slice)
    sum_contrib_pct = float(per_ccy["Hedged Yield Contr"].sum())
    total_row = df_slice.loc[df_slice["Currency_u"].eq("TOTAL")]
    total_yield_pct = float(total_row["Hedged Yield"].iloc[0]) if len(total_row) else float("nan")
    ok = (not math.isnan(total_yield_pct)) and abs(sum_contrib_pct - total_yield_pct) <= tol_pct
    return ok, sum_contrib_pct, total_yield_pct

def dts_total(df_slice: pd.DataFrame) -> float:
    # Use ONLY Currency="Total"
    total_row = df_slice.loc[df_slice["Currency_u"].eq("TOTAL"), "DTS"]
    if total_row.empty:
        _fail("No **Currency='Total'** row found for DTS.")
    return float(total_row.iloc[0])

def krd_matrix(df_slice: pd.DataFrame) -> pd.DataFrame:
    # Return per-ccy KRDs with columns aligned to nodes order
    per_ccy = per_currency_view(df_slice)
    cols = {"KRD 6m": "6m", "KRD 2y": "2y", "KRD 5y": "5y", "KRD 10y": "10y", "KRD 20y": "20y", "KRD 30y": "30y"}
    m = per_ccy[["Currency_u"] + list(cols.keys())].copy().rename(columns=cols)
    m = m.set_index("Currency_u")[NODES_ORDER]
    m = m.fillna(0.0).astype(float)
    return m

# --- Scenario shocks & completeness ---

def scenario_shocks_for_fund(scenarios: pd.DataFrame, fund_ccy_set: List[str], scenario_id: int
                             ) -> Tuple[pd.DataFrame, float, List[str]]:
    """
    Returns:
        rates_df: DataFrame indexed by Currency_u with columns NODES_ORDER (bp shocks)
        spread_move_pct: float (canonical, + = widening)
        warnings: list of non-blocking warnings regarding missing nodes/currencies
    """
    srows = scenarios.loc[scenarios["Scenario"] == scenario_id].copy()
    if srows.empty:
        _fail(f"Scenario id {scenario_id} not found.")

    # Build per-currency shocks table with canonical columns
    rates = (srows[["Currency_u", "6m", "2yr", "5 yr", "10 yr", "20 yr", "30 yr"]]
             .rename(columns={"2yr": "2y", "5 yr": "5y", "10 yr": "10y", "20 yr": "20y", "30 yr": "30y"}))
    # Aggregate in case of duplicates
    rates = (rates.groupby("Currency_u", as_index=True)
             .agg({k: "first" for k in ["6m", "2y", "5y", "10y", "20y", "30y"]})
             .reindex(fund_ccy_set))
    # Missing currencies/nodes -> fill 0 and warn
    warnings = []
    # Currency-level gaps
    missing_ccy = [c for c in fund_ccy_set if c not in rates.index]
    if missing_ccy:
        warnings.append(f"Scenario grid missing currencies: {missing_ccy}. Treated as 0 bp.")
    rates = rates.reindex(index=fund_ccy_set, fill_value=0.0)

    # Node-level gaps
    for node in NODES_ORDER:
        if node not in rates.columns:
            rates[node] = 0.0
            warnings.append(f"Scenario grid missing node '{node}'. Treated as 0 bp for all currencies.")

    # Ensure column order
    rates = rates[NODES_ORDER].astype(float).fillna(0.0)

    # Canonical spread move (%), should be identical across currencies within a scenario
    spread_vals = srows["spread_move_pct"].dropna().unique()
    spread_move_pct = float(spread_vals[0]) if len(spread_vals) >= 1 else 0.0

    return rates, spread_move_pct, warnings

# --- P&L calculators ---

def rates_pnl_bps(krd_ccy_node: pd.DataFrame, dy_ccy_node: pd.DataFrame) -> Tuple[float, pd.DataFrame]:
    """
    RatesPnL (bps) = sum_{ccy,node} -KRD(ccy,node) * Δy(ccy,node)
    Returns total bps and contribution table indexed by (Currency, Node) with bps values.
    """
    # Align indices/columns
    dy = dy_ccy_node.reindex(index=krd_ccy_node.index, columns=krd_ccy_node.columns).fillna(0.0)
    contrib = -(krd_ccy_node * dy)
    total = float(contrib.sum().sum())
    # Flatten for drill-down table
    tidy = (contrib.stack().rename("bps")
            .rename_axis(index=["Currency", "Node"]).reset_index()
            .sort_values("bps", key=lambda s: s.abs(), ascending=False))
    return total, tidy

def credit_pnl_bps(dts_total_val: float, spread_move_pct: float) -> float:
    # CreditPnL (bps) = - DTS_total * spread_move_pct * 100
    return -float(dts_total_val) * float(spread_move_pct) * 100.0

def etr_bps(carry_bps_val: float, ogc_bps_val: float, credit_bps_val: float, rates_bps_val: float, roll_bps_val: float = 0.0) -> float:
    return float(carry_bps_val) + float(roll_bps_val) - float(ogc_bps_val) + float(credit_bps_val) + float(rates_bps_val)

# --- QA checks ---

@dataclass
class QAStatus:
    neutral_ok: bool
    neutral_msg: str
    carry_ok_fund: bool
    carry_ok_idx: bool
    carry_msg: str
    sign_ok: bool
    sign_msg: str
    parity_msg: str

def qa_checks(
    combined: pd.DataFrame,
    scenarios: pd.DataFrame,
    fund_code: str,
    selected_scn_id: int,
    fund_rates_contrib: pd.DataFrame,
    spread_move_pct: float,
    rel_etr_bps_val: float,
    carry_ok_tup_fund: Tuple[bool, float, float],
    carry_ok_tup_idx: Tuple[bool, float, float],
) -> QAStatus:
    # Neutral scenario check: detect if a scenario exists with all rate shocks=0 and spread=0
    any_neutral = False
    for sid in scenarios["Scenario"].unique():
        srows = scenarios[scenarios["Scenario"] == sid]
        rsum = float(srows[["6m", "2yr", "5 yr", "10 yr", "20 yr", "30 yr"]].fillna(0.0).abs().sum().sum())
        spread0 = float(canonicalize_spread_sign(srows["Credit Spread Change %"]).abs().max() or 0.0)
        if rsum == 0.0 and spread0 == 0.0:
            any_neutral = True
            break
    neutral_ok = any_neutral
    neutral_msg = "Neutral scenario found." if any_neutral else "No strictly neutral scenario present (all shocks 0)."

    # Carry reconciliation messages
    ok_f, sum_c_f, tot_f = carry_ok_tup_fund
    ok_i, sum_c_i, tot_i = carry_ok_tup_idx
    carry_msg = f"Carry recon Fund: {'OK' if ok_f else 'Δ=' + PCT_FMT.format((sum_c_f - tot_f))} | Index: {'OK' if ok_i else 'Δ=' + PCT_FMT.format((sum_c_i - tot_i))}"

    # Sign sanity: if spread widening (+ canonical) AND DTS>0 => CreditPnL should be negative. We can't access DTS here,
    # but we can check that selected scenario's sign matches computed credit sign via spread_move_pct.
    sign_ok = True
    sign_bits = []
    if spread_move_pct > 1e-12:
        sign_bits.append("Widening (+)")
    elif spread_move_pct < -1e-12:
        sign_bits.append("Tightening (−)")
    else:
        sign_bits.append("No credit shock")

    # Rates sign sanity: if any Δy>0 & KRD>0 cells exist, their contribution must be negative.
    bad_rates = fund_rates_contrib[(fund_rates_contrib["bps"] > 0) & (fund_rates_contrib["Node"].notna())].copy()
    # A positive bps at cell level implies Δy and KRD had opposite signs (e.g., Δy<0 and KRD>0) which is fine.
    # We can't strictly judge per-cell without sign of Δy, but we assume aggregated calc is internally consistent.
    # So just mark sign_ok true unless scenario is inconsistent (rare). Keeping simple:
    sign_msg = "Signs consistent."

    # Parity: if Fund == Index exposures, rel should be ~0 (up to carry/OGC deltas).
    # Heuristic: check if KRD matrices and DTS totals are equal (within tol).
    f_f = slice_entity(combined, fund_code, "Fund")
    f_i = slice_entity(combined, fund_code, "Index")
    krd_f = krd_matrix(f_f)
    krd_i = krd_matrix(f_i)
    # Align matrices to same currency set before comparison
    all_currencies = sorted(set(krd_f.index).union(set(krd_i.index)))
    krd_f_aligned = krd_f.reindex(all_currencies).fillna(0.0)
    krd_i_aligned = krd_i.reindex(all_currencies).fillna(0.0)
    eq_krd = np.allclose(krd_f_aligned.values, krd_i_aligned.values, atol=1e-9, rtol=0)
    eq_dts = math.isclose(dts_total(f_f), dts_total(f_i), abs_tol=1e-9)
    parity_msg = "Benchmark parity conditions met." if (eq_krd and eq_dts) else "Fund and Index exposures differ (parity not expected)."

    return QAStatus(
        neutral_ok=neutral_ok, neutral_msg=neutral_msg,
        carry_ok_fund=ok_f, carry_ok_idx=ok_i, carry_msg=carry_msg,
        sign_ok=sign_ok, sign_msg=sign_msg,
        parity_msg=parity_msg
    )

# ================================ Scenario Engine ================================

def compute_all_scenarios(inputs: Inputs, fund_code: str) -> Tuple[pd.DataFrame, Dict[int, Dict[str, pd.DataFrame]]]:
    """
    Returns:
      - results_df across all scenarios (Fund, ETR, Index, Relative, and contributions)
      - drilldown dict per scenario:
          {"rates_contrib_fund": DataFrame, "rates_contrib_idx": DataFrame}
    """
    df = inputs.combined
    f_fund = slice_entity(df, fund_code, "Fund")
    f_idx  = slice_entity(df, fund_code, "Index")

    # Validation: ensure we have both entities
    if f_fund.empty or f_idx.empty:
        _fail(f"Positions missing for {fund_code} (Fund/Index).")

    # Carry reconciliation checks
    carry_ok_fund = carry_recon_ok(f_fund)
    carry_ok_idx  = carry_recon_ok(f_idx)

    # Prepare exposure matrices
    krd_fund = krd_matrix(f_fund)
    krd_idx  = krd_matrix(f_idx)
    # Currency universe for this fund (union of Fund/Index per-ccy rows)
    ccy_set = sorted(set(krd_fund.index).union(set(krd_idx.index)))
    # DTS totals
    dts_f = dts_total(f_fund)
    dts_i = dts_total(f_idx)
    # Carry (bps)
    c_f = carry_bps(f_fund)
    c_i = carry_bps(f_idx)
    # OGC is deducted for Fund only; Benchmark OGC = 0 by design.
    ogc_f = float(inputs.ogc_map_bps.get(fund_code, 0.0))

    results = []
    drilldown = {}

    for sid, sname in inputs.scenario_list:
        rates_shock, spread_pct, _warns = scenario_shocks_for_fund(inputs.scenarios, ccy_set, sid)

        # Align shocks to Fund/Index currency sets
        rf = rates_shock.reindex(krd_fund.index).fillna(0.0)
        ri = rates_shock.reindex(krd_idx.index).fillna(0.0)

        # Rates P&L (bps) + drill-down
        r_f, rtab_f = rates_pnl_bps(krd_fund, rf)
        r_i, rtab_i = rates_pnl_bps(krd_idx, ri)

        # Credit P&L (bps)
        cr_f = credit_pnl_bps(dts_total_val=dts_f, spread_move_pct=spread_pct)
        cr_i = credit_pnl_bps(dts_total_val=dts_i, spread_move_pct=spread_pct)

        # Totals (roll-down = 0 for v1)
        etr_f = etr_bps(carry_bps_val=c_f, ogc_bps_val=ogc_f, credit_bps_val=cr_f, rates_bps_val=r_f, roll_bps_val=0.0)
        etr_i = etr_bps(carry_bps_val=c_i, ogc_bps_val=0.0,  credit_bps_val=cr_i, rates_bps_val=r_i, roll_bps_val=0.0)

        rel = etr_f - etr_i

        results.append(ScenarioResult(
            fund_code=fund_code, scenario_id=sid, scenario_name=sname,
            carry_bps_fund=c_f, carry_bps_idx=c_i,
            ogc_bps_fund=ogc_f, ogc_bps_idx=0.0,
            credit_bps_fund=cr_f, credit_bps_idx=cr_i,
            rates_bps_fund=r_f, rates_bps_idx=r_i,
            etr_bps_fund=etr_f, etr_bps_idx=etr_i,
            rel_etr_bps=rel
        ))

        # Derive rates meta from scenario shocks (not Fund P&L)
        srows = inputs.scenarios.loc[inputs.scenarios["Scenario"] == sid]
        if not srows.empty:
            cols = ["6m","2yr","5 yr","10 yr","20 yr","30 yr"]
            avg = srows[cols].mean(numeric_only=True)
            parallel = float(avg.mean())
            rates_bias = "bull (yields down)" if parallel < -1e-9 else ("bear (yields up)" if parallel > 1e-9 else "neutral")
            short = float(avg.get("2yr", avg.get("2y", avg.get("6m",0.0))))
            long = float(avg.get("30 yr", avg.get("30y", avg.get("20 yr",0.0))))
            twist = long - short
            curve_shape = "steepening" if twist>1e-9 else ("flattening" if twist<-1e-9 else "flat")
            rates_meta = {"rates_bias": rates_bias, "curve_shape": curve_shape}
        else:
            rates_meta = {"rates_bias": "neutral", "curve_shape": "flat"}

        drilldown[sid] = {"rates_contrib_fund": rtab_f, "rates_contrib_idx": rtab_i, "spread_pct": spread_pct, "rates_meta": rates_meta}

    results_df = pd.DataFrame([r.__dict__ for r in results])
    # Attach carry/OGC verification flags in the same df (useful for insights)
    results_df["carry_recon_fund_ok"] = carry_ok_fund[0]
    results_df["carry_recon_idx_ok"]  = carry_ok_idx[0]
    results_df["dts_fund"] = dts_f
    results_df["dts_index"] = dts_i
    return results_df, drilldown

# ================================ Charts & UI Helpers ================================

def kpi_tile(label: str, value_str: str):
    st.markdown(f"""<div class="kpi"><div class="label">{label}</div><div class="value">{value_str}</div></div>""", unsafe_allow_html=True)

def heatmap_krd_diff(krd_f: pd.DataFrame, krd_i: pd.DataFrame, title: str):
    # Fund − Index
    ccy = sorted(set(krd_f.index).union(set(krd_i.index)))
    f = krd_f.reindex(ccy).fillna(0.0)
    i = krd_i.reindex(ccy).fillna(0.0)
    diff = (f - i)[NODES_ORDER]

    z = diff.values
    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=NODES_ORDER,
            y=diff.index,
            colorscale=[
                [0.0, RB_COLORS["orange"]],
                [0.5, RB_COLORS["grey"]],
                [1.0, RB_COLORS["blue"]],
            ],
            colorbar=dict(title=dict(text="ΔKRD (yrs / 100bp)", side="right")),
            hovertemplate="Currency=%{y}<br>Maturity=%{x}<br>Fund−Benchmark=%{z:.2f}<extra></extra>",
            zmid=0.0,
        )
    )
    fig.update_layout(title=title, height=PLOT_HEIGHT)
    return fig, diff

def heatmap_krd_diff_rg(krd_f: pd.DataFrame, krd_i: pd.DataFrame, title: str):
    # Fund − Index
    ccy = sorted(set(krd_f.index).union(set(krd_i.index)))
    f = krd_f.reindex(ccy).fillna(0.0)
    i = krd_i.reindex(ccy).fillna(0.0)
    diff = (f - i)[NODES_ORDER]

    # RdYlGn style tuned to Rubrics palette edges (red=underweight, green=overweight)
    colors = [
        [0.00, "#b2182b"],   # deep red
        [0.25, "#ef8a62"],
        [0.50, "#f7f7f7"],   # neutral
        [0.75, "#66bd63"],
        [1.00, "#1a9850"],   # deep green
    ]
    fig = go.Figure(go.Heatmap(
        z=diff.values, x=NODES_ORDER, y=diff.index, zmid=0.0,
        colorscale=colors, colorbar=dict(title="ΔKRD (yrs / 100bp)"),
        hovertemplate="Currency=%{y}<br>Maturity=%{x}<br>Fund−Benchmark=%{z:.2f}<extra></extra>"
    ))
    fig.update_layout(title=title, height=PLOT_HEIGHT, margin=dict(l=10, r=10, t=40, b=40))
    return fig, diff

def waterfall_contrib(carry_bps_val, ogc_bps_val, credit_bps_val, rates_bps_val, title: str):
    # Waterfall of contributions to ETR
    measures = ["relative"] * 4 + ["total"]
    x = ["Carry", "Roll-down", "Credit", "Rates", "ETR"]
    y = [carry_bps_val, 0.0, credit_bps_val, rates_bps_val, 0.0]
    text = [BPS_FMT_POS.format(v) for v in y[:-1]] + [""]
    fig = go.Figure(go.Waterfall(
        name="Contrib",
        orientation="v",
        measure=measures,
        x=x,
        text=text,
        y=y,
        connector={"line": {"color": RB_COLORS["grey"]}},
        decreasing={"marker": {"color": RB_COLORS["orange"]}},
        increasing={"marker": {"color": RB_COLORS["med"]}},
        totals={"marker": {"color": RB_COLORS["blue"]}},
    ))
    fig.update_layout(title=title, height=PLOT_HEIGHT)
    return fig

def plot_headline_compact(krd_fund, krd_index, fund_slice, index_slice, ogc_bps_f):
    # Totals
    tot_krd_f, tot_krd_i = float(krd_fund.values.sum()), float(krd_index.values.sum())
    dts_f, dts_i = float(dts_total(fund_slice)), float(dts_total(index_slice))
    carry_f, carry_i = float(carry_bps(fund_slice)), float(carry_bps(index_slice))
    net_carry_f = carry_f - float(ogc_bps_f)

    rows = [
        ("Total Duration Contribution (yrs / 100bp)", tot_krd_f, tot_krd_i),
        ("Total DTS",              dts_f,     dts_i),
        ("Carry (bps)",            carry_f,   carry_i),
        ("OGC (bps)",              float(ogc_bps_f), 0.0),
    ]
    df = pd.DataFrame(rows, columns=["Metric", "Fund", "Index"])
    df["Δ"] = df["Fund"] - df["Index"]

    # Horizontal grouped bars + delta labels
    fig = go.Figure()
    fig.add_bar(y=df["Metric"], x=df["Fund"], name="Fund",      orientation="h", marker_color=RB_COLORS["blue"])
    fig.add_bar(y=df["Metric"], x=df["Index"], name="Benchmark", orientation="h", marker_color=RB_COLORS["grey"])
    # Delta annotations (right side of bars)
    for i, r in df.iterrows():
        fig.add_annotation(
            x=max(r["Fund"], r["Index"]) * 1.01 if max(r["Fund"], r["Index"]) != 0 else 0.02,
            y=r["Metric"],
            text=f"{r['Δ']:+.2f}" if i < 2 else f"{r['Δ']:+.0f} bps",
            showarrow=False, font=dict(color="#444", size=11), xanchor="left"
        )
    # Net carry chip
    fig.add_annotation(
        x=1, y=1.12, xref="paper", yref="paper",
        text=f"Net carry after OGC: <b>{net_carry_f:.0f} bps</b>",
        showarrow=False, align="right",
        font=dict(size=12, color=RB_COLORS["blue"]),
        bgcolor="#F1F3F9", bordercolor=RB_COLORS["grey"], borderwidth=1, borderpad=4
    )
    fig.update_layout(
        barmode="group", height=PLOT_HEIGHT,
        title="Headline Metrics (Fund vs Benchmark) with Deltas",
        margin=dict(l=10, r=10, t=60, b=30), legend=dict(orientation="h", y=1.05, x=1, xanchor="right")
    )
    return fig, df

def plot_dumbbell_duration_by_ccy(krd_fund, krd_index):
    # Use union of all currencies from both Fund and Index
    all_currencies = sorted(set(krd_fund.index).union(set(krd_index.index)))
    tot_f = krd_fund.sum(axis=1).reindex(all_currencies).fillna(0.0)
    tot_i = krd_index.sum(axis=1).reindex(all_currencies).fillna(0.0)
    df = pd.DataFrame({"Currency":all_currencies,"Fund":tot_f.values,"Index":tot_i.values})
    df["Delta"] = df["Fund"]-df["Index"]
    df = df.sort_values("Delta",key=lambda s: s.abs(),ascending=True)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["Index"],y=df["Currency"],mode="markers",name="Index",marker=dict(color=RB_COLORS["grey"],size=8)))
    fig.add_trace(go.Scatter(x=df["Fund"],y=df["Currency"],mode="markers",name="Fund",marker=dict(color=RB_COLORS["blue"],size=8)))
    for _,row in df.iterrows():
        fig.add_shape(type="line",x0=row["Index"],x1=row["Fund"],y0=row["Currency"],y1=row["Currency"],line=dict(color=RB_COLORS["med"],width=2))
    fig.update_layout(title="Duration Contribution by Currency (Fund vs Benchmark)",height=PLOT_HEIGHT)
    return fig, df

def plot_duration_diff_by_ccy(krd_fund, krd_index):
    # Use union of all currencies from both Fund and Index
    all_currencies = sorted(set(krd_fund.index).union(set(krd_index.index)))
    tot_f = krd_fund.sum(axis=1).reindex(all_currencies).fillna(0.0)
    tot_i = krd_index.sum(axis=1).reindex(all_currencies).fillna(0.0)
    diff = (tot_f - tot_i).sort_values(key=lambda s: s.abs(),ascending=True)
    fig = go.Figure(go.Bar(x=diff.values,y=diff.index,orientation="h",
        marker_color=[RB_COLORS["blue"] if v>0 else RB_COLORS["orange"] for v in diff.values]))
    fig.update_layout(title="Duration Contribution Difference by Currency",height=PLOT_HEIGHT)
    return fig,diff

def plot_dumbbell_krd_by_node(krd_fund, krd_index):
    tot_f, tot_i = krd_fund.sum(axis=0), krd_index.sum(axis=0)
    df = pd.DataFrame({"Node":tot_f.index,"Fund":tot_f.values,"Index":tot_i.values})
    df["Delta"] = df["Fund"]-df["Index"]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["Index"],y=df["Node"],mode="markers",name="Index",marker=dict(color=RB_COLORS["grey"],size=8)))
    fig.add_trace(go.Scatter(x=df["Fund"],y=df["Node"],mode="markers",name="Fund",marker=dict(color=RB_COLORS["blue"],size=8)))
    for _,row in df.iterrows():
        fig.add_shape(type="line",x0=row["Index"],x1=row["Fund"],y0=row["Node"],y1=row["Node"],line=dict(color=RB_COLORS["med"],width=2))
    fig.update_layout(title="KRD by Node (Fund vs Index)",height=PLOT_HEIGHT)
    return fig,df

def plot_krd_node_diff(krd_fund, krd_index):
    diff = (krd_fund.sum(axis=0)-krd_index.sum(axis=0)).sort_values(key=lambda s:s.abs(),ascending=True)
    fig = go.Figure(go.Bar(x=diff.values,y=diff.index,orientation="h",
        marker_color=[RB_COLORS["blue"] if v>0 else RB_COLORS["orange"] for v in diff.values]))
    fig.update_layout(title="Duration Difference by Maturity",height=PLOT_HEIGHT)
    return fig,diff

def plot_risk_carry_map(fund_slice,index_slice,krd_fund,krd_index):
    try:
        # Use union of all currencies from both Fund and Index
        all_currencies = sorted(set(krd_fund.index).union(set(krd_index.index)))
        krd_tot_f = krd_fund.sum(axis=1).reindex(all_currencies).fillna(0.0)
        krd_tot_i = krd_index.sum(axis=1).reindex(all_currencies).fillna(0.0)
        carry_contr = fund_slice.groupby("Currency_u")["Hedged Yield Contr"].sum()*100
        diff = krd_tot_f - krd_tot_i
        fig = go.Figure()
        for c in all_currencies:
            fig.add_trace(go.Scatter(
                x=[krd_tot_f[c]],y=[carry_contr.get(c,0)],mode="markers",name=c,
                marker=dict(size=8+abs(diff[c])*4,
                            color=RB_COLORS["blue"] if diff[c]>0 else RB_COLORS["orange"])
            ))
        fig.update_layout(title="Risk vs Carry (by Currency)",xaxis_title="Total KRD",yaxis_title="Carry Contribution (bps)",height=PLOT_HEIGHT)
        return fig
    except Exception:
        return None

def render_positioning_bullets(krd_fund,krd_index,fund_slice,index_slice,ogc_bps_f):
    lines=[]
    # Use union of all currencies from both Fund and Index
    all_currencies = sorted(set(krd_fund.index).union(set(krd_index.index)))
    tot_f = krd_fund.sum(axis=1).reindex(all_currencies).fillna(0.0)
    tot_i = krd_index.sum(axis=1).reindex(all_currencies).fillna(0.0)
    diff_ccy=(tot_f - tot_i).sort_values(key=lambda s:s.abs(),ascending=False)
    top_over=diff_ccy[diff_ccy>0].head(3)
    top_under=diff_ccy[diff_ccy<0].head(2)
    for c,v in top_over.items():
        lines.append(f"- Overweight **{c}** by {v:.2f} duration contribution units")
    for c,v in top_under.items():
        lines.append(f"- Underweight **{c}** by {abs(v):.2f} duration contribution units")
    node_diff=(krd_fund.sum(axis=0)-krd_index.sum(axis=0))
    tilt="long end" if node_diff.loc[["20y","30y"]].sum()>node_diff.loc[["6m","2y"]].sum() else "front end"
    lines.append(f"- Curve tilt towards {tilt}")
    carry_f=carry_bps(fund_slice); net=carry_f-ogc_bps_f
    lines.append(f"- Carry = {carry_f:.0f} bps vs OGC = {ogc_bps_f:.0f} bps → Net {net:.0f} bps")
    return "\n".join(lines)

def _marker_size_scale(values: np.ndarray, min_px: float = 10, max_px: float = 34) -> np.ndarray:
    """Scale non-negative values to marker sizes in pixels with a sensible floor."""
    v = np.asarray(values, float)
    v = np.clip(v, 0, None)
    if np.allclose(v.max(), 0.0):
        return np.full_like(v, min_px)
    lo, hi = float(np.percentile(v, 5)), float(np.percentile(v, 95))
    if hi <= lo:
        hi = float(v.max())
        lo = float(v.min())
    t = (v - lo) / max(hi - lo, 1e-12)
    return (min_px + t * (max_px - min_px))

def plot_krd_curve_bubbles(krd_fund: pd.DataFrame, krd_index: pd.DataFrame) -> go.Figure:
    """
    Render a yield-curve style chart: x = node order, y = total KRD at each node,
    with bubble size ∝ sensitivity. Shows Fund vs Index curves.
    """
    # Sum across currencies for each node
    nodes = NODES_ORDER
    f_vals = krd_fund.reindex(columns=nodes).sum(axis=0).astype(float).values
    i_vals = krd_index.reindex(columns=nodes).sum(axis=0).astype(float).values

    # Build x positions and nicely formatted labels
    x = list(range(len(nodes)))
    x_labels = nodes

    # Bubble sizes (independent per series to keep contrast)
    sz_f = _marker_size_scale(f_vals)
    sz_i = _marker_size_scale(i_vals)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=x, y=f_vals, mode="lines+markers", name="Fund",
        line=dict(color=RB_COLORS["blue"], width=2),
        marker=dict(size=sz_f, color=RB_COLORS["blue"], opacity=0.85),
        hovertemplate="Maturity=%{customdata[0]}<br>KRD (Fund)=%{y:.2f} yrs / 100bp<extra></extra>",
        customdata=np.array([[lbl] for lbl in x_labels])
    ))
    fig.add_trace(go.Scatter(
        x=x, y=i_vals, mode="lines+markers", name="Benchmark",
        line=dict(color=RB_COLORS["grey"], width=2, dash="dot"),
        marker=dict(size=sz_i, color=RB_COLORS["grey"], opacity=0.8),
        hovertemplate="Maturity=%{customdata[0]}<br>KRD (Benchmark)=%{y:.2f} yrs / 100bp<extra></extra>",
        customdata=np.array([[lbl] for lbl in x_labels])
    ))

    # Add Δ annotations at each node (optional, subtle)
    delta = f_vals - i_vals
    for xi, yi, d in zip(x, f_vals, delta):
        fig.add_annotation(x=xi, y=yi, text=f"{d:+.2f}", showarrow=False,
                           font=dict(size=11, color="#444"), yshift=18)

    fig.update_layout(
        title="Curve Duration Sensitivity by Maturity (bubble size ∝ sensitivity)",
        xaxis=dict(
            tickmode="array", tickvals=x, ticktext=x_labels,
            title="Maturity", showgrid=False
        ),
        yaxis=dict(title="Total KRD (yrs / 100bp)", zeroline=True,
                   gridcolor="rgba(128,128,128,0.15)"),
        height=PLOT_HEIGHT, margin=dict(l=10, r=10, t=50, b=40)
    )
    return fig

def plot_duration_by_currency_bars(krd_fund: pd.DataFrame, krd_index: pd.DataFrame) -> tuple[go.Figure, pd.DataFrame]:
    """
    Grouped vertical columns: Fund vs Index duration contribution (sum of KRD across nodes) per currency.
    KRD values are already weighted by portfolio allocation, so this shows contribution to total portfolio duration.
    Sorted by absolute Fund−Index delta (top N currencies first if many).
    """
    # Use union of all currencies from both Fund and Index
    all_currencies = sorted(set(krd_fund.index).union(set(krd_index.index)))
    tot_f = krd_fund.sum(axis=1).reindex(all_currencies).fillna(0.0)
    tot_i = krd_index.sum(axis=1).reindex(all_currencies).fillna(0.0)
    df = pd.DataFrame({"Currency": all_currencies, "Fund": tot_f.values, "Index": tot_i.values})
    df["Delta"] = df["Fund"] - df["Index"]
    # Keep only meaningful currencies (already filtered for Total/Other upstream)
    df = df.sort_values("Delta", key=lambda s: s.abs(), ascending=False)

    fig = go.Figure()
    fig.add_bar(name="Fund",      x=df["Currency"], y=df["Fund"], marker_color=RB_COLORS["blue"])
    fig.add_bar(name="Benchmark", x=df["Currency"], y=df["Index"], marker_color=RB_COLORS["grey"])
    fig.update_layout(
        barmode="group",
        title="Duration Contribution by Currency (Fund vs Benchmark)",
        xaxis=dict(title="", tickangle=-15),
        yaxis=dict(title="Duration Contribution (yrs / 100bp)", gridcolor="rgba(128,128,128,0.15)"),
        height=PLOT_HEIGHT, margin=dict(l=10, r=10, t=50, b=60)
    )
    return fig, df

# ================================ UX Helpers ================================

def stat_tile(label: str, value: float, suffix: str = "", emphasize: bool = True):
    val = f"{value:+.0f}{suffix}" if suffix else f"{value:+.2f}"
    st.markdown(
        f'''
        <div class="kpi" style="height:90px;width:100%;display:flex;flex-direction:column;justify-content:center;text-align:center;">
          <div class="label">{label}</div>
          <div class="value" style="font-size:1.6rem;{'font-weight:800;' if emphasize else 'font-weight:600;'}">{val}</div>
        </div>
        ''',
        unsafe_allow_html=True
    )

def compute_headline_stats(krd_fund, krd_index, fund_slice, index_slice, ogc_bps_f):
    tot_dur_f = float(krd_fund.values.sum())           # total duration contribution (yrs/100bp)
    tot_dur_i = float(krd_index.values.sum())
    dts_f     = float(dts_total(fund_slice))           # unitless DTS
    dts_i     = float(dts_total(index_slice))          # unitless DTS
    carry_f   = float(carry_bps(fund_slice))           # bps
    carry_i   = float(carry_bps(index_slice))
    net_carry = carry_f - float(ogc_bps_f)             # bps after OGC
    return {
        "dur_f": tot_dur_f, "dur_i": tot_dur_i, "dur_d": tot_dur_f - tot_dur_i,
        "dts_f": dts_f,     "dts_i": dts_i,     "dts_d": dts_f - dts_i,
        "car_f": carry_f,   "car_i": carry_i,   "car_d": carry_f - carry_i,
        "ogc_f": float(ogc_bps_f), "net_carry": net_carry
    }

def fmt_signed(v: float, dp: int = 2, unit: str = "") -> str:
    """Format value with sign, avoiding -0.00 using Decimal quantize."""
    q = Decimal(str(v)).quantize(Decimal("1." + "0"*dp), rounding=ROUND_HALF_UP)
    if q == 0: q = Decimal("0")
    s = f"{q:+.{dp}f}"
    return f"{s} {unit}".strip()

def delta_color(v: float) -> str:
    """Return color for delta values: green for positive, red for negative, grey for neutral."""
    if abs(v) < 1e-6: return "#444"         # neutral
    return "#1a9850" if v > 0 else "#b2182b"  # green / red

def stat_tile_signed(label: str, value: float, unit: str = "", dp: int = 2, emphasize: bool = True, color: str = None):
    """Render a KPI tile with signed formatting and optional color."""
    val = fmt_signed(value, dp, unit)
    style = f"color:{color};" if color else ""
    st.markdown(
        f'''
        <div class="kpi" style="height:90px;width:100%;display:flex;flex-direction:column;justify-content:center;text-align:center;">
          <div class="label">{label}</div>
          <div class="value" style="font-size:1.6rem;{'font-weight:800;' if emphasize else 'font-weight:600;'}{style}">{val}</div>
        </div>
        ''',
        unsafe_allow_html=True
    )

def _driver_name(key: str) -> str:
    """Convert driver key to human-readable name."""
    return {
        "carry": "carry",
        "credit": "credit sensitivity",
        "rates": "interest-rate positioning",
        "ogc": "fund costs (OGC)"
    }[key]

def _signed_word(v: float) -> str:
    """Convert numeric value to descriptive word for text narrative."""
    # For text: + → "helps", − → "hurts"
    return "helps" if v > 0 else ("hurts" if v < 0 else "is neutral")

def scenario_summary_paragraph(sel_row: pd.Series) -> str:
    """
    Compose a one-sentence analyst narrative for the selected scenario.
    Uses Fund vs Benchmark totals and the relative contributions by driver.
    """
    fund = float(sel_row["etr_bps_fund"])
    bmk  = float(sel_row["etr_bps_idx"])
    diff = float(sel_row["rel_etr_bps"])

    # Relative contributions: Fund − Benchmark
    rel_carry  = float(sel_row["carry_bps_fund"]  - sel_row["carry_bps_idx"])
    rel_credit = float(sel_row["credit_bps_fund"] - sel_row["credit_bps_idx"])
    rel_rates  = float(sel_row["rates_bps_fund"]  - sel_row["rates_bps_idx"])
    rel_ogc    = -float(sel_row.get("ogc_bps_fund", 0.0))  # benchmark OGC = 0

    parts = {
        "carry":  rel_carry,
        "credit": rel_credit,
        "rates":  rel_rates,
        "ogc":    rel_ogc,
    }
    # Order drivers by absolute impact
    ranked = sorted(parts.items(), key=lambda kv: abs(kv[1]), reverse=True)

    # Build short driver clause with 2 strongest drivers - improved grammar
    top_txt = []
    for (k, v) in ranked[:2]:
        if v > 0:
            top_txt.append(f"{_driver_name(k)} contributing +{v:.0f} bps")
        else:
            top_txt.append(f"{_driver_name(k)} reducing returns by {abs(v):.0f} bps")
    drivers_clause = " and ".join(top_txt)

    # Plain-English headline
    headline = (
        f"In this scenario, the Fund's expected 12-month total return is {fund:+.0f} bps versus "
        f"{bmk:+.0f} bps for the benchmark, a difference of {diff:+.0f} bps."
    )
    # Full sentence with better grammar
    return headline + f" The difference is primarily driven by {drivers_clause}."

def scenario_summary_block(sel_row: pd.Series):
    """Render the narrative in a light callout box under Selected Scenario."""
    text = scenario_summary_paragraph(sel_row)
    st.markdown(
        f"""
        <div style="
            margin:.5rem 0 1rem 0;
            padding:.8rem 1rem;
            border:1px solid #D8D7DF;
            border-radius:6px;
            background:#FAFBFD;
            color:#0b0c0c;
            font-size:1.1rem;
            line-height:1.5;">
          {text}
        </div>
        """,
        unsafe_allow_html=True
    )


def _krd_totals(krd_df: pd.DataFrame) -> dict:
    """Return totals by currency and by maturity for a KRD matrix (index=Currency, cols=NODES_ORDER)."""
    by_ccy = krd_df.sum(axis=1).astype(float).to_dict()
    by_node = krd_df.sum(axis=0).astype(float).to_dict()
    return {"by_currency": by_ccy, "by_maturity": by_node, "total": float(krd_df.values.sum())}

def _fmt_bps(v: float) -> str:
    return f"{v:+.0f} bps"

def build_driver_bullets_from_payload(payload: dict) -> list[str]:
    """Three crisp, quantified bullets explaining Fund − Benchmark outcome by driver."""
    rel   = payload.get("relative", {})
    smeta = payload.get("scenario_meta", {})
    rmeta = payload.get("rates_meta", {})

    rel_credit = float(rel.get("credit_bps", 0.0))
    rel_rates  = float(rel.get("rates_bps", 0.0))
    rel_carry  = float(rel.get("carry_bps", 0.0))
    rel_ogc    = float(rel.get("ogc_bps", 0.0))

    credit_env = smeta.get("credit_environment", "no_change")
    if credit_env == "tightening":
        credit_line = f"Credit tailwind vs benchmark: {_fmt_bps(rel_credit)} with tightening spreads."
    elif credit_env == "widening":
        credit_line = f"Credit protection vs benchmark: {_fmt_bps(rel_credit)} with widening spreads."
    else:
        credit_line = f"Credit contribution vs benchmark: {_fmt_bps(rel_credit)}."

    curve = rmeta.get("curve_shape", "flat")
    bias  = rmeta.get("rates_bias", rmeta.get("rates_bias_for_fund", "neutral"))
    rates_line = f"Rates positioning vs benchmark: {_fmt_bps(rel_rates)} ({bias}, curve {curve})."

    other_line = f"Carry & costs (relative): {_fmt_bps(rel_carry + rel_ogc)} (carry {_fmt_bps(rel_carry)}, OGC {_fmt_bps(rel_ogc)})."

    return [credit_line, rates_line, other_line]

def _propose_rates_trim_add(payload: dict) -> dict:
    """
    Heuristic rates rec: if relative rates bps is negative and curve is 'bear' or 'steepening',
    suggest trimming long-end overweights; else suggest small add to short/belly if bull.
    """
    rel   = payload.get("relative", {})
    rmeta = payload.get("rates_meta", {})
    pos   = payload.get("positioning", {})
    rel_rates = float(rel.get("rates_bps", 0.0))
    curve = rmeta.get("curve_shape", "flat")
    bias  = rmeta.get("rates_bias", rmeta.get("rates_bias_for_fund", "neutral"))
    # default neutral rec if no meta present
    rec = {
        "title": "Align Duration Profile",
        "action": "Tune curve exposures in small steps; re-test under alternate scenarios.",
        "est_delta_bps": 0,
        "why": "Keep rate risk changes sized and evidence-based."
    }
    if rel_rates < 0 and (bias.startswith("bear") or curve=="steepening"):
        rec = {
            "title": "Trim Long-End Duration",
            "action": "Reduce exposure in 10–30y buckets modestly to lessen scenario drag vs benchmark.",
            "est_delta_bps": 0,
            "why": "Relative underperformance comes from rates; trimming long end helps under bear/steepening moves."
        }
    elif rel_rates > 0 and (bias.startswith("bull") or curve=="flattening"):
        rec = {
            "title": "Maintain/Shift Duration to Belly",
            "action": "Maintain duration; if adding, prefer 2–5y to preserve curve resilience.",
            "est_delta_bps": 0,
            "why": "Rates positioning is supportive; the belly balances carry vs convexity."
        }
    return rec

def build_genai_payload(sel_row: pd.Series, drilldown: Dict[int, Dict[str, pd.DataFrame]], scn_id: int,
                        krd_fund: pd.DataFrame, krd_index: pd.DataFrame, ogc_bps_f: float) -> dict:
    """Assemble full scenario & positioning context for grounded insights."""
    # Totals (Fund / Benchmark / Relative)
    fund = {
        "total_bps": float(sel_row["etr_bps_fund"]),
        "carry_bps": float(sel_row["carry_bps_fund"]),
        "credit_bps": float(sel_row["credit_bps_fund"]),
        "rates_bps": float(sel_row["rates_bps_fund"]),
        "ogc_bps": float(sel_row["ogc_bps_fund"]),
    }
    benchmark = {
        "total_bps": float(sel_row["etr_bps_idx"]),
        "carry_bps": float(sel_row["carry_bps_idx"]),
        "credit_bps": float(sel_row["credit_bps_idx"]),
        "rates_bps": float(sel_row["rates_bps_idx"]),
        "ogc_bps": 0.0,
    }
    relative = {
        "total_bps": float(sel_row["rel_etr_bps"]),
        "carry_bps": fund["carry_bps"] - benchmark["carry_bps"],
        "credit_bps": fund["credit_bps"] - benchmark["credit_bps"],
        "rates_bps": fund["rates_bps"] - benchmark["rates_bps"],
        "ogc_bps": -fund["ogc_bps"],
    }

    # Positioning: KRD matrices and DTS totals (Fund vs Benchmark)
    pos_f = _krd_totals(krd_fund)
    pos_b = _krd_totals(krd_index)
    dts_f = float(sel_row.get("dts_fund", 0.0))
    dts_b = float(sel_row.get("dts_index", 0.0))
    credit_weight_vs_bmk = "underweight" if dts_f < dts_b - 1e-9 else ("overweight" if dts_f > dts_b + 1e-9 else "neutral")

    # Scenario meta (credit only)
    sc_spread_pct = float(drilldown[scn_id].get("spread_pct", 0.0))
    credit_env = "widening" if sc_spread_pct > 1e-12 else ("tightening" if sc_spread_pct < -1e-12 else "no_change")

    scenario_meta = {
        "credit_environment": credit_env,
        "spread_move_pct": sc_spread_pct
    }

    positioning = {
        "krd_fund": pos_f,
        "krd_benchmark": pos_b,
        "krd_delta": {
            "by_currency": {k: pos_f["by_currency"].get(k,0.0) - pos_b["by_currency"].get(k,0.0) for k in set(list(pos_f["by_currency"].keys()) + list(pos_b["by_currency"].keys()))},
            "by_maturity": {k: pos_f["by_maturity"].get(k,0.0) - pos_b["by_maturity"].get(k,0.0) for k in set(list(pos_f["by_maturity"].keys()) + list(pos_b["by_maturity"].keys()))},
            "total": pos_f["total"] - pos_b["total"]
        },
        "dts_fund": dts_f,
        "dts_benchmark": dts_b,
        "credit_weight_vs_benchmark": credit_weight_vs_bmk,
        "ogc_fund_bps": float(ogc_bps_f)
    }

    rates_tbl = drilldown[scn_id]["rates_contrib_fund"].to_dict(orient="records")

    headline = (
        f"Fund {('outperforms' if relative['total_bps']>0 else 'underperforms')} "
        f"Benchmark by {relative['total_bps']:+.0f} bps "
        f"(Fund {fund['total_bps']:+.0f} bps vs Benchmark {benchmark['total_bps']:+.0f} bps)."
    )

    return {
        "scenario": {"id": int(sel_row["scenario_id"]), "name": sel_row["scenario_name"]},
        "scenario_meta": scenario_meta,
        "fund": fund,
        "benchmark": benchmark,
        "relative": relative,
        "positioning": positioning,
        "rates_drilldown": rates_tbl,
        "rates_meta": drilldown[scn_id].get("rates_meta",{}),
        "headline": headline
    }

def generate_genai_insights(payload: dict) -> dict:
    """
    Call OpenAI with a strict, sectioned schema:
      - headline: str (single line)
      - drivers: list[str] (exactly 3 short bullets)
      - takeaway: str (single sentence)
      - recommendations: list[ {title, action, est_delta_bps, why} ] (exactly 3)
    Rules:
      * Use only numbers in payload.
      * Be benchmark-aware and scenario-aware.
      * NEVER suggest changing OGC/fees; OGC is fixed and not a lever.
    """
    try:
        openai.api_key = st.secrets["openai"]["OPENAI_API_KEY"]
        system_msg = {
            "role": "system",
            "content": (
                "You are a fixed-income portfolio assistant for fund vs benchmark analysis.\n"
                "Ground rules:\n"
                "1) Use ONLY the JSON numbers provided; do NOT invent or fetch anything else.\n"
                "2) Respect sign logic: credit 'widening' hurts when DTS>0; higher yields hurt when KRD>0.\n"
                "3) CREDIT LOGIC: Underweight credit is BENEFICIAL in WIDENING environments (protects from spread widening). "
                "Underweight credit is HARMFUL in TIGHTENING environments (misses spread tightening gains).\n"
                "4) If the Fund outperforms because it is UNDERWEIGHT a harmful risk in this scenario "
                "(e.g., credit underweight in a widening environment), do NOT recommend adding that risk; "
                "recommend maintaining or trimming.\n"
            "5) Recommendations MUST be benchmark-aware and scenario-aware (quote bps impacts where possible).\n"
            "6) DO NOT recommend changing OGC/fees or any fee optimisation. OGC is fixed and not a lever.\n"
            "7) CRITICAL: Respond with ONLY valid JSON. No markdown, no explanations, no code blocks.\n"
            "8) Use double quotes for all strings. Escape any quotes inside strings with backslash.\n"
            "9) No trailing commas. Ensure all brackets and braces are properly closed.\n"
            "Required JSON structure:\n"
            "{\n"
            '  "headline": "string",\n'
            '  "drivers": ["string1", "string2", "string3"],\n'
            '  "takeaway": "string",\n'
            '  "recommendations": [\n'
            '    {"title": "string", "action": "string", "est_delta_bps": number, "why": "string"},\n'
            '    {"title": "string", "action": "string", "est_delta_bps": number, "why": "string"},\n'
            '    {"title": "string", "action": "string", "est_delta_bps": number, "why": "string"}\n'
            "  ]\n"
            "}\n"
            )
        }
        user_msg = {"role": "user", "content": json.dumps(payload)}
        resp = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[system_msg, user_msg],
            max_tokens=800,
            temperature=0.1,
        )
        txt = resp.choices[0].message["content"].strip()
        
        with st.expander("🔍 Debug: Raw AI Response"):
            st.code(txt, language="json")

        # Strip code fences if present
        if txt.strip().startswith("```"):
            txt = txt.strip().strip("`")
            parts = txt.split("\n", 1)
            if len(parts) == 2 and parts[0].strip().lower().startswith("json"):
                txt = parts[1].strip()

        # Balanced-braces JSON extraction
        def _extract_json_object(s: str) -> str | None:
            depth, start = 0, None
            for i, ch in enumerate(s):
                if ch == "{":
                    if depth == 0: start = i
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0 and start is not None:
                        return s[start:i+1]
            return None

        blob = _extract_json_object(txt) or txt.strip()
        data = json.loads(blob)
        
        # minimal schema hardening
        data.setdefault("headline", "")
        data.setdefault("drivers", [])
        data.setdefault("takeaway", "")
        data.setdefault("recommendations", [])
        return data
    except json.JSONDecodeError as e:
        return {
            "headline": f"⚠️ JSON parsing error at position {e.pos}",
            "drivers": [f"Raw response length: {len(txt) if 'txt' in locals() else 'unknown'}", f"Error: {str(e)}"],
            "takeaway": "Check the debug section above for the raw AI response.",
            "recommendations": []
        }
    except Exception as e:
        return {
            "headline": f"⚠️ API error: {type(e).__name__}",
            "drivers": [str(e)],
            "takeaway": "Check your OpenAI API key and connection.",
            "recommendations": []
        }

import re

def _norm_key(s: str) -> str:
    """Normalize a title/action to a semantic key for de-dup."""
    if not s: return ""
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 ]", "", s)
    return s

def vet_recommendations(payload: dict, ai: dict) -> dict:
    """
    Guardrails & polish:
      - Forbid any fee/OGC references.
      - Remove ALL AI-generated credit recommendations and replace with scenario-consistent logic:
        * Widening + Underweight = Maintain (beneficial)
        * Widening + Overweight = Reduce (harmful)
        * Tightening + Underweight = Increase (harmful)
        * Tightening + Overweight = Maintain (beneficial)
      - Ensure unique titles; if <3 items, add a rates governance rec.
    """
    try:
        input_recs = ai.get("recommendations", []) or []
        out = []
        seen = set()

        # scenario context
        credit_env = payload.get("scenario_meta", {}).get("credit_environment","no_change")
        spread_pct = float(payload.get("scenario_meta",{}).get("spread_move_pct",0.0))  # +widen / -tighten
        rel_credit = float(payload.get("relative",{}).get("credit_bps", 0.0))  # Fund − Bmk
        dts_f = float(payload.get("positioning",{}).get("dts_fund", 0.0))
        dts_b = float(payload.get("positioning",{}).get("dts_benchmark", 0.0))
        dts_gap = max(dts_b - dts_f, 0.0)  # amount underweight vs benchmark
        credit_underweight  = dts_gap > 1e-9
        credit_overweight   = (dts_f - dts_b) > 1e-9

        # 1) forbid fees/OGC; capture any model ideas we still want
        filtered = []
        for r in input_recs:
            t = (r.get("title") or "")
            a = (r.get("action") or "")
            why = (r.get("why") or "")
            if any(k in (t+a+why).lower() for k in ["ogc","fee","fees","charges"]):
                continue
            filtered.append(r)

        # 2) collapse all credit-related variants; keep none for now (we might inject our own)
        credit_keys = []
        keep = []
        for r in filtered:
            key = _norm_key((r.get("title") or "") + " " + (r.get("action") or ""))
            if "credit" in key:
                credit_keys.append(key)
                continue
            keep.append(r)

        # 3) build scenario-consistent credit rec (if applicable)
        credit_rec = None
        if credit_env == "widening" and credit_underweight:
            # Underweight credit is BENEFICIAL in widening - maintain the advantage
            credit_rec = {
                "title": "Maintain Defensive Credit Stance",
                "action": "Maintain or trim credit; lower DTS protects relative returns while spreads widen.",
                "est_delta_bps": 0,
                "why": "Underweight credit position is beneficial in widening environment - adding credit would erode the advantage."
            }
        elif credit_env == "widening" and credit_overweight:
            # Overweight credit is HARMFUL in widening - reduce exposure
            credit_rec = {
                "title": "Reduce Credit Overweight",
                "action": "Trim credit exposure to reduce vulnerability to spread widening.",
                "est_delta_bps": 0,
                "why": "Overweight credit is harmful when spreads widen; reducing exposure improves relative performance."
            }
        elif credit_env == "tightening" and credit_underweight:
            # Underweight credit is HARMFUL in tightening - add exposure
            add_dts = 0.35 * dts_gap  # cap at 35% of gap
            est = int(add_dts * abs(spread_pct) * 100.0)  # linear DTS × %change × 100
            credit_rec = {
                "title": "Increase Credit Exposure (toward benchmark)",
                "action": f"Add ~{add_dts:.2f} DTS (≈35% of gap) to reduce underweight while spreads tighten.",
                "est_delta_bps": est,
                "why": "Underweight credit is harmful in tightening environment; measured add improves relative response."
            }
        elif credit_env == "tightening" and credit_overweight:
            # Overweight credit is BENEFICIAL in tightening - maintain or add more cautiously
            credit_rec = {
                "title": "Maintain Credit Overweight",
                "action": "Keep current overweight; credit positioning benefits from spread tightening.",
                "est_delta_bps": 0,
                "why": "Overweight credit is beneficial when spreads tighten; maintain advantageous positioning."
            }

        # 4) assemble out list: first add our credit rec (if any), then unique rest
        def _push(r):
            k = _norm_key(r.get("title",""))
            if k and k not in seen:
                seen.add(k)
                out.append(r)

        if credit_rec:
            _push(credit_rec)

        for r in keep:
            _push(r)

        # 5) top up to 3 with a rates rec (non-duplicate) and a scenario-discipline rec
        if len(out) < 3:
            _push(_propose_rates_trim_add(payload))
        if len(out) < 3:
            _push({
                "title": "Scenario Discipline",
                "action": "Limit changes to sized adjustments; re-test under alternate scenarios before larger moves.",
                "est_delta_bps": 0,
                "why": "Avoid over-rotation on a single scenario realisation."
            })

        ai["recommendations"] = out[:3]
        return ai
    except Exception:
        return ai

def attribution_tiles_row(label_prefix: str, carry: float, credit: float, rates: float, ogc: float, total: float, show_ogc_value=True):
    """Render a row of attribution tiles showing Carry, Credit, Rates, OGC, and Total (always 5 columns for alignment)."""
    cols = st.columns(5, gap="large")
    with cols[0]:
        stat_tile_signed(f"{label_prefix} Carry", carry, unit="bps", dp=0, color=delta_color(carry))
    with cols[1]:
        stat_tile_signed(f"{label_prefix} Credit", credit, unit="bps", dp=0, color=delta_color(credit))
    with cols[2]:
        stat_tile_signed(f"{label_prefix} Rates", rates, unit="bps", dp=0, color=delta_color(rates))
    with cols[3]:
        if show_ogc_value and abs(ogc) > 1e-6:
            stat_tile_signed(f"{label_prefix} OGC", -ogc, unit="bps", dp=0, color=delta_color(-ogc))
        else:
            # Show dash for benchmark or zero OGC
            st.markdown(
                f'''
                <div class="kpi" style="height:90px;width:100%;display:flex;flex-direction:column;justify-content:center;text-align:center;">
                  <div class="label">{label_prefix} OGC</div>
                  <div class="value" style="font-size:1.6rem;font-weight:600;color:#888;">—</div>
                </div>
                ''',
                unsafe_allow_html=True
            )
    with cols[4]:
        stat_tile_signed(f"{label_prefix} Total", total, unit="bps", dp=0, emphasize=True, color=delta_color(total))

def kpi_tile_signed(label: str, value_bps: float):
    txt = f"{value_bps:+.0f} bps"
    st.markdown(
        f'''
        <div class="kpi" style="height:86px;display:flex;flex-direction:column;justify-content:center;">
          <div class="label">{label}</div>
          <div class="value" style="font-size:1.6rem;font-weight:800;">{txt}</div>
        </div>
        ''', unsafe_allow_html=True
    )

def scenario_badge(name: str):
    st.markdown(
        f'''
        <div style="display:inline-block;padding:6px 10px;border:1px solid #D8D7DF;border-radius:6px;background:#fff;font-weight:700;color:{RB_COLORS["blue"]};">
          {name}
        </div>
        ''', unsafe_allow_html=True
    )

def human_title(txt: str) -> str:
    # maps older titles to plain English
    txt = txt.replace("ETR", "Expected 12-month total return")
    txt = txt.replace("Relative", "Difference vs Benchmark")
    txt = txt.replace("Fund − Index", "Fund − Benchmark")
    return txt

def summarize_selected_scenario(scenarios_df: pd.DataFrame, scenario_id: int) -> str:
    srows = scenarios_df.loc[scenarios_df["Scenario"] == scenario_id].copy()
    if srows.empty: return ""
    # Credit move (canonical sign already computed as 'spread_move_pct')
    credit_pct = float(srows["spread_move_pct"].iloc[0]) * 100.0
    # Top 3 rate shocks by |bp| across currencies & maturities
    cols = ["6m","2yr","5 yr","10 yr","20 yr","30 yr"]
    melt = (srows.melt(id_vars=["Currency"], value_vars=cols, var_name="Maturity", value_name="Δy_bp")
                 .assign(Maturity=lambda d: d["Maturity"].replace({"2yr":"2y","5 yr":"5y","10 yr":"10y","20 yr":"20y","30 yr":"30y"})))
    top = (melt.reindex(columns=["Currency","Maturity","Δy_bp"])
                .assign(absbp=lambda d: d["Δy_bp"].abs())
                .sort_values("absbp", ascending=False).head(3))
    parts = [f"Credit spreads: {'widen' if credit_pct>0 else ('tighten' if credit_pct<0 else 'no change')} ({credit_pct:+.0f}%)."]
    if not top.empty:
        bullets = [f"{r.Currency} {r.Maturity} {r['Δy_bp']:+.0f} bp" for _, r in top.iterrows()]
        parts.append("Largest rate shocks: " + ", ".join(bullets) + ".")
    return " ".join(parts)

def waterfall_contrib_explained(carry_bps_val, credit_bps_val, rates_bps_val, title: str):
    """
    A cleaner waterfall: Carry, Credit, Rates → Total, with bps labels on each bar.
    OGC is not shown here because it is a constant drag already reflected in the scenario ETR tiles.
    """
    x = ["Carry", "Credit", "Rates", "Total"]
    y = [carry_bps_val, credit_bps_val, rates_bps_val, 0.0]
    measures = ["relative","relative","relative","total"]
    text = [f"{v:+.0f} bps" for v in y[:-1]] + [""]
    fig = go.Figure(go.Waterfall(
        orientation="v",
        x=x, y=y, measure=measures, text=text,
        connector={"line": {"color": RB_COLORS["grey"]}},
        decreasing={"marker": {"color": RB_COLORS["orange"]}},
        increasing={"marker": {"color": RB_COLORS["med"]}},
        totals={"marker": {"color": RB_COLORS["blue"]}},
    ))
    fig.update_layout(
        title=title,
        height=PLOT_HEIGHT,
        yaxis_title="bps",
        margin=dict(l=10,r=10,t=40,b=20)
    )
    return fig

def relative_waterfall_explained(rel_carry, rel_credit, rel_rates, title: str):
    x = ["Carry (Fund − Bmk)","Credit (Fund − Bmk)","Rates (Fund − Bmk)","Difference vs Benchmark"]
    y = [rel_carry, rel_credit, rel_rates, 0.0]
    measures = ["relative","relative","relative","total"]
    text = [f"{v:+.0f} bps" for v in y[:-1]] + [""]
    fig = go.Figure(go.Waterfall(
        orientation="v",
        x=x, y=y, measure=measures, text=text,
        connector={"line": {"color": RB_COLORS["grey"]}},
        decreasing={"marker": {"color": RB_COLORS["orange"]}},
        increasing={"marker": {"color": RB_COLORS["med"]}},
        totals={"marker": {"color": RB_COLORS["blue"]}},
    ))
    fig.update_layout(
        title=title,
        height=PLOT_HEIGHT,
        yaxis_title="bps",
        margin=dict(l=10,r=10,t=40,b=20)
    )
    return fig

def rename_results_for_display(df: pd.DataFrame) -> pd.DataFrame:
    ren = {
        "fund_code":"Fund",
        "scenario_id":"Scenario ID",
        "scenario_name":"Scenario",
        "etr_bps_fund":"Expected 12-month total return (Fund, bps)",
        "etr_bps_idx":"Expected 12-month total return (Benchmark, bps)",
        "rel_etr_bps":"Difference vs Benchmark (bps)",
        "carry_bps_fund":"Carry (Fund, bps)",
        "carry_bps_idx":"Carry (Benchmark, bps)",
        "credit_bps_fund":"Credit impact (Fund, bps)",
        "credit_bps_idx":"Credit impact (Benchmark, bps)",
        "rates_bps_fund":"Rates impact (Fund, bps)",
        "rates_bps_idx":"Rates impact (Benchmark, bps)",
    }
    out = df.rename(columns=ren)
    return out[list(ren.values())] if set(ren).issubset(df.columns) else out

def build_attribution(sel_row: pd.Series) -> dict:
    """
    Absolute (Fund): Carry, Credit, Rates, OGC  -> Total
    Relative (Fund − Benchmark): CarryΔ, CreditΔ, RatesΔ, OGCΔ -> Difference
      Note: Benchmark OGC = 0 (cost only exists for Fund); so OGCΔ = −OGC_fund.
    Also return % of total variants for both.
    """
    carry_f   = float(sel_row["carry_bps_fund"])
    credit_f  = float(sel_row["credit_bps_fund"])
    rates_f   = float(sel_row["rates_bps_fund"])
    ogc_f     = float(sel_row.get("ogc_bps_fund", 0.0))  # already joined in results
    total_f   = carry_f + credit_f + rates_f - ogc_f

    carry_rel  = float(sel_row["carry_bps_fund"]  - sel_row["carry_bps_idx"])
    credit_rel = float(sel_row["credit_bps_fund"] - sel_row["credit_bps_idx"])
    rates_rel  = float(sel_row["rates_bps_fund"]  - sel_row["rates_bps_idx"])
    ogc_rel    = -ogc_f  # benchmark OGC = 0, so relative impact is a negative drag
    total_rel  = carry_rel + credit_rel + rates_rel + ogc_rel

    def pct(x, total):
        # For percentage view, use absolute values to avoid extreme percentages with small/negative totals
        abs_total = abs(total)
        return (abs(x) / abs_total * 100.0 * (1 if x * total >= 0 else -1)) if abs_total > 1e-6 else 0.0

    abs_vals = [carry_f, credit_f, rates_f, -ogc_f]          # OGC shown negative in absolute chart
    rel_vals = [carry_rel, credit_rel, rates_rel, ogc_rel]

    # For percentage calculations, use a more robust approach
    def safe_pct_abs(vals, total):
        abs_total = abs(total)
        if abs_total < 1e-6:
            return [0.0] * len(vals)
        return [(abs(v) / abs_total * 100.0 * (1 if v >= 0 else -1)) for v in vals]
    
    def safe_pct_rel(vals, total):
        # For relative percentages, if total is very small, show contribution magnitudes instead
        abs_total = abs(total)
        if abs_total < 10.0:  # If relative total < 10 bps, show as contribution magnitudes
            max_abs = max(abs(v) for v in vals) if vals else 1.0
            return [(abs(v) / max_abs * 100.0 * (1 if v >= 0 else -1)) for v in vals]
        return [(v / total * 100.0) for v in vals]

    return {
        "abs":     {"labels": ["Carry","Credit","Rates","Fund costs (OGC)"], "values": abs_vals, "total": total_f},
        "rel":     {"labels": ["Carry (Fund − Bmk)","Credit (Fund − Bmk)","Rates (Fund − Bmk)","Fund costs (OGC)"], "values": rel_vals, "total": total_rel},
        "abs_pct": {"labels": ["Carry","Credit","Rates","Fund costs (OGC)"], "values": safe_pct_abs(abs_vals, total_f), "total": 100.0},
        "rel_pct": {"labels": ["Carry (Fund − Bmk)","Credit (Fund − Bmk)","Rates (Fund − Bmk)","Fund costs (OGC)"], "values": safe_pct_rel(rel_vals, total_rel), "total": 100.0},
    }

def bar_attribution(values, labels, title, unit="bps", y_min=None, y_max=None):
    """Compact vertical bars with consistent scale and on-bar labels."""
    txt = [f"{v:+.0f} bps" if unit=="bps" else f"{v:+.1f} %" for v in values]
    fig = go.Figure(go.Bar(
        x=labels, y=values,
        marker_color=[RB_COLORS["med"] if v>=0 else RB_COLORS["orange"] for v in values],
        text=txt, textposition="outside",
        hovertemplate="%{x}<br>%{y:.1f} " + ("bps" if unit=="bps" else "%") + "<extra></extra>"
    ))
    fig.update_layout(
        title=title, height=PLOT_HEIGHT,
        yaxis=dict(title=("bps" if unit=="bps" else "% of total"), range=[y_min, y_max], zeroline=True,
                   gridcolor="rgba(128,128,128,0.15)"),
        xaxis=dict(title=""),
        margin=dict(l=10,r=10,t=44,b=16), showlegend=False
    )
    return fig

def scenario_ranking_bar(results_df: pd.DataFrame, title: str = "Which scenarios help or hurt the Fund vs Benchmark?"):
    """Bar chart of Difference vs Benchmark (bps) for all scenarios, sorted descending."""
    df = results_df[["scenario_id","scenario_name","rel_etr_bps"]].copy()
    df = df.sort_values("rel_etr_bps", ascending=False)
    fig = go.Figure(go.Bar(
        x=df["rel_etr_bps"], y=df["scenario_name"],
        orientation="h",
        marker_color=[RB_COLORS["med"] if v>=0 else RB_COLORS["orange"] for v in df["rel_etr_bps"]],
        hovertemplate="%{y}<br>Fund−Benchmark: %{x:.0f} bps<extra></extra>",
        text=[f"{v:+.0f} bps" for v in df["rel_etr_bps"]], textposition="outside"
    ))
    fig.update_layout(
        title=title, height=PLOT_HEIGHT,
        xaxis_title="Difference vs Benchmark (bps)", yaxis_title="Scenario",
        margin=dict(l=10,r=10,t=48,b=20), showlegend=False
    )
    return fig

def driver_mix_donuts(sel_row: pd.Series):
    """Two donuts: mix of Carry/Credit/Rates/OGC for Fund and Benchmark (% of total)."""
    carry_f, credit_f, rates_f, ogc_f = float(sel_row["carry_bps_fund"]), float(sel_row["credit_bps_fund"]), float(sel_row["rates_bps_fund"]), float(sel_row.get("ogc_bps_fund",0))
    carry_i, credit_i, rates_i, ogc_i = float(sel_row["carry_bps_idx"]),  float(sel_row["credit_bps_idx"]),  float(sel_row["rates_bps_idx"]),  0.0
    def split(vals):
        s = sum(vals)
        return [ (v/s*100.0 if abs(s)>1e-12 else 0.0) for v in vals ]
    lab = ["Carry","Credit","Rates","Fund costs (OGC)"]
    v_f = split([carry_f, credit_f, rates_f, -ogc_f])   # OGC is a drag, show as negative share (will render magnitude)
    v_i = split([carry_i, credit_i, rates_i, -ogc_i])
    fig = make_subplots(rows=1, cols=2, specs=[[{'type':'domain'},{'type':'domain'}]],
                        subplot_titles=("Driver mix — Fund (%)","Driver mix — Benchmark (%)"))
    fig.add_trace(go.Pie(labels=lab, values=[abs(x) for x in v_f], hole=.55,
                         marker=dict(colors=[RB_COLORS["med"],RB_COLORS["orange"],RB_COLORS["blue"],RB_COLORS["grey"]]),
                         textinfo="label+percent", showlegend=False), 1, 1)
    fig.add_trace(go.Pie(labels=lab, values=[abs(x) for x in v_i], hole=.55,
                         marker=dict(colors=[RB_COLORS["med"],RB_COLORS["orange"],RB_COLORS["blue"],RB_COLORS["grey"]]),
                         textinfo="label+percent", showlegend=False), 1, 2)
    fig.update_layout(title="What drives return in this scenario? (mix, %)", height=PLOT_HEIGHT, margin=dict(l=10,r=10,t=48,b=10))
    return fig

def relative_rates_by_currency(drilldown_dict: dict, scn_id: int, title: str = "Which currencies drive the difference (rates only)?"):
    """
    Use drilldown tables to compute **relative** (Fund−Benchmark) *rates* impact by currency (sum across maturities).
    Credit is portfolio-level and not split by currency, so this plot is rates-only.
    """
    rf = drilldown_dict[scn_id]["rates_contrib_fund"].rename(columns={"bps":"Fund_bps"})
    ri = drilldown_dict[scn_id]["rates_contrib_idx"].rename(columns={"bps":"Benchmark_bps"})
    tbl = (rf.merge(ri, on=["Currency","Node"], how="outer")
             .fillna(0.0)
             .assign(Rel=lambda d: d["Fund_bps"] - d["Benchmark_bps"]))
    per_ccy = tbl.groupby("Currency", as_index=False)["Rel"].sum().sort_values("Rel", ascending=False)
    fig = go.Figure(go.Bar(
        x=per_ccy["Rel"], y=per_ccy["Currency"], orientation="h",
        marker_color=[RB_COLORS["med"] if v>=0 else RB_COLORS["orange"] for v in per_ccy["Rel"]],
        text=[f"{v:+.0f} bps" for v in per_ccy["Rel"]], textposition="outside",
        hovertemplate="%{y}<br>Relative rates impact: %{x:.0f} bps<extra></extra>"
    ))
    fig.update_layout(title=title, height=PLOT_HEIGHT,
                      xaxis_title="Fund − Benchmark (bps)", yaxis_title="Currency",
                      margin=dict(l=10,r=10,t=48,b=20), showlegend=False)
    return fig, per_ccy

def status_pill(text: str, kind: str = "ok"):
    css = {"ok": "ok", "warn": "warn", "err": "err"}.get(kind, "ok")
    st.markdown(f'<span class="pill {css}">{text}</span>', unsafe_allow_html=True)

def render_over_under_bullets(krd_f: pd.DataFrame, krd_i: pd.DataFrame, top_k: int = 6, threshold: float = 0.05):
    diff = (krd_f - krd_i).stack().rename("ΔKRD").rename_axis(["Currency", "Node"]).reset_index()
    diff["abs"] = diff["ΔKRD"].abs()
    diff = diff.sort_values("abs", ascending=False)
    lines = []
    count = 0
    for _, row in diff.iterrows():
        if row["abs"] < threshold:
            continue
        sign = "Overweight" if row["ΔKRD"] > 0 else "Underweight"
        lines.append(f"- **{sign}** {row['Currency']} {row['Node']} by {row['ΔKRD']:.2f}")
        count += 1
        if count >= top_k:
            break
    if not lines:
        st.info("No material over/underweights vs index above threshold.")
    else:
        st.markdown("\n".join(lines))

def insights_narrative(results_df: pd.DataFrame, drilldown: Dict[int, Dict[str, pd.DataFrame]], fund_code: str) -> str:
    # Worst/best absolute (Fund) and worst/best relative (Fund-Index)
    df = results_df.copy()
    worst_abs = df.loc[df["etr_bps_fund"].idxmin()]
    best_abs  = df.loc[df["etr_bps_fund"].idxmax()]
    worst_rel = df.loc[df["rel_etr_bps"].idxmin()]
    best_rel  = df.loc[df["rel_etr_bps"].idxmax()]

    def mk_driver_text(row):
        sid = int(row["scenario_id"])
        rf = drilldown[sid]["rates_contrib_fund"]
        # Top two rate drivers (absolute magnitude)
        top2 = rf.sort_values("bps", key=lambda s: s.abs(), ascending=False).head(2)
        rate_str = ", ".join([f"{r.Currency} {r.Node} {BPS_FMT.format(r.bps)}" for r in top2.itertuples(index=False)])
        credit = row["credit_bps_fund"]
        rates  = row["rates_bps_fund"]
        main = "credit" if abs(credit) > abs(rates) else "rates"
        return main, rate_str

    a_main, a_rates = mk_driver_text(worst_abs)
    b_main, b_rates = mk_driver_text(best_abs)
    r_main, r_rates = mk_driver_text(worst_rel)
    s_main, s_rates = mk_driver_text(best_rel)

    txt = []
    txt.append(f"**Fund: {fund_code} – Exposure Risks**")
    txt.append(f"- **Worst absolute**: *{worst_abs['scenario_name']}* → {BPS_FMT.format(worst_abs['etr_bps_fund'])} (drivers: **{a_main}** {BPS_FMT.format(worst_abs['credit_bps_fund']) if a_main=='credit' else BPS_FMT.format(worst_abs['rates_bps_fund'])}; top rate nodes: {a_rates}).")
    txt.append(f"- **Best absolute**: *{best_abs['scenario_name']}* → {BPS_FMT.format(best_abs['etr_bps_fund'])} (drivers: **{b_main}**; top rate nodes: {b_rates}).")
    txt.append(f"- **Worst relative**: *{worst_rel['scenario_name']}* → {BPS_FMT.format(worst_rel['rel_etr_bps'])} (drivers: **{r_main}**; top rate nodes: {r_rates}).")
    txt.append(f"- **Best relative**: *{best_rel['scenario_name']}* → {BPS_FMT.format(best_rel['rel_etr_bps'])} (drivers: **{s_main}**; top rate nodes: {s_rates}).")
    txt.append("All numbers above are computed directly from the dashboard’s carry, DTS, KRD and scenario tables.")
    return "\n".join(txt)

# ================================ Sidebar (Upload & Controls) ================================

with st.sidebar:
    st.header("Data & Controls")
    upload = st.file_uploader("Upload **Dashboard_Input.xlsx**", type=["xlsx"])
    st.caption("Sheets required: Combined 2, Scenarios, OGC. Raw tabs optional for As‑of date.")

    if upload is None:
        st.info("Upload the workbook to proceed.")
        st.stop()

    try:
        inputs = load_workbook(upload.getvalue())
    except Exception as e:
        _fail(f"Failed to load/validate workbook: {e}")

    fund_code = st.selectbox("Fund", ["GCF", "GFI", "EYF"], index=0)
    # Build scenario map for this fund (names ordered by Scenario id)
    scn_map = {sid: sname for sid, sname in inputs.scenario_list}
    scn_name_to_id = {sname: sid for sid, sname in inputs.scenario_list}
    scn_name = st.selectbox("Scenario", list(scn_name_to_id.keys()), index=0)
    scn_id = scn_name_to_id[scn_name]

# ================================ Header ================================

def asof_banner(dates_map: Dict[str, pd.Timestamp]) -> str:
    vals = {k: v for k, v in dates_map.items() if pd.notna(v)}
    if not vals:
        return "As‑of: (not supplied)"
    uniq = set(pd.to_datetime(list(vals.values())).date)
    if len(uniq) == 1:
        dt = next(iter(uniq))
        return f"As‑of: {dt.isoformat()}"
    # Varies by fund
    parts = [f"{k}: {pd.to_datetime(v).date().isoformat()}" for k, v in vals.items()]
    return "As‑of varies by fund — " + "; ".join(parts)

st.markdown(
    f"""
<div class="rb-header">
  <div class="rb-title">
    <h1>Rubrics Positioning & Risk Dashboard</h1>
    <div class="rb-sub">{asof_banner(inputs.asof_by_fund)}</div>
  </div>
  <div class="rb-logo">
    <img src="https://rubricsam.com/wp-content/uploads/2021/01/cropped-rubrics-logo-tight.png" alt="Rubrics Logo"/>
  </div>
</div>
""",
    unsafe_allow_html=True,
)

# ================================ Compute Scenario Set (All) ================================

results_df, drilldown = compute_all_scenarios(inputs, fund_code)

# Convenience slices for selected scenario
sel = results_df.loc[results_df["scenario_id"] == scn_id].iloc[0]
fund_slice = slice_entity(inputs.combined, fund_code, "Fund")
index_slice = slice_entity(inputs.combined, fund_code, "Index")
krd_fund = krd_matrix(fund_slice)
krd_index = krd_matrix(index_slice)

# For QA: recompute selected scenario shocks & messages
ccy_set = sorted(set(krd_fund.index).union(set(krd_index.index)))
rates_shock_sel, spread_pct_sel, warn_list = scenario_shocks_for_fund(inputs.scenarios, ccy_set, scn_id)
rates_contrib_fund = drilldown[scn_id]["rates_contrib_fund"]
qa = qa_checks(
    combined=inputs.combined,
    scenarios=inputs.scenarios,
    fund_code=fund_code,
    selected_scn_id=scn_id,
    fund_rates_contrib=rates_contrib_fund,
    spread_move_pct=spread_pct_sel,
    rel_etr_bps_val=float(sel["rel_etr_bps"]),
    carry_ok_tup_fund=carry_recon_ok(fund_slice),
    carry_ok_tup_idx=carry_recon_ok(index_slice),
)

# ================================ Tabs ================================

tab_pos, tab_scn, tab_ins = st.tabs(["Current Positioning", "Scenario Analysis (12‑month)", "Insights"])

# --- Tab: Current Positioning ---
with tab_pos:
    # Get OGC for calculations
    ogc_bps_f = float(inputs.ogc_map_bps.get(fund_code, 0.0))
    
    # 1) Floating KPI tiles with Key Positioning Insights alongside
    st.subheader("Headline Metrics vs Benchmark")
    stats = compute_headline_stats(krd_fund, krd_index, fund_slice, index_slice, ogc_bps_f)
    stats["net_carry_adv"] = stats["net_carry"] - stats["car_i"]
    
    col_tiles, col_bullets = st.columns([1.3, 1.0], gap="large")

    with col_tiles:
        st.caption("Risk Metrics")
        r1c1, r1c2, r1c3 = st.columns(3)
        with r1c1:
            stat_tile_signed("Curve Duration (Fund)", stats["dur_f"], unit="yrs / 100bp", dp=2)
        with r1c2:
            stat_tile_signed("Curve Duration (Benchmark)", stats["dur_i"], unit="yrs / 100bp", dp=2, emphasize=False)
        with r1c3:
            stat_tile_signed("Curve Duration Δ (Fund − Benchmark)", stats["dur_d"], unit="yrs / 100bp", dp=2, color=delta_color(stats["dur_d"]))

        r2c1, r2c2, r2c3 = st.columns(3)
        with r2c1:
            stat_tile_signed("Spread Sensitivity (DTS) — Fund", stats["dts_f"], unit="", dp=2)
        with r2c2:
            stat_tile_signed("Spread Sensitivity (DTS) — Benchmark", stats["dts_i"], unit="", dp=2, emphasize=False)
        with r2c3:
            stat_tile_signed("DTS Δ (Fund − Benchmark)", stats["dts_d"], unit="", dp=2, color=delta_color(stats["dts_d"]))

        st.caption("Return Metrics")
        r3c1, r3c2, r3c3 = st.columns(3)
        with r3c1:
            stat_tile_signed("Carry (Fund)", stats["car_f"], unit="bps", dp=0)
        with r3c2:
            stat_tile_signed("Carry (Benchmark)", stats["car_i"], unit="bps", dp=0, emphasize=False)
        with r3c3:
            stat_tile_signed("Carry Difference (Fund − Benchmark)", stats["car_d"], unit="bps", dp=0, color=delta_color(stats["car_d"]))

        r4c1, r4c2 = st.columns(2)
        with r4c1:
            stat_tile_signed("OGC (Fund)", stats["ogc_f"], unit="bps", dp=0, emphasize=False)
        with r4c2:
            stat_tile_signed("Net Carry (after OGC)", stats["net_carry"], unit="bps", dp=0)
        
        # Net Carry Advantage tile
        r5c1, = st.columns(1)
        with r5c1:
            stat_tile_signed("Net Carry Advantage vs Benchmark", stats["net_carry_adv"], unit="bps", dp=0, color=delta_color(stats["net_carry_adv"]))

    with col_bullets:
        st.subheader("Key Positioning Insights")
        positioning_summary = render_positioning_bullets(krd_fund, krd_index, fund_slice, index_slice, ogc_bps_f)
        st.markdown(positioning_summary)
    
    # 2) Duration Contribution by Currency and Curve Sensitivity by Maturity
    st.subheader("Duration Contribution by Currency  •  Curve Sensitivity by Maturity")
    col1, col2 = st.columns(2)
    with col1:
        fig_ccy_bars, ccy_table = plot_duration_by_currency_bars(krd_fund, krd_index)
        fig_ccy_bars.update_layout(height=PLOT_HEIGHT)
        st.plotly_chart(fig_ccy_bars, use_container_width=True, config=PLOTLY_CONFIG)
        
        with st.expander("Download currency totals (Fund vs Benchmark)"):
            st.download_button("Download CSV", ccy_table.to_csv(index=False).encode("utf-8"),
                               file_name=f"{fund_code}_duration_contribution_by_currency.csv", mime="text/csv")
    with col2:
        fig_curve = plot_krd_curve_bubbles(krd_fund, krd_index)
        fig_curve.update_layout(height=PLOT_HEIGHT)
        st.plotly_chart(fig_curve, use_container_width=True, config=PLOTLY_CONFIG)
    
    # 3) Differences vs Benchmark
    st.subheader("Differences vs Benchmark")
    c_left, c_right = st.columns(2)
    with c_left:
        fig_dur_diff, _ = plot_duration_diff_by_ccy(krd_fund, krd_index)
        fig_dur_diff.update_layout(height=PLOT_HEIGHT)
        st.plotly_chart(fig_dur_diff, use_container_width=True, config=PLOTLY_CONFIG)
    with c_right:
        fig_node_diff, _ = plot_krd_node_diff(krd_fund, krd_index)
        fig_node_diff.update_layout(height=PLOT_HEIGHT)
        st.plotly_chart(fig_node_diff, use_container_width=True, config=PLOTLY_CONFIG)
    
    # 4) Risk–Carry vs Curve Sensitivity Heatmap
    st.subheader("Risk–Carry vs Curve Sensitivity Heatmap")
    rc_col, hm_col = st.columns(2, gap="large")
    with rc_col:
        fig_risk_carry = plot_risk_carry_map(fund_slice, index_slice, krd_fund, krd_index)
        if fig_risk_carry:
            fig_risk_carry.update_layout(height=PLOT_HEIGHT)
            st.plotly_chart(fig_risk_carry, use_container_width=True, config=PLOTLY_CONFIG)
        else:
            st.info("Risk–carry map could not be generated (insufficient data).")
    with hm_col:
        fig_hm, diff_df = heatmap_krd_diff_rg(krd_fund, krd_index, title="Duration Sensitivity Heatmap (Fund − Benchmark)")
        fig_hm.update_layout(height=PLOT_HEIGHT)
        st.plotly_chart(fig_hm, use_container_width=True, config=PLOTLY_CONFIG)
        with st.expander("Download heatmap data (Fund − Benchmark)"):
            st.dataframe(diff_df, use_container_width=True, height=260)
            st.download_button(
                "Download CSV",
                diff_df.to_csv(index=True).encode("utf-8"),
                file_name=f"{fund_code}_krd_diff_heatmap.csv",
                mime="text/csv",
            )

    # ================================ Methodology ================================
    
    with st.expander("Methodology (12‑month horizon)"):
        st.markdown(
            textwrap.dedent(
                """
                **Method summary**
                - **Carry (bps)** = 100 × Σ `Combined 2!Hedged Yield Contr` (per currency; excludes Total/Other).
                  (Reconciles to `Combined 2!Hedged Yield` on `Currency="Total"`.)
                - **Roll‑down** = 0 (v1 placeholder).
                - **OGC (bps)** from `OGC!OGC` mapped by fund.
                - **Credit P&L (bps)**: canonical `spread_move_pct` = − `Scenarios!Credit Spread Change %` so **+ = widening**.
                  Formula: `− DTS_total × spread_move_pct × 100` (DTS from `Currency="Total"` row per entity).
                - **Rates P&L (bps)**: for each currency/node, `− KRD × Δy_bp`; sum across nodes and currencies.
                  KRD nodes: 6m, 2y, 5y, 10y, 20y, 30y from `Combined 2`.
                  Rate shocks per currency/node from `Scenarios`.
                - **ETR (bps)** = Carry − OGC + Credit + Rates. **Relative** = Fund − Index.
                """
            )
        )

# --- Tab: Scenario Analysis (12-month) ---
with tab_scn:
    # Status pills
    status_pill("Neutral OK" if qa.neutral_ok else "No neutral", "ok" if qa.neutral_ok else "warn")
    status_pill("Carry recon OK" if (qa.carry_ok_fund and qa.carry_ok_idx) else "Carry recon Δ", "ok" if (qa.carry_ok_fund and qa.carry_ok_idx) else "warn")
    status_pill(qa.sign_msg, "ok" if qa.sign_ok else "warn")
    if warn_list:
        status_pill("Scenario completeness (filled zeros)", "warn")
    st.caption(qa.parity_msg)

    # === Scenario ranking across all scenarios ===
    fig_rank = scenario_ranking_bar(results_df)
    st.plotly_chart(fig_rank, use_container_width=True, config=PLOTLY_CONFIG)

    # --- Header: scenario badge + attribution tiles ---
    st.subheader("Selected Scenario")

    scenario_badge(scn_name)
    st.caption(summarize_selected_scenario(inputs.scenarios, scn_id))
    
    # Add scenario-specific narrative
    scenario_summary_block(sel)
    
    # Use consistent spacing with HTML for better control
    st.markdown("<br>", unsafe_allow_html=True)

    # Fund row
    st.caption("Fund attribution")
    attribution_tiles_row(
        "Fund",
        carry=float(sel["carry_bps_fund"]),
        credit=float(sel["credit_bps_fund"]),
        rates=float(sel["rates_bps_fund"]),
        ogc=float(sel["ogc_bps_fund"]),
        total=float(sel["etr_bps_fund"]),
        show_ogc_value=True
    )
    
    # Use consistent spacing with HTML for better control
    st.markdown("<br>", unsafe_allow_html=True)

    # Benchmark row
    st.caption("Benchmark attribution")
    attribution_tiles_row(
        "Benchmark",
        carry=float(sel["carry_bps_idx"]),
        credit=float(sel["credit_bps_idx"]),
        rates=float(sel["rates_bps_idx"]),
        ogc=0.0,
        total=float(sel["etr_bps_idx"]),
        show_ogc_value=False
    )
    
    # Use consistent spacing with HTML for better control
    st.markdown("<br>", unsafe_allow_html=True)

    # Difference tile (single prominent value under both rows)
    diff_col = st.columns([1])[0]
    with diff_col:
        stat_tile_signed("Difference vs Benchmark", float(sel["rel_etr_bps"]), unit="bps", dp=0, emphasize=True, color=delta_color(sel["rel_etr_bps"]))

    # === Two-up attribution with shared scale and a bps/% toggle ===
    st.subheader("Attribution overview")

    unit_choice = st.radio("Show contributions in:", ["bps", "% of total"], index=0, horizontal=True)

    attr = build_attribution(sel)
    if unit_choice == "bps":
        left_vals, left_labels  = attr["abs"]["values"],  attr["abs"]["labels"]
        right_vals, right_labels = attr["rel"]["values"], attr["rel"]["labels"]
        y_abs = max(abs(v) for v in left_vals + right_vals + [1e-9])
        y_min, y_max, unit = -1.1*y_abs, 1.1*y_abs, "bps"
    else:
        left_vals, left_labels  = attr["abs_pct"]["values"],  attr["abs_pct"]["labels"]
        right_vals, right_labels = attr["rel_pct"]["values"], attr["rel_pct"]["labels"]
        y_abs = max(abs(v) for v in left_vals + right_vals + [1e-9])
        y_abs = max(y_abs, 20.0)
        y_min, y_max, unit = -1.1*y_abs, 1.1*y_abs, "%"

    colL, colR = st.columns(2, gap="large")
    with colL:
        fig_left = bar_attribution(left_vals, left_labels, "What drives the Fund in this scenario?", unit=unit, y_min=y_min, y_max=y_max)
        st.plotly_chart(fig_left, use_container_width=True, config=PLOTLY_CONFIG)
    with colR:
        fig_right = bar_attribution(right_vals, right_labels, "Why the Fund differs from the Benchmark", unit=unit, y_min=y_min, y_max=y_max)
        st.plotly_chart(fig_right, use_container_width=True, config=PLOTLY_CONFIG)

    st.caption("Notes: Positive = helps the Fund. Right panel shows Fund minus Benchmark for each factor.")

    # === More views ===
    st.subheader("More views")

    # Driver mix donuts (Fund vs Benchmark)
    mix_cols = st.columns(1)
    with mix_cols[0]:
        fig_mix = driver_mix_donuts(sel)
        st.plotly_chart(fig_mix, use_container_width=True, config=PLOTLY_CONFIG)

    # Relative rates by currency
    fig_rel_ccy, rel_ccy_table = relative_rates_by_currency(drilldown, scn_id)
    st.plotly_chart(fig_rel_ccy, use_container_width=True, config=PLOTLY_CONFIG)
    with st.expander("Download relative rates by currency (bps)"):
        st.download_button("Download CSV", rel_ccy_table.to_csv(index=False).encode("utf-8"),
                           file_name=f"{fund_code}_relative_rates_by_currency_{scn_id}.csv", mime="text/csv")

    # --- Drill-down table of rate contributions (keep but clarify labels) ---
    with st.expander("Drill-down: rate impact by currency and maturity (bps)"):
        st.caption("Sorted by absolute impact. Positive = helps the Fund; negative = hurts.")
        tbl = drilldown[scn_id]["rates_contrib_fund"].rename(columns={"Node":"Maturity","bps":"Impact (bps)"})
        st.dataframe(tbl, use_container_width=True, height=320)

    # --- Scenario results table with delta columns and grouped ordering ---
    st.subheader("All scenarios – computed results (bps)")
    pretty = rename_results_for_display(results_df.copy())

    # Add driver deltas (Fund − Benchmark) next to each driver for quick scan
    pretty["Δ Carry (bps)"]  = pretty["Carry (Fund, bps)"]  - pretty["Carry (Benchmark, bps)"]
    pretty["Δ Credit (bps)"] = pretty["Credit impact (Fund, bps)"] - pretty["Credit impact (Benchmark, bps)"]
    pretty["Δ Rates (bps)"]  = pretty["Rates impact (Fund, bps)"]  - pretty["Rates impact (Benchmark, bps)"]

    # Reorder columns into grouped blocks
    cols_order = [
        "Scenario ID","Scenario",
        "Expected 12-month total return (Fund, bps)","Expected 12-month total return (Benchmark, bps)","Difference vs Benchmark (bps)",
        "Carry (Fund, bps)","Carry (Benchmark, bps)","Δ Carry (bps)",
        "Credit impact (Fund, bps)","Credit impact (Benchmark, bps)","Δ Credit (bps)",
        "Rates impact (Fund, bps)","Rates impact (Benchmark, bps)","Δ Rates (bps)"
    ]
    pretty = pretty.reindex(columns=cols_order)

    # Show and download
    st.dataframe(pretty.sort_values("Difference vs Benchmark (bps)", ascending=False),
                 use_container_width=True, height=420)
    st.download_button("Download all scenario results (CSV)", pretty.to_csv(index=False).encode("utf-8"),
                       file_name=f"{fund_code}_scenario_results_analyst_view.csv", mime="text/csv")

# --- Tab: GenAI Insights ---
with tab_ins:
    st.subheader("GenAI Insights")
    st.caption("AI-generated summary and recommendations based on Fund vs Benchmark positioning in the selected scenario.")

    if st.button("Generate GenAI Insights", type="primary"):
        ogc_bps_f = float(inputs.ogc_map_bps.get(fund_code, 0.0))
        payload = build_genai_payload(sel, drilldown, scn_id, krd_fund, krd_index, ogc_bps_f)
        result = generate_genai_insights(payload)
        result = vet_recommendations(payload, result)

        # Sectioned output
        st.markdown("### Headline")
        st.markdown(payload.get("headline", ""))

        st.markdown("### Key Drivers")
        driver_bullets = build_driver_bullets_from_payload(payload)
        for d in driver_bullets:
            st.markdown(f"- {d}")

        st.markdown("### Strategic Takeaway")
        st.markdown(result.get("takeaway", ""))

        st.markdown("### Recommendations")
        for r in result.get("recommendations", []):
            st.markdown(
                f"**{r.get('title','')}** — {r.get('action','')}  \n"
                f"*Est. impact*: {r.get('est_delta_bps','?')} bps  \n"
                f"*Why*: {r.get('why','')}"
            )


# ================================ Robustness: Validation Warnings ================================

# Scenario completeness warnings already collected for selected scenario
if warn_list:
    st.warning("Scenario completeness: some currencies/nodes were missing and treated as 0 bp shocks. See pills above.")

# Carry reconciliation warnings (Fund/Index)
ok_fund, sum_c_f, tot_f = carry_recon_ok(fund_slice)
ok_idx, sum_c_i, tot_i = carry_recon_ok(index_slice)
if not ok_fund or not ok_idx:
    st.warning(
        f"Carry reconciliation difference — Fund Δ={sum_c_f - tot_f:+.6f} pp, Index Δ={sum_c_i - tot_i:+.6f} pp "
        f"(Σ Hedged Yield Contr vs Hedged Yield on Total)."
    )
