"""P6: BQ1 / BQ2 results dashboard over Athena.  Run: app/.venv/bin/streamlit run app/streamlit_app.py"""
import hmac
import os

import altair as alt
import pandas as pd
import streamlit as st
from pyathena import connect

PROFILE = os.environ.get("AWS_PROFILE")  # None on the server: instance role
REGION = "ap-southeast-1"
WG = "bda8-aml"
DB = "bda8_aml"

st.set_page_config(page_title="AML pipeline · BDA Group 8", layout="wide")

# Shared password when hosted (APP_PASSWORD set on the server); no gate when run locally.
_pw = os.environ.get("APP_PASSWORD")
if _pw and not st.session_state.get("authed"):
    st.title("AML pipeline · BDA Group 8")
    entered = st.text_input("Password", type="password")
    if entered:
        if hmac.compare_digest(entered, _pw):
            st.session_state["authed"] = True
            st.rerun()
        st.error("Wrong password")
    st.stop()


@st.cache_resource
def conn():
    return connect(profile_name=PROFILE, region_name=REGION, work_group=WG, schema_name=DB)


@st.cache_data(ttl=300, show_spinner="Querying Athena…")
def q(sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, conn())


def safe(sql):
    try:
        return q(sql)
    except Exception as e:  # table not crawled yet, etc.
        st.info(f"Not available yet: {e.__class__.__name__}")
        return pd.DataFrame()


st.title("IBM transactions for anti-money laundering")
st.caption("BDA Group 8 · HI-Small · train 01–07 Sep 2022, test 08–10 Sep 2022 · rates in basis points (1 bp = 0.01%)")

tab1, tab2 = st.tabs(["BQ1 · What signals laundering?", "BQ2 · Near-real-time flagging"])

with tab1:
    st.subheader("Laundering rate by group")
    c1, c2 = st.columns(2)
    pf = safe("SELECT * FROM v_rate_by_payment_format ORDER BY rate_bp DESC")
    if not pf.empty:
        c1.altair_chart(alt.Chart(pf).mark_bar().encode(
            x=alt.X("rate_bp:Q", title="Laundering rate (bp)"), y=alt.Y("payment_format:N", sort="-x", title=None),
            tooltip=["payment_format", "txns", "laundering", alt.Tooltip("rate_bp", format=".1f")]
        ).properties(title="By payment format", height=260), use_container_width=True)
    hr = safe("SELECT * FROM v_rate_by_hour ORDER BY hour_of_day")
    if not hr.empty:
        c2.altair_chart(alt.Chart(hr).mark_area(opacity=0.35, line=True).encode(
            x=alt.X("hour_of_day:O", title="Hour of day"), y=alt.Y("rate_bp:Q", title="Laundering rate (bp)"),
            tooltip=["hour_of_day", "txns", "laundering", alt.Tooltip("rate_bp", format=".1f")]
        ).properties(title="By hour of day", height=260), use_container_width=True)

    fc = safe("SELECT * FROM v_rate_by_format_currency WHERE txns >= 500")
    if not fc.empty:
        st.altair_chart(alt.Chart(fc).mark_rect().encode(
            x=alt.X("payment_currency:N", title="Payment currency"), y=alt.Y("payment_format:N", title=None),
            color=alt.Color("rate_bp:Q", title="Rate (bp)", scale=alt.Scale(scheme="blues")),
            tooltip=["payment_format", "payment_currency", "txns", "laundering", alt.Tooltip("rate_bp", format=".1f")]
        ).properties(title="Payment format × currency (groups with ≥ 500 transactions)", height=280),
            use_container_width=True)
    cf = safe("SELECT * FROM v_rate_by_cross_flags ORDER BY rate_bp DESC")
    if not cf.empty:
        st.markdown("**Cross-bank / cross-currency / self-transfer**")
        st.dataframe(cf, hide_index=True, use_container_width=True)

    st.subheader("Live: laundering rates computed on the Kafka stream")
    st.caption("Spark reads the raw rows off Kafka, cleans them in the stream and keeps running totals "
               "(no data-lake step). Updated about once a minute while a replay runs.")
    sr = safe("SELECT DISTINCT run FROM res_rates ORDER BY run DESC")
    if not sr.empty:
        srun = st.selectbox("Stream run", sr["run"].tolist(), key="bq1_stream_run")
        rt = q(f"SELECT dim, value, txns, laundering, rate_bp, as_of_event_ts FROM res_rates WHERE run = '{srun}'")
        st.caption(f"Event time reached: {rt['as_of_event_ts'].max()} · {int(rt[rt.dim == 'payment_format'].txns.sum()):,} rows")
        dim = st.selectbox("Group by", ["payment_format", "payment_currency", "receiving_currency", "format_currency",
                                        "hour_of_day", "is_cross_bank", "is_cross_currency", "is_self_transfer"])
        d = rt[(rt.dim == dim) & (rt.txns >= 100)]
        st.altair_chart(alt.Chart(d).mark_bar().encode(
            x=alt.X("rate_bp:Q", title="Laundering rate (bp)"),
            y=alt.Y("value:N", sort="-x", title=None),
            tooltip=["value", "txns", "laundering", alt.Tooltip("rate_bp", format=".1f")]
        ).properties(height=max(160, 22 * len(d))), use_container_width=True)

    st.subheader("Feature importance (tree models, 3 seeds each)")
    fi = safe("SELECT model, feature, avg(importance) AS importance, min(rank) AS best_rank, max(rank) AS worst_rank "
              "FROM res_feature_importance GROUP BY 1, 2")
    if not fi.empty:
        st.altair_chart(alt.Chart(fi).mark_bar().encode(
            x=alt.X("importance:Q", title="Mean importance"), y=alt.Y("feature:N", sort="-x", title=None),
            color=alt.Color("model:N", title="Model"), yOffset="model:N",
            tooltip=["model", "feature", alt.Tooltip("importance", format=".3f"), "best_rank", "worst_rank"]
        ).properties(height=420), use_container_width=True)
    m = safe("SELECT * FROM res_metrics ORDER BY model, seed")
    if not m.empty:
        st.markdown("**Test-period metrics** (baseline PR-AUC = base rate; precision/recall when flagging the top 0.1% / 0.5% / 1%)")
        st.dataframe(m, hide_index=True, use_container_width=True)

with tab2:
    st.subheader("Laundering patterns: injected vs mined (GraphFrames)")
    pfq = safe("SELECT * FROM res_pattern_frequency ORDER BY attempts DESC")
    if not pfq.empty:
        c1, c2 = st.columns(2)
        c1.altair_chart(alt.Chart(pfq.dropna(subset=["attempts"])).mark_bar().encode(
            x=alt.X("attempts:Q", title="Injected attempts"), y=alt.Y("pattern:N", sort="-x", title=None),
            tooltip=list(pfq.columns)).properties(title="Pattern frequency (ground truth)", height=280),
            use_container_width=True)
        c2.altair_chart(alt.Chart(pfq.dropna(subset=["attempts"])).mark_bar().encode(
            x=alt.X("recall_same_type:Q", title="Recall (same-type detector)", axis=alt.Axis(format="%")),
            y=alt.Y("pattern:N", sort="-x", title=None),
            tooltip=list(pfq.columns)).properties(title="Detector recall per pattern", height=280),
            use_container_width=True)
        st.dataframe(pfq, hide_index=True, use_container_width=True)

    st.subheader("Stream replay")
    runs = safe("SELECT DISTINCT run FROM res_stream_scored ORDER BY run DESC")
    if not runs.empty:
        run = st.selectbox("Run", runs["run"].tolist())
        s = q(f"""SELECT count(*) AS scored, sum(flagged) AS flagged, sum(is_laundering) AS laundering,
                    sum(flagged * is_laundering) AS true_flags,
                    approx_percentile(latency_s, 0.5) AS p50_latency_s,
                    approx_percentile(latency_s, 0.95) AS p95_latency_s
                  FROM res_stream_scored WHERE run = '{run}'""").iloc[0]
        k = st.columns(5)
        k[0].metric("Scored", f"{int(s.scored):,}")
        k[1].metric("Flagged", f"{int(s.flagged):,}")
        k[2].metric("Precision", f"{(s.true_flags / s.flagged if s.flagged else 0):.1%}")
        k[3].metric("Recall", f"{(s.true_flags / s.laundering if s.laundering else 0):.1%}")
        k[4].metric("Latency p50 / p95", f"{s.p50_latency_s:.1f} s / {s.p95_latency_s:.1f} s")
        lat = q(f"SELECT latency_s FROM res_stream_scored WHERE run = '{run}' AND rand() < 0.05")
        st.altair_chart(alt.Chart(lat).mark_bar().encode(
            x=alt.X("latency_s:Q", bin=alt.Bin(maxbins=40), title="Event-to-flag latency (s)"),
            y=alt.Y("count():Q", title="Transactions (5% sample)")).properties(height=220),
            use_container_width=True)
        st.markdown("**Flagged transaction feed**")
        st.dataframe(q(f"""SELECT ts, txn_id, from_key, to_key, amount_paid, payment_currency, payment_format,
                             round(risk_score, 3) AS risk_score, matched_rule, is_laundering,
                             round(latency_s, 1) AS latency_s
                           FROM res_flags WHERE run = '{run}' ORDER BY risk_score DESC LIMIT 500"""),
                     hide_index=True, use_container_width=True)
