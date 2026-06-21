import sys
import os
import streamlit as st
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from data_loader import load_audit_log, load_dq_results, load_analytics_summary, fetch_recent_alerts

st.set_page_config(page_title="Medical Imaging Lakehouse", layout="wide")

st.title("Medical Imaging Lakehouse Dashboard")

tab1, tab2, tab3 = st.tabs(["Pipeline Ops", "Clinical Analytics", "Live Alerts"])

with tab1:
    st.header("Pipeline Run History")
    audit_df = load_audit_log()

    if audit_df.empty:
        st.warning("No audit log entries found.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Runs", len(audit_df))
        col2.metric("Unique Jobs", audit_df["job_name"].nunique())
        col3.metric("Latest Run", audit_df["run_timestamp"].max().strftime("%Y-%m-%d %H:%M"))

        st.dataframe(
            audit_df[["job_name", "run_timestamp", "row_count", "notes"]],
            use_container_width=True,
        )

    st.header("Data Quality Pass Rates")
    try:
        dq_df = load_dq_results()
        if not dq_df.empty:
            dq_summary = dq_df.groupby("check_name")["passed"].mean().reset_index()
            dq_summary.columns = ["Check", "Pass Rate"]
            st.bar_chart(dq_summary.set_index("Check"))
            st.dataframe(dq_df, use_container_width=True)
        else:
            st.warning("No DQ results found.")
    except Exception as e:
        st.error(f"Could not load DQ results: {e}")


with tab2:
    st.header("Case Volumes by Modality & Diagnosis")
    try:
        case_counts, demographics = load_analytics_summary()

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("By Modality / View")
            st.dataframe(case_counts, use_container_width=True)
            chart_data = case_counts.copy()
            chart_data["label"] = chart_data["label"].map({0: "Normal/Other", 1: "Pneumonia"})
            st.bar_chart(chart_data.set_index("view_position")["case_count"])

        with col2:
            st.subheader("By Demographics")
            st.dataframe(demographics.head(20), use_container_width=True)

    except Exception as e:
        st.error(f"Could not load analytics summary: {e}")


with tab3:
    st.header("Live Clinical Alerts")
    st.caption("High-confidence (>70%) pneumonia predictions from the FastAPI service")

    if st.button("Refresh Alerts"):
        st.rerun()

    alerts = fetch_recent_alerts()

    if not alerts:
        st.info("No alerts found. Send a prediction via the API with a high-confidence positive to see alerts here.")
    else:
        alerts_df = pd.DataFrame(alerts)
        alerts_df = alerts_df.sort_values("alert_timestamp", ascending=False)
        st.dataframe(alerts_df, use_container_width=True)

        for alert in alerts[:5]:
            st.warning(
                f"⚠️ **{alert['filename']}** — {alert['pneumonia_probability']*100:.1f}% pneumonia probability "
                f"at {alert['alert_timestamp']}"
            )