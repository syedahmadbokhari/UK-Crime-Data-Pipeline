"""Streamlit UI for the AI Crime Report Generator."""

import logging

import pandas as pd
import streamlit as st

from report_generator.features import extract_features
from report_generator.generator import generate_report
from report_generator.loader import load_crime_data
from report_generator.output import save_report
from report_generator.prompt_builder import PromptBuilder

logging.basicConfig(level=logging.INFO)

st.set_page_config(
    page_title="AI Crime Report Generator",
    page_icon="🔍",
    layout="centered",
)

with st.sidebar:
    st.header("Settings")
    prompt_version = st.selectbox(
        "Prompt version",
        options=["v2 (recommended)", "v1 (basic)"],
        index=0,
    )
    version_key = "v2" if prompt_version.startswith("v2") else "v1"
    json_export = st.checkbox("Export JSON alongside report", value=False)
    st.divider()
    st.markdown(
        "**About**\n\n"
        "Converts UK police crime CSV data into formal "
        "narrative briefings using the Gemini API.\n\n"
        "All generated reports should be verified against "
        "source figures before publication."
    )

st.title("AI Crime Report Generator")
st.caption(
    "Upload a UK police CSV file and generate a formal analytical briefing report."
)

st.subheader("1. Upload Data")
current_file = st.file_uploader(
    "Current period CSV (required)",
    type="csv",
    help="Download from data.police.uk",
)
prev_file = st.file_uploader(
    "Previous period CSV (optional — enables month-on-month comparison)",
    type="csv",
)

if not current_file:
    st.info("Upload a crime CSV file to get started.", icon="📂")
    st.stop()

try:
    df = load_crime_data(current_file)
except ValueError as exc:
    st.error(f"Invalid file: {exc}")
    st.stop()

prev_df = None
if prev_file:
    try:
        prev_df = load_crime_data(prev_file)
    except ValueError as exc:
        st.warning(f"Could not load previous period — skipping comparison. ({exc})")

features = extract_features(df, prev_df)

st.subheader("2. Key Statistics")
col1, col2, col3 = st.columns(3)
col1.metric("Force Area", features["force"])
col2.metric("Period", features["period"])
col3.metric("Total Crimes", f"{features['total_crimes']:,}")

mom = features.get("mom_change")
if mom and mom.get("absolute") is not None:
    sign = "+" if mom["absolute"] > 0 else ""
    delta_str = f"{sign}{mom['absolute']:,} ({mom['pct']}%)"
    st.metric(
        label="Month-on-Month Change",
        value=mom["direction"].title(),
        delta=delta_str,
        delta_color="inverse",
    )

st.markdown("**Crime type breakdown**")
dist_df = (
    pd.DataFrame(
        features["distribution"].items(),
        columns=["Crime Type", "Share (%)"],
    )
    .sort_values("Share (%)", ascending=False)
    .reset_index(drop=True)
)
st.bar_chart(dist_df.set_index("Crime Type"))

with st.expander("View full breakdown table"):
    st.dataframe(dist_df, use_container_width=True, hide_index=True)

st.subheader("3. Generate Report")
if st.button("Generate Briefing Report", type="primary", use_container_width=True):
    builder = PromptBuilder()
    system, user = builder.build(features, version=version_key)

    with st.spinner("Calling Gemini API — this usually takes 5–15 seconds..."):
        try:
            report = generate_report(system, user)
        except EnvironmentError as exc:
            st.error(f"API key error: {exc}")
            st.stop()
        except RuntimeError as exc:
            st.error(f"Report generation failed: {exc}")
            st.stop()

    st.success("Report generated successfully.")
    st.divider()
    st.markdown(report)
    st.divider()

    saved_path = save_report(report, features, json_export=json_export)

    col_a, col_b = st.columns(2)
    with col_a:
        st.download_button(
            label="Download report (.txt)",
            data=saved_path.read_text(encoding="utf-8"),
            file_name=saved_path.name,
            mime="text/plain",
            use_container_width=True,
        )

    if json_export:
        json_path = saved_path.with_suffix(".json")
        with col_b:
            st.download_button(
                label="Download JSON export",
                data=json_path.read_text(encoding="utf-8"),
                file_name=json_path.name,
                mime="application/json",
                use_container_width=True,
            )

    st.caption(f"Report saved to `{saved_path}`")
    st.warning(
        "Verify all cited figures against the source CSV before publication.",
        icon="⚠️",
    )
