import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

session = get_active_session()

st.set_page_config(page_title="IPL Dashboard", layout="wide")
st.title("IPL Analytics Dashboard")

df = session.table("IPL.IPL_SCHEMA.BIGDATA_IPL_CLEAN_PY").to_pandas()

st.subheader("Dataset Preview")
st.dataframe(df.head(10), use_container_width=True)

st.divider()

st.sidebar.title("Filters")

teams = sorted(df["BATTING_TEAM"].dropna().unique())
selected_team = st.sidebar.selectbox("Select Team", ["All Teams"] + teams)

phases = ["POWERPLAY", "MIDDLE OVERS", "DEATH OVERS"]
selected_phase = st.sidebar.selectbox("Select Phase", ["All Phases"] + phases)

df_filtered = df.copy()

if selected_team != "All Teams":
    df_filtered = df_filtered[df_filtered["BATTING_TEAM"] == selected_team]

if selected_phase != "All Phases":
    df_filtered = df_filtered[df_filtered["PHASE"] == selected_phase]

st.subheader("Filtered Dataset")
st.dataframe(df_filtered.head(10), use_container_width=True)

st.divider()

st.subheader("Runs Per Over")

if df_filtered.empty:
    st.write("No data available for the selected filters.")
else:
    runs_per_over = (
        df_filtered.groupby("OVER")["BATSMAN_RUNS"]
        .sum()
        .reset_index()
        .sort_values("OVER")
    )

    st.line_chart(data=runs_per_over, x="OVER", y="BATSMAN_RUNS")

st.subheader("Top 10 Batsmen by Total Runs")

top_batsmen = (
    df.groupby("BATSMAN")["BATSMAN_RUNS"]
    .sum()
    .reset_index()
    .sort_values("BATSMAN_RUNS", ascending=False)
    .head(10)
)

st.bar_chart(data=top_batsmen, x="BATSMAN", y="BATSMAN_RUNS")

st.dataframe(top_batsmen, use_container_width=True)

st.subheader("Top 10 Bowlers by Economy Rate")

bowler_stats = (
    df.groupby("BOWLER")
    .agg({"TOTAL_RUNS": "sum", "MATCH_ID": "count"})
    .reset_index()
)

bowler_stats["ECONOMY"] = (bowler_stats["TOTAL_RUNS"] * 6) / bowler_stats["MATCH_ID"]
top_bowlers = bowler_stats.sort_values("ECONOMY").head(10)

st.bar_chart(data=top_bowlers, x="BOWLER", y="ECONOMY")
st.dataframe(top_bowlers, use_container_width=True)
