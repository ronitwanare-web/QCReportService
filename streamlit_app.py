import requests
import streamlit as st
import pandas as pd
from io import BytesIO

FASTAPI_BASE_URL = "http://backend:8000"

st.set_page_config(page_title="Report Downloader", layout="wide")
st.title("JSON Report Downloader")

tab1, tab2 = st.tabs(["Reports", "Metrics CSV"])

with tab1:
    st.subheader("Fetch UUIDs and Download Reports")

    col1, col2 = st.columns(2)
    with col1:
        station_type = st.text_input("Station Type", value="assembly")
        start_time = st.text_input("Start Time (ISO)", value="2026-03-12T00:00:00Z")
        source_flag = st.selectbox("Source Flag", ["centum", "sks"])
    with col2:
        end_time = st.text_input("End Time (ISO)", value="2026-03-13T00:00:00Z")
        download_type = st.selectbox("Download Type", ["report", "image", "both"])

    if st.button("Fetch UUIDs"):
        payload = {
            "station_type": station_type,
            "start_time": start_time,
            "end_time": end_time,
            "source_flag": source_flag,
            "size": 10000,
        }
        resp = requests.post(f"{FASTAPI_BASE_URL}/reports/uuids", json=payload, timeout=120)
        if resp.ok:
            data = resp.json()["data"]
            df = pd.DataFrame(data)
            st.session_state["uuid_df"] = df
            st.success(f"Fetched {len(df)} UUIDs")
            st.dataframe(df, use_container_width=True)
        else:
            st.error(resp.text)

    df = st.session_state.get("uuid_df")
    if df is not None and not df.empty:
        uuid_options = df["uuid"].tolist()
        selected_uuids = st.multiselect("Choose UUIDs", uuid_options, default=uuid_options)

        mode = st.radio("Selection Mode", ["Selected UUIDs", "Single UUID", "All fetched"], horizontal=True)

        single_uuid = None
        uuid_list = None

        if mode == "Selected UUIDs":
            uuid_list = selected_uuids
        elif mode == "Single UUID":
            single_uuid = st.selectbox("Single UUID", uuid_options)

        if st.button("Download ZIP"):
            payload = {
                "station_type": station_type,
                "start_time": start_time,
                "end_time": end_time,
                "source_flag": source_flag,
                "download_type": download_type,
                "uuid_list": uuid_list if mode == "Selected UUIDs" else None,
                "single_uuid": single_uuid if mode == "Single UUID" else None,
            }

            resp = requests.post(f"{FASTAPI_BASE_URL}/reports/download", json=payload, timeout=300)
            if resp.ok:
                st.download_button(
                    label="Download ZIP file",
                    data=BytesIO(resp.content),
                    file_name=f"{source_flag}_{download_type}_reports.zip",
                    mime="application/zip",
                )
            else:
                st.error(resp.text)

with tab2:
    st.subheader("Download preEOL Metrics CSV")

    col1, col2 = st.columns(2)
    with col1:
        metric_station_type = st.text_input("Station Type", value="preeol", key="metric_station_type")
        metric_start_time = st.text_input("Start Time (ISO)", value="2026-03-12T00:00:00Z", key="metric_start")
        metric_source_flag = st.selectbox("Source Flag", ["centum", "sks"], key="metric_source")
    with col2:
        metric_end_time = st.text_input("End Time (ISO)", value="2026-03-13T00:00:00Z", key="metric_end")

    st.caption("This extracts report.tests[10].metadata.metrics[70:106] from GCS report JSON files.")

    if st.button("Download preEOL CSV"):
        payload = {
            "station_type": metric_station_type,
            "start_time": metric_start_time,
            "end_time": metric_end_time,
            "source_flag": metric_source_flag,
            "size": 10000,
        }

        resp = requests.post(f"{FASTAPI_BASE_URL}/metrics/csv", json=payload, timeout=300)
        if resp.ok:
            st.download_button(
                label="Download CSV",
                data=BytesIO(resp.content),
                file_name=f"{metric_source_flag}_{metric_station_type}_preeol_metrics.csv",
                mime="text/csv",
            )
        else:
            st.error(resp.text)