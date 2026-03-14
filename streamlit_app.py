import requests
import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime, time

FASTAPI_BASE_URL = "http://backend:8000"

STATION_TYPES = ["preeol", "eol", "camera"]
SOURCE_FLAGS = ["centum", "sks"]
DOWNLOAD_TYPES = ["report", "image", "both"]
IMAGE_VARIANTS = ["G0", "G4", "G6", "all"]
PHASE_OPTIONS = ["G0", "G4", "G6"]
CAMERA_EV_OPTIONS = ["-1EV", "0EV", "+1EV"]


def build_iso_datetime(selected_date, selected_time) -> str:
    dt = datetime.combine(selected_date, selected_time)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


st.set_page_config(page_title="QC Report Service", layout="wide")
st.title("QC Report Service")

tab1, tab2, tab3 = st.tabs(["Reports", "Metrics CSV", "Download by UUID"])

with tab1:
    st.subheader("Fetch UUIDs and Download Reports")

    source_flag = st.selectbox("Source Flag", SOURCE_FLAGS, index=0, key="reports_source_flag")

    top_col1, top_col2 = st.columns(2)
    with top_col1:
        station_type = st.selectbox("Station Type", STATION_TYPES, index=0, key="reports_station_type")
    with top_col2:
        download_type = st.selectbox("Download Type", DOWNLOAD_TYPES, index=2, key="reports_download_type")

    st.markdown("### Time Range")
    start_col1, start_col2 = st.columns(2)
    with start_col1:
        start_date = st.date_input(
            "Start Date",
            value=datetime.utcnow().date(),
            key="reports_start_date",
        )
    with start_col2:
        start_clock = st.time_input(
            "Start Time",
            value=time(0, 0),
            key="reports_start_time",
        )

    end_col1, end_col2 = st.columns(2)
    with end_col1:
        end_date = st.date_input(
            "End Date",
            value=datetime.utcnow().date(),
            key="reports_end_date",
        )
    with end_col2:
        end_clock = st.time_input(
            "End Time",
            value=time(23, 59, 59),
            key="reports_end_time",
        )

    image_variant = None
    if download_type in ["image", "both"]:
        if station_type in ["preeol", "eol"]:
            image_variant = st.selectbox("Image Type", IMAGE_VARIANTS, index=3, key="reports_image_variant")
        elif station_type == "camera":
            st.info("For camera station, all images will be downloaded.")

    start_time = build_iso_datetime(start_date, start_clock)
    end_time = build_iso_datetime(end_date, end_clock)

    st.caption(f"Selected range: {start_time} to {end_time}")

    if st.button("Fetch UUIDs", key="fetch_uuids_btn"):
        payload = {
            "station_type": station_type,
            "start_time": start_time,
            "end_time": end_time,
            "source_flag": source_flag,
            "size": 10000,
        }

        try:
            with st.spinner("Fetching UUIDs..."):
                resp = requests.post(
                    f"{FASTAPI_BASE_URL}/reports/uuids",
                    json=payload,
                    timeout=120,
                )

            if resp.ok:
                response_json = resp.json()
                data = response_json.get("data", [])
                df = pd.DataFrame(data)

                st.session_state["uuid_df"] = df
                st.session_state["report_filters"] = {
                    "station_type": station_type,
                    "start_time": start_time,
                    "end_time": end_time,
                    "source_flag": source_flag,
                    "download_type": download_type,
                    "image_variant": image_variant,
                }

                st.success(f"Fetched {len(df)} UUIDs")

                if not df.empty:
                    st.dataframe(df, use_container_width=True)
                else:
                    st.warning("No UUIDs found for the selected filters.")
            else:
                st.error(f"UUID fetch failed: {resp.status_code} - {resp.text}")

        except Exception as e:
            st.error(f"Request failed: {e}")

    df = st.session_state.get("uuid_df")
    report_filters = st.session_state.get("report_filters")

    if df is not None and not df.empty and report_filters is not None:
        st.markdown("---")
        st.subheader("Download Reports / Images")

        uuid_options = df["uuid"].dropna().tolist()

        mode = st.radio(
            "Selection Mode",
            ["Selected UUIDs", "Single UUID", "All fetched"],
            horizontal=True,
            key="reports_selection_mode",
        )

        uuid_list = None
        single_uuid = None

        if mode == "Selected UUIDs":
            uuid_list = st.multiselect(
                "Choose UUIDs",
                uuid_options,
                default=uuid_options,
                key="reports_selected_uuids",
            )
        elif mode == "Single UUID":
            single_uuid = st.selectbox("Single UUID", uuid_options, key="reports_single_uuid")

        if st.button("Download ZIP", key="download_reports_zip_btn"):
            payload = {
                "station_type": report_filters["station_type"],
                "start_time": report_filters["start_time"],
                "end_time": report_filters["end_time"],
                "source_flag": report_filters["source_flag"],
                "download_type": download_type,
                "image_variant": image_variant,
                "uuid_list": uuid_list if mode == "Selected UUIDs" else None,
                "single_uuid": single_uuid if mode == "Single UUID" else None,
            }

            try:
                with st.spinner("Preparing ZIP from GCS..."):
                    resp = requests.post(
                        f"{FASTAPI_BASE_URL}/reports/download",
                        json=payload,
                        timeout=600,
                    )

                if resp.ok:
                    filename_parts = [
                        report_filters["source_flag"],
                        report_filters["station_type"],
                        download_type,
                    ]
                    if image_variant and download_type in ["image", "both"]:
                        filename_parts.append(image_variant)

                    file_name = "_".join(filename_parts) + "_reports.zip"

                    st.success("ZIP is ready.")
                    st.download_button(
                        label="Download ZIP file",
                        data=BytesIO(resp.content),
                        file_name=file_name,
                        mime="application/zip",
                        key="download_reports_zip_file_btn",
                    )
                else:
                    st.error(f"Download failed: {resp.status_code} - {resp.text}")

            except Exception as e:
                st.error(f"Download failed: {e}")

with tab2:
    st.subheader("Download Metrics CSV")

    metric_source_flag = st.selectbox(
        "Source Flag",
        SOURCE_FLAGS,
        index=0,
        key="metric_source",
    )

    metric_station_type = st.selectbox(
        "Station Type",
        STATION_TYPES,
        index=0,
        key="metric_station_type",
    )

    phase = None
    camera_ev = None

    if metric_source_flag == "centum":
        if metric_station_type in ["preeol", "eol"]:
            phase = st.selectbox(
                "Phase",
                PHASE_OPTIONS,
                index=0,
                key="phase_selector_centum",
            )
        elif metric_station_type == "camera":
            camera_ev = st.selectbox(
                "Camera EV",
                CAMERA_EV_OPTIONS,
                index=1,
                key="camera_ev",
            )

    elif metric_source_flag == "sks":

        if metric_station_type == "preeol":
            st.info("SKS preeol only supports G0.")
            phase = "G0"

        elif metric_station_type == "eol":
            phase = st.selectbox(
                "Phase",
                PHASE_OPTIONS,
                index=0,
                key="phase_selector_sks",
            )

        elif metric_station_type == "camera":
            camera_ev = st.selectbox(
                "Camera EV",
                CAMERA_EV_OPTIONS,
                index=1,
                key="camera_ev_sks",
            )

    st.markdown("### Time Range")
    metric_start_col1, metric_start_col2 = st.columns(2)
    with metric_start_col1:
        metric_start_date = st.date_input(
            "Start Date",
            value=datetime.utcnow().date(),
            key="metrics_start_date",
        )
    with metric_start_col2:
        metric_start_clock = st.time_input(
            "Start Time",
            value=time(0, 0),
            key="metrics_start_time",
        )

    metric_end_col1, metric_end_col2 = st.columns(2)
    with metric_end_col1:
        metric_end_date = st.date_input(
            "End Date",
            value=datetime.utcnow().date(),
            key="metrics_end_date",
        )
    with metric_end_col2:
        metric_end_clock = st.time_input(
            "End Time",
            value=time(23, 59, 59),
            key="metrics_end_time",
        )

    metric_start_time = build_iso_datetime(metric_start_date, metric_start_clock)
    metric_end_time = build_iso_datetime(metric_end_date, metric_end_clock)

    st.caption(f"Selected range: {metric_start_time} to {metric_end_time}")

    if metric_source_flag == "centum":
        if metric_station_type == "preeol":
            st.caption("Centum preeol: G0 -> test 10 and metrics 70:106, G4 -> test 12 and metrics 122:241, G6 -> test 14 and metrics 122:241.")
        elif metric_station_type == "eol":
            st.caption("Centum eol: G0 -> test 11 and metrics 70:106, G4 -> test 13 and metrics 122:241, G6 -> test 15 and metrics 122:241.")
        elif metric_station_type == "camera":
            st.caption("Centum camera: -1EV -> test 0, 0EV -> test 1, +1EV -> test 2 from images[0].")
    elif metric_source_flag == "sks":
        if metric_station_type == "preeol":
            st.caption("SKS preeol: G0 -> test 2 and metrics 4:7.")
        elif metric_station_type == "eol":
            st.caption("SKS eol: G0 -> test 12 metrics 4:7, G4 -> test 13 metrics 110:219, G6 -> test 14 metrics 110:219.")
        elif metric_station_type == "camera":
            st.caption("SKS camera: -1EV -> test 0, 0EV -> test 1, +1EV -> test 2 from metadata.metrics[0].image.compute_server_report.verdict.metrics.measurements.")
    if st.button("Download Metrics CSV", key="download_metrics_csv_btn"):
        payload = {
            "station_type": metric_station_type,
            "start_time": metric_start_time,
            "end_time": metric_end_time,
            "source_flag": metric_source_flag,
            "preeol_phase": phase,
            "camera_ev": camera_ev,
            "size": 10000,
        }

        try:
            with st.spinner("Generating CSV from GCS reports..."):
                resp = requests.post(
                    f"{FASTAPI_BASE_URL}/metrics/csv",
                    json=payload,
                    timeout=600,
                )

            if resp.ok:
                if metric_station_type == "camera":
                    file_name = f"{metric_source_flag}_{metric_station_type}_{camera_ev}_metrics.csv"
                elif metric_station_type in ["preeol", "eol"]:
                    phase_label = phase if phase else "G0"
                    file_name = f"{metric_source_flag}_{metric_station_type}_{phase_label}_metrics.csv"
                else:
                    file_name = f"{metric_source_flag}_{metric_station_type}_metrics.csv"

                st.success("CSV is ready.")
                st.download_button(
                    label="Download CSV",
                    data=BytesIO(resp.content),
                    file_name=file_name,
                    mime="text/csv",
                    key="download_metrics_csv_file_btn",
                )
            else:
                st.error(f"CSV download failed: {resp.status_code} - {resp.text}")

        except Exception as e:
            st.error(f"CSV download failed: {e}")

with tab3:
    st.subheader("Download Report / Images by UUID")

    uuid_source_flag = st.selectbox(
        "Source Flag",
        SOURCE_FLAGS,
        index=0,
        key="uuid_source_flag",
    )

    uuid_station_type = st.selectbox(
        "Station Type",
        STATION_TYPES,
        index=0,
        key="uuid_station_type",
    )

    uuid_download_type = st.selectbox(
        "Download Type",
        DOWNLOAD_TYPES,
        index=2,
        key="uuid_download_type",
    )

    input_uuid = st.text_input(
        "UUID",
        key="single_uuid_input",
        placeholder="Enter UUID here",
    ).strip()

    uuid_image_variant = None
    if uuid_download_type in ["image", "both"]:
        if uuid_station_type in ["preeol", "eol"]:
            uuid_image_variant = st.selectbox(
                "Image Type",
                IMAGE_VARIANTS,
                index=3,
                key="uuid_image_variant",
            )
        elif uuid_station_type == "camera":
            st.info("For camera station, all images will be downloaded.")

    if st.button("Download by UUID", key="download_by_uuid_btn"):
        if not input_uuid:
            st.error("Please enter a UUID.")
        else:
            payload = {
                "station_type": uuid_station_type,
                "start_time": "1970-01-01T00:00:00Z",
                "end_time": "2100-01-01T00:00:00Z",
                "source_flag": uuid_source_flag,
                "download_type": uuid_download_type,
                "image_variant": uuid_image_variant,
                "single_uuid": input_uuid,
            }

            try:
                with st.spinner("Preparing ZIP from GCS..."):
                    resp = requests.post(
                        f"{FASTAPI_BASE_URL}/reports/download",
                        json=payload,
                        timeout=600,
                    )

                if resp.ok:
                    filename_parts = [
                        uuid_source_flag,
                        uuid_station_type,
                        input_uuid,
                        uuid_download_type,
                    ]

                    if uuid_image_variant and uuid_download_type in ["image", "both"]:
                        filename_parts.append(uuid_image_variant)

                    file_name = "_".join(filename_parts) + ".zip"

                    st.success("ZIP is ready.")
                    st.download_button(
                        label="Download ZIP",
                        data=BytesIO(resp.content),
                        file_name=file_name,
                        mime="application/zip",
                        key="download_by_uuid_zip_file_btn",
                    )

                else:
                    st.error(f"Download failed: {resp.status_code} - {resp.text}")

            except Exception as e:
                st.error(f"Download failed: {e}")