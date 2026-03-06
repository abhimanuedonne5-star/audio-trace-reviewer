import streamlit as st
import pandas as pd
import os
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
VOLUME_PATH  = "/Volumes/dev_omni/dev_omni_gold/audio_files"
TRACES_TABLE = "dev_omni.dev_omni_gold.traces"

WAREHOUSE_ID = os.environ.get(
    "DATABRICKS_WAREHOUSE_ID",
    "2a6b5b84e8974695"
)

# ─────────────────────────────────────────────
# STREAMLIT PAGE
# ─────────────────────────────────────────────
st.set_page_config(page_title="Audio + Trace Reviewer", layout="wide")
st.title("🎧 Audio + Trace Review Dashboard")

# ─────────────────────────────────────────────
# DATABRICKS CLIENT
# ─────────────────────────────────────────────
@st.cache_resource
def get_client():
    return WorkspaceClient()


# ─────────────────────────────────────────────
# FETCH TRACES + AUDIO FILES
# ─────────────────────────────────────────────
@st.cache_data(ttl=60)
def fetch_traces(date_str: str):

    w = get_client()

    sql_date = datetime.strptime(date_str, "%Y%m%d").date()

    query = f"""
    SELECT
        t.trace_id,
        t.input,
        audio.file_path
    FROM {TRACES_TABLE} t
    JOIN (

        SELECT
            regexp_extract(_metadata.file_path,'([^/]+)\\\\.wav$',1) AS trace_id,
            _metadata.file_path AS file_path

        FROM read_files(
            '{VOLUME_PATH}/{date_str}/*.wav'
        )

    ) audio
    ON TRIM(t.trace_id) = audio.trace_id

    WHERE DATE(t.event_date) = DATE(:event_date)

    ORDER BY t.trace_id DESC
    """

    try:

        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            parameters=[
                StatementParameterListItem(
                    name="event_date",
                    value=str(sql_date)
                )
            ],
            wait_timeout="30s"
        )

    except Exception as e:
        return None, str(e)

    if response.result is None:
        return None, "Query returned no result"

    columns = [c.name for c in response.manifest.schema.columns]
    rows = [list(r) for r in response.result.data_array]

    df = pd.DataFrame(rows, columns=columns)

    return df, None


# ─────────────────────────────────────────────
# DOWNLOAD AUDIO
# ─────────────────────────────────────────────
def get_audio(file_path: str):

    w = get_client()

    try:
        response = w.files.download(file_path)
        return response.contents.read()
    except Exception:
        return None


# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:

    st.header("📅 Date Filter")

    picked = st.date_input("Select Date")

    selected_date_str = picked.strftime("%Y%m%d")

    st.divider()

    search = st.text_input("🔍 Search Trace ID")


# ─────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────
with st.spinner("Loading traces..."):

    df, err = fetch_traces(selected_date_str)


if err:
    st.error(err)
    st.stop()

if df is None or df.empty:
    st.warning("No traces with audio found")
    st.stop()


# ─────────────────────────────────────────────
# SEARCH FILTER
# ─────────────────────────────────────────────
if search:

    df = df[
        df["trace_id"].str.contains(search, case=False)
    ]


st.success(f"{len(df)} traces with audio found")


# ─────────────────────────────────────────────
# TRACE TABLE
# ─────────────────────────────────────────────
st.subheader("📋 Trace List")

preview_df = df[["trace_id", "input"]].copy()

preview_df["input"] = preview_df["input"].fillna("").str[:120] + "..."

selection = st.dataframe(
    preview_df,
    use_container_width=True,
    selection_mode="single-row",
    on_select="rerun",
    hide_index=True
)

rows = selection.selection.rows


# ─────────────────────────────────────────────
# TRACE DETAILS
# ─────────────────────────────────────────────
if rows:

    idx = rows[0]

    selected = df.iloc[idx]

    trace_id = selected["trace_id"]

    file_path = selected["file_path"]

    st.divider()

    st.subheader(f"Trace: {trace_id}")

    col1, col2 = st.columns(2)

    with col1:

        st.markdown("### 🎧 Audio")

        audio_bytes = get_audio(file_path)

        if audio_bytes:
            st.audio(audio_bytes)

        st.caption(file_path)

    with col2:

        st.markdown("### 📝 Query")

        st.info(selected["input"])