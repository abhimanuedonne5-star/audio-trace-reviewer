import streamlit as st
import pandas as pd
import os
from datetime import date, datetime
from databricks.sdk import WorkspaceClient

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
VOLUME_PATH = "/Volumes/dev_omni/dev_omni_gold/audio_files"
TRACES_TABLE = "dev_omni.dev_omni_gold.traces"
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "2a6b5b84e8974695")

PAGE_SIZE = 50


# ─────────────────────────────────────────────
# CLIENT
# ─────────────────────────────────────────────
@st.cache_resource
def get_client():
    return WorkspaceClient()


# ─────────────────────────────────────────────
# GET DATE FOLDERS
# ─────────────────────────────────────────────
@st.cache_data(ttl=300)
def get_available_dates():

    w = get_client()

    entries = list(w.files.list_directory_contents(VOLUME_PATH))

    dates = sorted(
        [
            e.name
            for e in entries
            if e.is_directory and e.name.isdigit() and len(e.name) == 8
        ],
        reverse=True,
    )

    return dates


# ─────────────────────────────────────────────
# AUDIO IDS
# ─────────────────────────────────────────────
@st.cache_data(ttl=120)
def get_audio_ids(date_str):

    w = get_client()

    path = f"{VOLUME_PATH}/{date_str}"

    try:
        entries = list(w.files.list_directory_contents(path))
    except:
        return []

    return [
        os.path.splitext(e.name)[0]
        for e in entries
        if not e.is_directory and e.name.endswith(".wav")
    ]


# ─────────────────────────────────────────────
# FETCH TRACES
# ─────────────────────────────────────────────
@st.cache_data(ttl=60)
def fetch_traces(date_str, audio_ids):

    if not audio_ids:
        return pd.DataFrame()

    ids_sql = ",".join([f"'{x}'" for x in audio_ids])

    sql_date = datetime.strptime(date_str, "%Y%m%d").date()

    query = f"""
    SELECT trace_id,input,event_date
    FROM {TRACES_TABLE}
    WHERE event_date = DATE('{sql_date}')
    AND trace_id IN ({ids_sql})
    ORDER BY trace_id DESC
    """

    w = get_client()

    response = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=query,
        wait_timeout="30s"
    )

    if response.result is None or response.result.data_array is None:
        return pd.DataFrame()

    columns = [c.name for c in response.manifest.schema.columns]
    rows = [list(r) for r in response.result.data_array]

    df = pd.DataFrame(rows, columns=columns)

    return df


# ─────────────────────────────────────────────
# LOAD AUDIO
# ─────────────────────────────────────────────
def get_audio(trace_id, date_str):

    w = get_client()

    try:
        r = w.files.download(f"{VOLUME_PATH}/{date_str}/{trace_id}.wav")
        return r.contents.read()
    except:
        return None


# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Audio Trace Reviewer",
    layout="wide"
)

st.title("🎧 Audio + Trace Review Dashboard")


# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:

    st.header("📅 Date")

    dates = get_available_dates()

    today = date.today().strftime("%Y%m%d")

    default = dates.index(today) if today in dates else 0

    selected_date = st.selectbox(
        "Select Date",
        dates,
        index=default,
        format_func=lambda x: f"{x[:4]}-{x[4:6]}-{x[6:]}"
    )

    st.divider()

    search = st.text_input("🔎 Search")

    st.divider()

    page = st.number_input(
        "Page",
        min_value=1,
        value=1
    )


# ─────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────
with st.spinner("Loading..."):

    audio_ids = get_audio_ids(selected_date)

    traces_df = fetch_traces(selected_date, audio_ids)


# ─────────────────────────────────────────────
# STATS
# ─────────────────────────────────────────────
col1,col2,col3 = st.columns(3)

col1.metric("Audio Files", len(audio_ids))
col2.metric("Matching Traces", len(traces_df))
col3.metric("Date", f"{selected_date[:4]}-{selected_date[4:6]}-{selected_date[6:]}")


st.divider()


# ─────────────────────────────────────────────
# SEARCH
# ─────────────────────────────────────────────
if search:

    traces_df = traces_df[
        traces_df["trace_id"].str.contains(search,case=False)
        |
        traces_df["input"].str.contains(search,case=False,na=False)
    ]


# ─────────────────────────────────────────────
# PAGINATION
# ─────────────────────────────────────────────
start = (page-1)*PAGE_SIZE
end = start + PAGE_SIZE

page_df = traces_df.iloc[start:end]


# ─────────────────────────────────────────────
# TABLE
# ─────────────────────────────────────────────
st.subheader("Trace List")

st.dataframe(
    page_df[["trace_id","input"]],
    use_container_width=True,
    hide_index=True
)


# ─────────────────────────────────────────────
# TRACE SELECT
# ─────────────────────────────────────────────
if not page_df.empty:

    trace_id = st.selectbox(
        "Select Trace",
        page_df["trace_id"]
    )

    record = traces_df[traces_df["trace_id"] == trace_id].iloc[0]

    st.divider()

    col1,col2 = st.columns(2)

    with col1:

        st.subheader("🎧 Audio")

        audio_bytes = get_audio(trace_id,selected_date)

        if audio_bytes:
            st.audio(audio_bytes)

        else:
            st.error("Audio missing")

    with col2:

        st.subheader("📝 Query")

        st.info(record["input"])