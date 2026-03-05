import streamlit as st
import pandas as pd
import os
from datetime import date, datetime
from databricks.sdk import WorkspaceClient

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
VOLUME_PATH  = "/Volumes/dev_omni/dev_omni_gold/audio_files"
TRACES_TABLE = "dev_omni.dev_omni_gold.traces"

# Databricks Apps injects the configured warehouse ID via environment variable.
# Falls back to the hardcoded ID if running outside of a Databricks Apps context.
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "2a6b5b84e8974695")

# ─────────────────────────────────────────────
# STREAMLIT VERSION CHECK
# on_select="rerun" requires Streamlit >= 1.35.0
# ─────────────────────────────────────────────
def _st_version():
    try:
        return tuple(int(x) for x in st.__version__.split(".")[:2])
    except Exception:
        return (0, 0)

SUPPORTS_ON_SELECT = _st_version() >= (1, 35)

# ─────────────────────────────────────────────
# DATABRICKS CLIENT
# ─────────────────────────────────────────────
@st.cache_resource
def get_client():
    return WorkspaceClient()

# ─────────────────────────────────────────────
# LIST AVAILABLE DATES from volume subdirectories
# Returns (list_of_date_strings_desc, error_string)
# ─────────────────────────────────────────────
@st.cache_data(ttl=300)
def get_available_dates():
    w = get_client()
    try:
        entries = list(w.files.list_directory_contents(VOLUME_PATH))
    except Exception as e:
        return [], str(e)

    dates = sorted(
        [e.name for e in entries if e.is_directory and e.name and e.name.isdigit() and len(e.name) == 8],
        reverse=True,
    )
    return dates, None

# ─────────────────────────────────────────────
# LIST AUDIO FILES via Databricks SDK Files API
# Uses authenticated HTTP — works even when the /Volumes/ FUSE mount is absent.
# Returns (frozenset, error_string)
# ─────────────────────────────────────────────
@st.cache_data(ttl=60)
def get_audio_trace_ids(date_str: str):
    w = get_client()
    path = f"{VOLUME_PATH}/{date_str}"
    try:
        entries = list(w.files.list_directory_contents(path))
    except Exception as e:
        return frozenset(), str(e)

    wav_names = [
        e.name for e in entries
        if e.name and e.name.lower().endswith(".wav") and not e.is_directory
    ]

    if not wav_names:
        other = [e.name for e in entries if not e.is_directory]
        detail = (
            f"Directory `{path}` is accessible but contains no .wav files. "
            f"Other files found: {other[:5]}"
            if other else f"Directory `{path}` is accessible but empty."
        )
        return frozenset(), detail

    return frozenset(os.path.splitext(n)[0] for n in wav_names), None

# ─────────────────────────────────────────────
# FETCH TRACES FROM TABLE
# Returns (DataFrame, error_string)
# ─────────────────────────────────────────────
@st.cache_data(ttl=60)
def fetch_all_traces(date_str: str):
    w = get_client()
    sql_date = datetime.strptime(date_str, "%Y%m%d").date()

    query = f"""
        SELECT trace_id, input
        FROM {TRACES_TABLE}
        WHERE event_date = DATE('{sql_date}')
        ORDER BY trace_id DESC
    """

    try:
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            wait_timeout="30s"
        )
    except Exception as e:
        return None, str(e)

    if response is None:
        return None, "Query returned None — check your WAREHOUSE_ID."

    if response.result is None or response.result.data_array is None:
        return pd.DataFrame(columns=["trace_id", "input"]), None

    columns = [col.name for col in response.manifest.schema.columns]
    rows = [list(row) for row in response.result.data_array]

    df = pd.DataFrame(rows, columns=columns)
    df["trace_id"] = df["trace_id"].astype(str)

    return df, None

# ─────────────────────────────────────────────
# GET AUDIO BYTES via Databricks SDK Files API
# ─────────────────────────────────────────────
def get_audio(trace_id: str, date_str: str):
    w = get_client()
    try:
        response = w.files.download(f"{VOLUME_PATH}/{date_str}/{trace_id}.wav")
        return response.contents.read()
    except Exception:
        return None

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(page_title="Audio + Trace Reviewer", layout="wide")

st.title("🎧 Audio + Trace Review Dashboard")
st.caption("Shows only traces that have a matching audio file. Click a row to play audio and review the query.")

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:

    st.header("📅 Date Filter")

    available_dates, dates_error = get_available_dates()
    today_str = date.today().strftime("%Y%m%d")

    if available_dates:
        default_idx = available_dates.index(today_str) if today_str in available_dates else 0

        selected_date_str = st.selectbox(
            "Select Date",
            options=available_dates,
            index=default_idx,
            format_func=lambda d: f"{d[:4]}-{d[4:6]}-{d[6:]}"
        )
    else:
        if dates_error:
            st.warning(f"Could not list dates from volume: {dates_error}")

        picked = st.date_input("Select Date", value=date.today())
        selected_date_str = picked.strftime("%Y%m%d")

    st.divider()

    st.header("🔍 Search")

    search = st.text_input(
        "Search by Trace ID or Query",
        placeholder="e.g. trace_001"
    )

# ─────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────
with st.spinner("Loading traces..."):
    df_all, fetch_error = fetch_all_traces(selected_date_str)
    audio_ids, volume_error = get_audio_trace_ids(selected_date_str)

# ─────────────────────────────────────────────
# ERROR STATES
# ─────────────────────────────────────────────
if fetch_error:
    st.error(f"❌ Query execution failed: {fetch_error}")

elif df_all is None:
    st.error("❌ No data returned from the traces table.")

elif volume_error:
    st.error(f"❌ Cannot read audio volume: {volume_error}")

else:

    df = df_all[df_all["trace_id"].isin(audio_ids)].reset_index(drop=True)

    if search:
        mask = (
            df["trace_id"].str.contains(search, case=False, na=False)
            | df["input"].fillna("").astype(str).str.contains(search, case=False, na=False)
        )
        df = df[mask].reset_index(drop=True)

    if df.empty:

        st.warning(
            f"No traces match any of the {len(audio_ids)} audio file(s) found in the volume."
        )

    else:

        st.success(
            f"✅ {len(df)} traces with audio | {len(audio_ids)} audio files"
        )

        st.subheader("📋 Trace List")

        preview_df = df[["trace_id", "input"]].copy()
        preview_df["input"] = df["input"].fillna("").astype(str).str[:120] + "..."

        selection = st.dataframe(
            preview_df,
            use_container_width=True,
            on_select="rerun",
            selection_mode="single-row",
            hide_index=True,
        )

        selected_rows = list(selection.selection.rows)

        if selected_rows:

            idx = selected_rows[0]
            selected = df.iloc[idx]
            trace_id = selected["trace_id"]

            st.divider()
            st.subheader(f"🔎 Trace ID: `{trace_id}`")

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("### 🎧 Audio")

                audio_bytes = get_audio(trace_id, selected_date_str)

                if audio_bytes:
                    st.audio(audio_bytes, format="audio/wav")
                else:
                    st.error("Audio file missing")

            with col2:

                st.markdown("### 📝 Trace")

                st.info(
                    selected["input"]
                    if pd.notna(selected["input"])
                    else "_No input recorded_"
                )