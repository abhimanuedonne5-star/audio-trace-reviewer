import streamlit as st
import pandas as pd
import os
from databricks.sdk import WorkspaceClient

# ─────────────────────────────────────────────
# RESET STALE SELECTION ON FRESH LOAD
# ─────────────────────────────────────────────
if "fresh_load" not in st.session_state:
    st.session_state["fresh_load"] = True
    if "selection" in st.session_state:
        del st.session_state["selection"]

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
VOLUME_PATH  = "/Volumes/dev_omni/dev_omni_gold/audio_files"
TRACES_TABLE = "dev_omni.dev_omni_gold.traces"
WAREHOUSE_ID = "2a6b5b84e8974695"

# ─────────────────────────────────────────────
# DATABRICKS CLIENT
# ─────────────────────────────────────────────
@st.cache_resource
def get_client():
    return WorkspaceClient()

# ─────────────────────────────────────────────
# FETCH TRACES
# ─────────────────────────────────────────────
@st.cache_data(ttl=60)
def fetch_all_traces():
    w = get_client()

    response = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=f"""
            SELECT trace_id, input
            FROM {TRACES_TABLE}
            ORDER BY trace_id DESC
        """,
        wait_timeout="30s"
    )

    if response is None:
        st.error("❌ Query returned None — check your WAREHOUSE_ID")
        st.stop()

    if response.result is None or response.result.data_array is None:
        return pd.DataFrame(columns=["trace_id", "input"])

    columns = [col.name for col in response.manifest.schema.columns]
    rows    = [list(row) for row in response.result.data_array]
    return pd.DataFrame(rows, columns=columns)

# ─────────────────────────────────────────────
# GET AUDIO
# ─────────────────────────────────────────────
def get_audio(trace_id):
    file_path = f"{VOLUME_PATH}/{trace_id}.wav"
    if os.path.exists(file_path):
        with open(file_path, "rb") as f:
            return f.read()
    return None

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(page_title="Audio + Trace Reviewer", layout="wide")
st.title("🎧 Audio + Trace Review Dashboard")
st.caption("Select a trace from the list to play audio and view the associated query side by side.")

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.header("🔍 Search & Filter")
    search = st.text_input("Search by Trace ID or Query", placeholder="e.g. trace_001")
    show_only_audio = st.checkbox("Show only traces with audio", value=False)
    st.divider()
    st.caption(f"Volume: `{VOLUME_PATH}`")
    st.caption(f"Table: `{TRACES_TABLE}`")

# ─────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────
with st.spinner("Loading traces..."):
    df = fetch_all_traces()

# Apply search filter
if search:
    df = df[
        df["trace_id"].str.contains(search, case=False, na=False) |
        df["input"].str.contains(search, case=False, na=False)
    ].reset_index(drop=True)

# Apply audio filter
if show_only_audio:
    df = df[df["trace_id"].apply(
        lambda tid: os.path.exists(f"{VOLUME_PATH}/{tid}.wav")
    )].reset_index(drop=True)

if df.empty:
    st.warning("No traces found.")
    st.stop()

st.success(f"✅ {len(df)} traces found")
st.divider()

# ─────────────────────────────────────────────
# TRACE SELECTION TABLE
# ─────────────────────────────────────────────
st.subheader("📋 Trace List — Click a row to review")

preview_df = df.copy()
preview_df["input"] = preview_df["input"].str[:100] + "..."
preview_df["audio"] = df["trace_id"].apply(
    lambda tid: "✅" if os.path.exists(f"{VOLUME_PATH}/{tid}.wav") else "❌"
)

selection = st.dataframe(
    preview_df,
    use_container_width=True,
    on_select="rerun",
    selection_mode="single-row",
    column_config={
        "trace_id": "Trace ID",
        "input"   : "Query Preview",
        "audio"   : "Audio"
    }
)

# ─────────────────────────────────────────────
# SAFE ROW SELECTION
# ─────────────────────────────────────────────
selected_rows = selection.selection.rows

if not selected_rows:
    st.info("👆 Click any row above to review the trace and play its audio.")
    st.stop()

current_idx = selected_rows[0]

if current_idx >= len(df):
    st.info("👆 Click any row above to review the trace and play its audio.")
    st.stop()

selected = df.iloc[current_idx]
trace_id = selected["trace_id"]

st.divider()

# ─────────────────────────────────────────────
# SIDE BY SIDE — AUDIO + TRACE
# ─────────────────────────────────────────────
st.subheader(f"🔎 Trace ID: `{trace_id}`")

audio_col, trace_col = st.columns([1, 1])

with audio_col:
    st.markdown("### 🎧 Audio")
    audio_bytes = get_audio(trace_id)
    if audio_bytes:
        st.audio(audio_bytes, format="audio/wav")
        st.caption(f"📁 `{VOLUME_PATH}/{trace_id}.wav`")
    else:
        st.error(f"No audio file found for `{trace_id}`")
        st.caption(f"Expected: `{VOLUME_PATH}/{trace_id}.wav`")

with trace_col:
    st.markdown("### 📝 Trace Details")
    st.markdown("**🧑 User Query**")
    st.info(selected["input"])
    st.caption(f"🔑 Trace ID: `{trace_id}`")

# ─────────────────────────────────────────────
# PREV / NEXT NAVIGATION
# ─────────────────────────────────────────────
st.divider()
col_prev, col_mid, col_next = st.columns([1, 3, 1])

with col_prev:
    st.button("⬅️ Previous", disabled=(current_idx == 0))
with col_mid:
    st.markdown(
        f"<div style='text-align:center; padding-top:8px'>Record <b>{current_idx + 1}</b> of <b>{len(df)}</b></div>",
        unsafe_allow_html=True
    )
with col_next:
    st.button("Next ➡️", disabled=(current_idx >= len(df) - 1))