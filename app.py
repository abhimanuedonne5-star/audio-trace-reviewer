import streamlit as st
import pandas as pd
import os
from databricks.sdk import WorkspaceClient

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
VOLUME_PATH  = "/Volumes/dev_omni/dev_omni_gold/audio_files"
TRACES_TABLE = "dev_omni.dev_omni_gold.traces"
WAREHOUSE_ID = "2a6b5b84e8974695"

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
# LIST AUDIO FILES — scanned once per minute
# ─────────────────────────────────────────────
@st.cache_data(ttl=60)
def get_audio_trace_ids():
    """Return a frozenset of trace IDs that have a matching .wav file."""
    try:
        return frozenset(
            os.path.splitext(f)[0]
            for f in os.listdir(VOLUME_PATH)
            if f.lower().endswith(".wav")
        )
    except Exception:
        return frozenset()

# ─────────────────────────────────────────────
# FETCH TRACES FROM TABLE
# Returns (DataFrame, error_string) — no st.* calls inside (cache must be pure)
# ─────────────────────────────────────────────
@st.cache_data(ttl=60)
def fetch_all_traces():
    w = get_client()
    try:
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=f"""
                SELECT trace_id, input
                FROM {TRACES_TABLE}
                ORDER BY trace_id DESC
            """,
            wait_timeout="30s"
        )
    except Exception as e:
        return None, str(e)

    if response is None:
        return None, "Query returned None — check your WAREHOUSE_ID."

    if response.result is None or response.result.data_array is None:
        return pd.DataFrame(columns=["trace_id", "input"]), None

    columns = [col.name for col in response.manifest.schema.columns]
    rows    = [list(row) for row in response.result.data_array]
    df = pd.DataFrame(rows, columns=columns)
    df["trace_id"] = df["trace_id"].astype(str)
    return df, None

# ─────────────────────────────────────────────
# GET AUDIO BYTES
# ─────────────────────────────────────────────
def get_audio(trace_id: str):
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
st.caption("Shows only traces that have a matching audio file. Click a row to play audio and review the query.")

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.header("🔍 Search")
    search = st.text_input("Search by Trace ID or Query", placeholder="e.g. trace_001")
    st.divider()
    st.caption(f"Volume: `{VOLUME_PATH}`")
    st.caption(f"Table: `{TRACES_TABLE}`")
    st.caption(f"Streamlit: `{st.__version__}`")          # helps diagnose version issues
    st.caption(f"on_select supported: `{SUPPORTS_ON_SELECT}`")

# ─────────────────────────────────────────────
# LOAD DATA
# ─────────────────────────────────────────────
with st.spinner("Loading traces..."):
    df_all, fetch_error = fetch_all_traces()
    audio_ids = get_audio_trace_ids()

# ─────────────────────────────────────────────
# ERROR STATES  (no st.stop() — if/else only)
# ─────────────────────────────────────────────
if fetch_error:
    st.error(f"❌ Query execution failed: {fetch_error}")

elif df_all is None:
    st.error("❌ No data returned from the traces table.")

else:
    # ── Filter: only traces that have a matching audio file ────────────────────
    df = df_all[df_all["trace_id"].isin(audio_ids)].reset_index(drop=True)

    # ── Apply search ───────────────────────────────────────────────────────────
    if search:
        mask = (
            df["trace_id"].str.contains(search, case=False, na=False) |
            df["input"].fillna("").astype(str).str.contains(search, case=False, na=False)
        )
        df = df[mask].reset_index(drop=True)

    if df.empty:
        if not audio_ids:
            st.error(f"❌ No .wav files found in `{VOLUME_PATH}`. Check the volume path.")
        else:
            st.warning("No traces found matching your search.")

    else:
        st.success(
            f"✅ {len(df)} traces with audio  |  "
            f"{len(audio_ids)} total audio files in volume"
        )
        st.divider()

        # ── Trace selection table ──────────────────────────────────────────────
        st.subheader("📋 Trace List — Click a row to review")

        preview_df = df[["trace_id", "input"]].copy()
        preview_df["input"] = df["input"].fillna("").astype(str).str[:120] + "..."

        selected_rows = []   # default: nothing selected

        if SUPPORTS_ON_SELECT:
            # ── Interactive row-click selection (Streamlit >= 1.35) ────────────
            try:
                selection = st.dataframe(
                    preview_df,
                    use_container_width=True,
                    on_select="rerun",
                    selection_mode="single-row",
                    column_config={
                        "trace_id": st.column_config.TextColumn("Trace ID"),
                        "input"   : st.column_config.TextColumn("Query Preview"),
                    },
                    hide_index=True,
                    # Key resets stale widget state when row count changes
                    key=f"trace_table_{len(df)}",
                )
                selected_rows = list(selection.selection.rows)
            except Exception as e:
                st.dataframe(preview_df, use_container_width=True, hide_index=True)
                st.warning(f"Row-click selection unavailable: {e}")
                selected_rows = []
        else:
            # ── Fallback: selectbox (Streamlit < 1.35) ─────────────────────────
            st.dataframe(preview_df, use_container_width=True, hide_index=True)
            st.warning(
                f"Your Streamlit version (`{st.__version__}`) does not support "
                "interactive row selection. Use the dropdown below instead."
            )
            chosen = st.selectbox(
                "Select Trace ID to review:",
                options=["— select —"] + df["trace_id"].tolist(),
                index=0,
            )
            if chosen != "— select —":
                matches = df[df["trace_id"] == chosen]
                if not matches.empty:
                    selected_rows = [int(matches.index[0])]

        # ── Detail panel — no st.stop(), pure if/else ─────────────────────────
        row_valid = (
            len(selected_rows) > 0
            and 0 <= int(selected_rows[0]) < len(df)
        )

        if not row_valid:
            st.info("👆 Click any row above to review the trace and play its audio.")

        else:
            current_idx = int(selected_rows[0])
            selected    = df.iloc[current_idx]
            trace_id    = selected["trace_id"]

            st.divider()
            st.subheader(f"🔎 Trace ID: `{trace_id}`")
            st.caption(f"Record {current_idx + 1} of {len(df)}")

            audio_col, trace_col = st.columns([1, 1])

            with audio_col:
                st.markdown("### 🎧 Audio")
                audio_bytes = get_audio(trace_id)
                if audio_bytes:
                    st.audio(audio_bytes, format="audio/wav")
                    st.caption(f"📁 `{VOLUME_PATH}/{trace_id}.wav`")
                else:
                    st.error(f"Audio file unexpectedly missing for `{trace_id}`")
                    st.caption(f"Expected: `{VOLUME_PATH}/{trace_id}.wav`")

            with trace_col:
                st.markdown("### 📝 Trace Details")
                st.markdown("**🧑 User Query**")
                st.info(
                    selected["input"]
                    if pd.notna(selected["input"])
                    else "_No input recorded_"
                )
                st.caption(f"🔑 Trace ID: `{trace_id}`")
