import streamlit as st
import pandas as pd
import os
from datetime import date, datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

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
# FETCH TRACES FROM TABLE — only for IDs that have audio files
# Returns (DataFrame, error_string) — no st.* calls inside (cache must be pure)
# ─────────────────────────────────────────────
@st.cache_data(ttl=60)
def fetch_traces_for_audio_ids(date_str: str, audio_ids: frozenset):
    if not audio_ids:
        return pd.DataFrame(columns=["trace_id", "input"]), None

    w = get_client()
    sql_date = datetime.strptime(date_str, "%Y%m%d").date()

    # Build IN list — IDs come from our own volume listing, sanitize to be safe
    id_list = ", ".join(f"'{tid.replace(chr(39), '')}'" for tid in audio_ids)
    # query = f"""
    #     SELECT trace_id, input
    #     FROM {TRACES_TABLE}
    #     WHERE event_date IN (DATE(:event_date))
    #       AND TRIM(trace_id) IN ({id_list})
    #     ORDER BY trace_id DESC
    # """
    query = f"""
        SELECT trace_id, input
        FROM {TRACES_TABLE}
    """
    try:
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            parameters=[StatementParameterListItem(name="event_date", value=str(sql_date))],
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
    df["trace_id"] = df["trace_id"].astype(str).str.strip()
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

    if available_dates:
        selected_date_str = st.selectbox(
            "Select Date",
            options=[None] + available_dates,
            index=0,
            format_func=lambda d: "— select a date —" if d is None else f"{d[:4]}-{d[4:6]}-{d[6:]}",
            key="date_selector",
        )
    else:
        if dates_error:
            st.warning(f"Could not list dates from volume: {dates_error}")
        # Fall back to manual date input
        picked = st.date_input("Select Date", value=None, key="date_input_fallback")
        selected_date_str = picked.strftime("%Y%m%d") if picked else None

    st.divider()
    st.header("🔍 Search")
    search = st.text_input("Search by Trace ID or Query", placeholder="e.g. trace_001")
    st.divider()
    st.caption(f"Volume: `{VOLUME_PATH}/{selected_date_str}`")
    st.caption(f"Table: `{TRACES_TABLE}`")
    st.caption(f"Warehouse: `{WAREHOUSE_ID}`")
    st.caption(f"Streamlit: `{st.__version__}`")
    st.caption(f"on_select supported: `{SUPPORTS_ON_SELECT}`")

# ─────────────────────────────────────────────
# LOAD DATA — only after a date is selected
# ─────────────────────────────────────────────
if not selected_date_str:
    st.info("👈 Select a date from the sidebar to load traces.")

else:
    with st.spinner("Loading audio file list..."):
        audio_ids, volume_error = get_audio_trace_ids(selected_date_str)

    with st.spinner("Loading traces..."):
        df_all, fetch_error = fetch_traces_for_audio_ids(selected_date_str, audio_ids)

    # ─────────────────────────────────────────────
    # ERROR STATES  (no st.stop() — if/else only)
    # ─────────────────────────────────────────────
    if volume_error:
        st.error(f"❌ Cannot read audio volume: {volume_error}")
        st.info(
            "**How to fix:** Make sure the app's service principal has been granted "
            "`READ VOLUME` on the volume in Unity Catalog:\n\n"
            f"```sql\nGRANT READ VOLUME ON VOLUME dev_omni.dev_omni_gold.audio_files "
            f"TO <your-app-service-principal>;\n```"
        )

    elif fetch_error:
        st.error(f"❌ Query execution failed: {fetch_error}")

    elif df_all is None:
        st.error("❌ No data returned from the traces table.")

    else:
        # df_all already contains only traces with matching audio files
        df = df_all.reset_index(drop=True)

        # ── Apply search ───────────────────────────────────────────────────────
        if search:
            mask = (
                df["trace_id"].str.contains(search, case=False, na=False) |
                df["input"].fillna("").astype(str).str.contains(search, case=False, na=False)
            )
            df = df[mask].reset_index(drop=True)

        if df.empty:
            st.warning(
                f"No traces match any of the {len(audio_ids)} audio file(s) found in the volume. "
                "Check that your trace IDs match the audio file names (without `.wav`)."
            )
            with st.expander("🔍 Debug: show IDs from table vs volume"):
                sql_date_debug = datetime.strptime(selected_date_str, "%Y%m%d").date()
                st.markdown("**Audio file IDs from volume** (first 20):")
                st.code("\n".join(sorted(audio_ids)[:20]) if audio_ids else "— no audio files found —")

                st.divider()
                st.markdown("**Trace IDs from SQL table for this date** (no audio filter — first 20):")
                try:
                    w = get_client()
                    raw_traces = w.statement_execution.execute_statement(
                        warehouse_id=WAREHOUSE_ID,
                        # statement=f"SELECT trace_id FROM {TRACES_TABLE} WHERE event_date IN (DATE(:event_date)) ORDER BY trace_id DESC LIMIT 20",
                        statement=f"SELECT * FROM {TRACES_TABLE} ",

                        # parameters=[StatementParameterListItem(name="event_date", value=str(sql_date_debug))],
                        wait_timeout="30s",
                    )
                    if raw_traces.result and raw_traces.result.data_array:
                        table_ids = [row[0] for row in raw_traces.result.data_array]
                        st.code("\n".join(str(i) for i in table_ids))
                    else:
                        st.code("— no rows returned for this date —")
                except Exception as ex:
                    st.code(f"Query failed: {ex}")

                st.divider()
                st.markdown("**Raw sample from table** (no filters — first 5 rows, all columns):")
                try:
                    w = get_client()
                    raw = w.statement_execution.execute_statement(
                        warehouse_id=WAREHOUSE_ID,
                        statement=f"SELECT * FROM {TRACES_TABLE} LIMIT 5",
                        wait_timeout="30s",
                    )
                    if raw.result and raw.result.data_array:
                        cols = [c.name for c in raw.manifest.schema.columns]
                        st.code("Columns: " + ", ".join(cols))
                        for row in raw.result.data_array:
                            st.code(str(list(row)))
                    else:
                        st.code("— table is empty or inaccessible —")
                except Exception as ex:
                    st.code(f"Query failed: {ex}")

        else:
            st.success(
                f"✅ {len(df)} traces with audio  |  "
                f"{len(audio_ids)} total audio files  |  "
                f"Date: {selected_date_str[:4]}-{selected_date_str[4:6]}-{selected_date_str[6:]}"
            )
            st.divider()

            # ── Trace selection table ──────────────────────────────────────────
            st.subheader("📋 Trace List — Click a row to review")

            preview_df = df[["trace_id", "input"]].copy()
            preview_df["input"] = df["input"].fillna("").astype(str).str[:120] + "..."

            selected_rows = []   # default: nothing selected

            if SUPPORTS_ON_SELECT:
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
                        key=f"trace_table_{selected_date_str}_{len(df)}",
                    )
                    selected_rows = list(selection.selection.rows)
                except Exception as e:
                    st.dataframe(preview_df, use_container_width=True, hide_index=True)
                    st.warning(f"Row-click selection unavailable: {e}")
                    selected_rows = []
            else:
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
                    audio_bytes = get_audio(trace_id, selected_date_str)
                    if audio_bytes:
                        st.audio(audio_bytes, format="audio/wav")
                        st.caption(f"📁 `{VOLUME_PATH}/{selected_date_str}/{trace_id}.wav`")
                    else:
                        st.error(f"Audio file unexpectedly missing for `{trace_id}`")
                        st.caption(f"Expected: `{VOLUME_PATH}/{selected_date_str}/{trace_id}.wav`")

                with trace_col:
                    st.markdown("### 📝 Trace Details")
                    st.markdown("**🧑 User Query**")
                    st.info(
                        selected["input"]
                        if pd.notna(selected["input"])
                        else "_No input recorded_"
                    )
                    st.caption(f"🔑 Trace ID: `{trace_id}`")