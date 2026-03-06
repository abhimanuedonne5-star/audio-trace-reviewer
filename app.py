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
# LIST AUDIO FILES via Databricks SDK Files API for a date range
# Iterates each date directory in the range and maps trace_id → date_str.
# Returns (dict[trace_id -> date_str], error_string)
# ─────────────────────────────────────────────
@st.cache_data(ttl=60)
def get_audio_trace_ids_for_range(start_str: str, end_str: str, available_dates: tuple):
    w = get_client()
    dates_in_range = [d for d in available_dates if start_str <= d <= end_str]

    if not dates_in_range:
        return {}, f"No volume directories found between {start_str} and {end_str}."

    id_to_date = {}
    errors = []
    for date_str in dates_in_range:
        path = f"{VOLUME_PATH}/{date_str}"
        try:
            entries = list(w.files.list_directory_contents(path))
            for e in entries:
                if e.name and e.name.lower().endswith(".wav") and not e.is_directory:
                    tid = os.path.splitext(e.name)[0]
                    id_to_date[tid] = date_str
        except Exception as e:
            errors.append(f"{date_str}: {e}")

    if not id_to_date and errors:
        return {}, "; ".join(errors)
    return id_to_date, None

# ─────────────────────────────────────────────
# FETCH TRACES FROM TABLE — only for IDs that have audio files
# Returns (DataFrame, error_string) — no st.* calls inside (cache must be pure)
# ─────────────────────────────────────────────
@st.cache_data(ttl=60)
def fetch_traces_for_audio_ids(start_str: str, end_str: str, audio_ids: frozenset):
    if not audio_ids:
        return pd.DataFrame(columns=["trace_id", "input"]), None

    w = get_client()
    start_date = datetime.strptime(start_str, "%Y%m%d").date()
    end_date   = datetime.strptime(end_str,   "%Y%m%d").date()

    # Build IN list — IDs come from our own volume listing, sanitize to be safe
    id_list = ", ".join(f"'{tid.replace(chr(39), '')}'" for tid in audio_ids)
    query = f"""
        SELECT trace_id, input
        FROM {TRACES_TABLE}
        WHERE event_date BETWEEN DATE(:start_date) AND DATE(:end_date)
          AND TRIM(trace_id) IN ({id_list})
        ORDER BY trace_id DESC
    """

    try:
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            parameters=[
                StatementParameterListItem(name="start_date", value=str(start_date)),
                StatementParameterListItem(name="end_date",   value=str(end_date)),
            ],
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
    st.header("📅 Date Range Filter")

    available_dates, dates_error = get_available_dates()

    def _fmt(d):
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"

    if available_dates:
        start_date_str = st.selectbox(
            "Start Date",
            options=available_dates,
            index=len(available_dates) - 1,   # oldest available
            format_func=_fmt,
            key="start_date_selector",
        )
        end_date_str = st.selectbox(
            "End Date",
            options=available_dates,
            index=0,                           # most recent available
            format_func=_fmt,
            key="end_date_selector",
        )
        if start_date_str > end_date_str:
            st.warning("Start date must be on or before end date.")
            start_date_str, end_date_str = None, None
    else:
        if dates_error:
            st.warning(f"Could not list dates from volume: {dates_error}")
        # Fall back to manual date inputs
        picked_start = st.date_input("Start Date", value=None, key="start_date_fallback")
        picked_end   = st.date_input("End Date",   value=None, key="end_date_fallback")
        start_date_str = picked_start.strftime("%Y%m%d") if picked_start else None
        end_date_str   = picked_end.strftime("%Y%m%d")   if picked_end   else None
        if start_date_str and end_date_str and start_date_str > end_date_str:
            st.warning("Start date must be on or before end date.")
            start_date_str, end_date_str = None, None

    st.divider()
    st.header("🔍 Search")
    search = st.text_input("Search by Trace ID or Query", placeholder="e.g. trace_001")

# ─────────────────────────────────────────────
# LOAD DATA — only after a date is selected
# ─────────────────────────────────────────────
if not (start_date_str and end_date_str):
    st.info("👈 Select a date range from the sidebar to load traces.")

else:
    with st.spinner("Loading audio file list..."):
        audio_id_to_date, volume_error = get_audio_trace_ids_for_range(
            start_date_str, end_date_str, tuple(available_dates)
        )
    audio_ids = frozenset(audio_id_to_date.keys())

    with st.spinner("Loading traces..."):
        df_all, fetch_error = fetch_traces_for_audio_ids(start_date_str, end_date_str, audio_ids)

    # ─────────────────────────────────────────────
    # ERROR STATES  (no st.stop() — if/else only)
    # ─────────────────────────────────────────────
    if volume_error:
        st.error(f"❌ Cannot read audio volume: {volume_error}")
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
                start_date_debug = datetime.strptime(start_date_str, "%Y%m%d").date()
                end_date_debug   = datetime.strptime(end_date_str,   "%Y%m%d").date()
                st.markdown("**Audio file IDs from volume** (first 20):")
                st.code("\n".join(sorted(audio_ids)[:20]) if audio_ids else "— no audio files found —")

                st.divider()
                st.markdown("**Trace IDs from SQL table for this date range** (no audio filter — first 20):")
                try:
                    w = get_client()
                    raw_traces = w.statement_execution.execute_statement(
                        warehouse_id=WAREHOUSE_ID,
                        statement=f"SELECT trace_id FROM {TRACES_TABLE} WHERE event_date BETWEEN DATE(:start_date) AND DATE(:end_date) ORDER BY trace_id DESC LIMIT 20",
                        parameters=[
                            StatementParameterListItem(name="start_date", value=str(start_date_debug)),
                            StatementParameterListItem(name="end_date",   value=str(end_date_debug)),
                        ],
                        wait_timeout="30s",
                    )
                    if raw_traces.result and raw_traces.result.data_array:
                        table_ids = [row[0] for row in raw_traces.result.data_array]
                        st.code("\n".join(str(i) for i in table_ids))
                    else:
                        st.code("— no rows returned for this date range —")
                except Exception as ex:
                    st.code(f"Query failed: {ex}")

        else:
            st.success(
                f"✅ {len(df)} traces with audio  |  "
                f"{len(audio_ids)} total audio files  |  "
                f"Range: {_fmt(start_date_str)} → {_fmt(end_date_str)}"
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
                        key=f"trace_table_{start_date_str}_{end_date_str}_{len(df)}",
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
                    audio_date_str = audio_id_to_date.get(trace_id, end_date_str)
                    audio_bytes = get_audio(trace_id, audio_date_str)
                    if audio_bytes:
                        st.audio(audio_bytes, format="audio/wav")
                    else:
                        st.error(f"Audio file unexpectedly missing for `{trace_id}`")

                with trace_col:
                    st.markdown("### 📝 Trace Details")
                    st.markdown("**🧑 User Query**")
                    st.info(
                        selected["input"]
                        if pd.notna(selected["input"])
                        else "_No input recorded_"
                    )
                    st.caption(f"🔑 Trace ID: `{trace_id}`")