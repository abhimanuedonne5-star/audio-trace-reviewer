# Audio + Trace Review Dashboard

A Databricks App built with Streamlit that lets reviewers manually verify that a recorded audio file matches the text input captured in a traces table — side by side, in the browser.

---

## Purpose

When a voice-based system records a user's spoken query, two things are stored separately:

| What | Where |
|---|---|
| The transcript / text input | `dev_omni.dev_omni_gold.traces` (Unity Catalog table) |
| The raw audio recording | `dev_omni.dev_omni_gold.audio_files` (Unity Catalog volume) |

Both are linked by the same **Trace ID** — the audio file is named `{trace_id}.wav`.

This app joins the two together so a human reviewer can:
1. See all traces that have a matching audio file
2. Click a trace to open the detail view
3. Play the audio and read the text input side by side
4. Manually confirm whether they match

---

## Architecture

```
Databricks Workspace
│
├── Unity Catalog
│   ├── Table: dev_omni.dev_omni_gold.traces
│   │         (trace_id, input)
│   │
│   └── Volume: dev_omni.dev_omni_gold.audio_files
│               {trace_id}.wav files
│
└── Databricks App (Streamlit)
    │
    ├── Databricks SDK (WorkspaceClient)
    │   ├── statement_execution  → queries the traces table via SQL Warehouse
    │   └── files API            → lists and downloads audio files from the volume
    │
    └── Streamlit UI
        ├── Sidebar (search)
        ├── Filtered trace list (table)
        └── Detail panel (audio player + query text)
```

---

## How It Works — Step by Step

### Step 1: Authentication

```python
@st.cache_resource
def get_client():
    return WorkspaceClient()
```

`WorkspaceClient()` with no arguments automatically reads credentials from the environment. In Databricks Apps, the platform injects the app's service principal credentials at startup — no tokens or passwords need to be hardcoded.

`@st.cache_resource` means the client is created **once per app instance** and reused across all users and reruns, avoiding repeated authentication overhead.

---

### Step 2: Load data in parallel

On every page load, two data sources are fetched simultaneously:

#### 2a. Traces table (SQL)

```python
@st.cache_data(ttl=60)
def fetch_all_traces():
    w = get_client()
    response = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement="SELECT trace_id, input FROM traces ORDER BY trace_id DESC",
        wait_timeout="30s"
    )
    ...
    return df, None          # returns (DataFrame, error_string)
```

- Uses the **Databricks Statement Execution API** to run a SQL query against the Unity Catalog table via the configured SQL Warehouse.
- Returns every `trace_id` and its `input` text.
- `@st.cache_data(ttl=60)` caches the result for 60 seconds so the warehouse is not queried on every user interaction — only on fresh page loads after the TTL expires.
- Returns a `(DataFrame, error_string)` tuple. The function never calls `st.error()` internally — errors are surfaced to the UI at the call site, which is required because `@st.cache_data` functions must be pure (no side effects).

#### 2b. Audio file list (Files API)

```python
@st.cache_data(ttl=60)
def get_audio_trace_ids():
    w = get_client()
    entries = list(w.files.list_directory_contents(VOLUME_PATH))
    wav_names = [e.name for e in entries if e.name.lower().endswith(".wav")]
    return frozenset(os.path.splitext(n)[0] for n in wav_names), None
```

- Uses the **Databricks Files API** (`w.files.list_directory_contents`) to list all files in the Unity Catalog volume.
- This is done via authenticated HTTP, **not** via the filesystem path `/Volumes/...`. Databricks Apps does not guarantee a FUSE mount for volumes, so direct `os.listdir()` on `/Volumes/` fails. The SDK Files API always works.
- Returns a `frozenset` of trace IDs (filenames with `.wav` stripped), also cached for 60 seconds.

---

### Step 3: Filter — only traces with audio

```python
df = df_all[df_all["trace_id"].isin(audio_ids)].reset_index(drop=True)
```

The two data sources are **joined in memory** using a set lookup:
- `df_all` has every trace from the table (could be thousands)
- `audio_ids` is the set of trace IDs that have a `.wav` file
- `isin()` keeps only rows where the trace ID appears in both — the intersection

This means the table shown to the reviewer **only contains traces that can actually be reviewed** (both text and audio are available).

---

### Step 4: Display the trace list

```python
selection = st.dataframe(
    preview_df,
    on_select="rerun",
    selection_mode="single-row",
    key=f"trace_table_{len(df)}",
)
```

- Shows a table with Trace ID and a 120-character preview of the input text.
- `on_select="rerun"` makes the entire page re-render when the user clicks a row, passing the selected row index back to the script.
- `key=f"trace_table_{len(df)}"` — the widget key changes whenever the number of rows changes (e.g. after a search or cache refresh). This forces Streamlit to reset the widget's selection state, preventing stale row indices from pointing to the wrong trace.
- Requires Streamlit ≥ 1.35. A `selectbox` fallback is used for older versions.

**Important:** `st.stop()` is not used anywhere in this app. In Databricks Apps, `st.stop()` does not reliably halt script execution. All branching uses standard `if/else` instead.

---

### Step 5: Detail panel — audio + query

When a row is selected:

```python
current_idx = int(selected_rows[0])
selected    = df.iloc[current_idx]
trace_id    = selected["trace_id"]
```

The index is validated (`0 <= current_idx < len(df)`) before any access to prevent `IndexError` when the dataset changes between cache refreshes.

#### Audio playback

```python
def get_audio(trace_id: str):
    w = get_client()
    response = w.files.download(f"{VOLUME_PATH}/{trace_id}.wav")
    return response.contents.read()
```

- Downloads the audio file **on demand** (only when a row is selected) using `w.files.download()` — again via authenticated HTTP, not the filesystem.
- Returns raw bytes, which are passed directly to `st.audio()` for in-browser playback.

#### Query text

```python
st.info(selected["input"] if pd.notna(selected["input"]) else "_No input recorded_")
```

The full `input` text is shown alongside the audio player. `pd.notna()` guards against NULL values in the database.

---

## File Structure

```
audio-trace-reviewer/
├── app.py              # Main Streamlit application
├── app.yaml            # Databricks Apps startup configuration
└── requirements.txt    # Python dependencies
```

### app.yaml

```yaml
command:
  - streamlit
  - run
  - app.py
  - --server.address=0.0.0.0
  - --server.headless=true
```

This file is **required**. Without it, Databricks Apps defaults to running `python app.py` instead of `streamlit run app.py`, which produces a "missing ScriptRunContext" crash with no useful error in the logs.

### requirements.txt

```
streamlit>=1.35.0       # on_select="rerun" requires 1.35+
databricks-sdk>=0.20.0  # Files API and statement_execution
pandas>=1.5.0
```

---

## Databricks Apps Configuration

### Resources to add in the App UI

| Key | Type | Detail | Permission |
|---|---|---|---|
| `volume` | UC Volume | `/Volumes/dev_omni/dev_omni_gold/audio_files` | Can read |
| `sql-warehouse` | SQL Warehouse | Serverless Starter Warehouse | Can use |
| `table` | UC Table | `dev_omni.dev_omni_gold.traces` | Can select |

### Warehouse ID

The warehouse ID is read from the environment variable `DATABRICKS_WAREHOUSE_ID`, which Databricks Apps injects automatically when a SQL warehouse resource is configured:

```python
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "2a6b5b84e8974695")
```

The hardcoded value is a fallback for local development only.

### Finding your service principal

Go to your Databricks workspace → **Apps** (left sidebar) → click your app → **Permissions** tab. The service principal name is listed there.

To grant access manually (if needed):
```sql
GRANT USE CATALOG  ON CATALOG dev_omni                          TO `<service-principal>`;
GRANT USE SCHEMA   ON SCHEMA  dev_omni.dev_omni_gold            TO `<service-principal>`;
GRANT SELECT       ON TABLE   dev_omni.dev_omni_gold.traces     TO `<service-principal>`;
GRANT READ VOLUME  ON VOLUME  dev_omni.dev_omni_gold.audio_files TO `<service-principal>`;
```

---

## Data Flow Diagram

```
User opens app
      │
      ▼
 fetch_all_traces()          get_audio_trace_ids()
 [cached 60s]                [cached 60s]
      │                            │
      │  SQL via Statement          │  Files API (HTTP)
      │  Execution API              │  list_directory_contents()
      │                            │
      ▼                            ▼
 DataFrame                   frozenset of
 (trace_id, input)           trace IDs with .wav
      │                            │
      └──────────┬─────────────────┘
                 │
                 ▼
         df[trace_id.isin(audio_ids)]
         (intersection — only reviewable traces)
                 │
                 ▼
         Search filter (optional)
                 │
                 ▼
         st.dataframe  ←── user clicks a row
                 │
                 ▼
         get_audio(trace_id)
         [Files API: files.download()]
                 │
                 ▼
         st.audio()  +  st.info(input text)
         (side-by-side review panel)
```

---

## Caching Strategy

| Function | Cache type | TTL | Why |
|---|---|---|---|
| `get_client()` | `@st.cache_resource` | Forever (per process) | SDK client is expensive to create; shared across all users |
| `fetch_all_traces()` | `@st.cache_data` | 60 seconds | Avoids querying the warehouse on every click |
| `get_audio_trace_ids()` | `@st.cache_data` | 60 seconds | Avoids listing the volume directory on every click |
| `get_audio()` | No cache | — | Downloaded on demand per selected trace; caching binary audio data in memory is wasteful |

---

## Modifying the App

### Change the data source
Edit the constants at the top of `app.py`:
```python
VOLUME_PATH  = "/Volumes/<catalog>/<schema>/<volume>"
TRACES_TABLE = "<catalog>.<schema>.<table>"
```

### Change the cache refresh interval
```python
@st.cache_data(ttl=60)   # change 60 to any number of seconds
```

### Show more columns from the traces table
Add columns to the SQL query in `fetch_all_traces()` and include them in the `preview_df` and detail panel rendering.
