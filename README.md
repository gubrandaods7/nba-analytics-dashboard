# nba-analytics-dashboard

NBA dashboard (2025–26) built with Python, nba_api, GCP and Streamlit

flowchart LR
  %% =========================
  %% Styles
  %% =========================
  classDef source fill:#f7f7f7,stroke:#333,stroke-width:1px;
  classDef etl fill:#e8f0fe,stroke:#1a73e8,stroke-width:1px;
  classDef storage fill:#e6f4ea,stroke:#1e8e3e,stroke-width:1px;
  classDef compute fill:#fff7e6,stroke:#f9ab00,stroke-width:1px;
  classDef bi fill:#fde7e9,stroke:#d93025,stroke-width:1px;

  %% =========================
  %% Nodes
  %% =========================
  NBA[(NBA Stats API)]:::source

  subgraph LOCAL["Ambiente de Execução (Windows/PowerShell/.venv)"]
    A1["ETL 01 — Ingestão<br/>etl/01_pull_raw.py<br/>• headers browser-like<br/>• SSL corporativo (CA)<br/>• staging local → upload"]:::etl
    A2["ETL 02 — Transformação<br/>etl/02_build_gold.py<br/>• download local → read<br/>• agregações (KPIs/Teams/Standings)<br/>• overwrite GOLD"]:::etl
    A3["Orquestração simples<br/>etl/03_backfill_seasons.py<br/>• loop seasons<br/>• tolera falhas"]:::etl
  end

  subgraph GCS["GCP — Google Cloud Storage (Data Lake)"]
    RAW["RAW (Parquet)<br/>gs://nba-data-gustavo/raw/<br/>season=YYYY-YY/endpoint=.../asof=YYYY-MM-DD/data.parquet<br/><i>snapshots imutáveis</i>"]:::storage
    GOLD["GOLD (Parquet)<br/>gs://nba-data-gustavo/gold/<br/>season=YYYY-YY/<br/>kpis.parquet<br/>team_totals.parquet<br/>standings.parquet<br/><i>latest por season (sobrescrito)</i>"]:::storage
  end

  subgraph CONSUMO["Camada de Consumo"]
    ST["Streamlit Dashboard<br/>app/app.py<br/>• lê apenas GOLD<br/>• cache (st.cache_data)<br/>• download local → read"]:::bi
    NB["Vertex AI Workbench (Jupyter)<br/>• validação (sanity checks)<br/>• EDA / estatística / regressões<br/>• prototipagem de métricas"]:::compute
  end

  %% =========================
  %% Flows
  %% =========================
  NBA -->|"HTTPS (anti-bot + SSL tratado)"| A1
  A1 -->|"Parquet (upload)"| RAW
  RAW -->|"download local + processamento batch"| A2
  A2 -->|"Parquet (upload)"| GOLD

  GOLD -->|"read-only (download local + read)"| ST
  GOLD -->|"read (pandas/pyarrow)"| NB
  RAW -->|"read (para auditoria / drill-down)"| NB

  A3 -->|"chama 01/02 em loop"| A1
  A3 -->|"chama 01/02 em loop"| A2
