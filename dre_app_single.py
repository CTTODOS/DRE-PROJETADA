
# -*- coding: utf-8 -*-
"""
DRE - Cartão de TODOS (Single-file App) - v2
--------------------------------------------
Rodar com:
    pip install streamlit pandas numpy plotly python-dateutil openpyxl
    streamlit run dre_app_single.py

Opcional:
    export DRE_DB_PATH=dre_app.db
"""

import os, io, re, json, hashlib, sqlite3
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import streamlit as st

# ===============================
# Config & Theme
# ===============================
st.set_page_config(page_title="DRE - Cartão de TODOS", layout="wide")

BRAND_PRIMARY = "#00A859"   # Ajuste conforme brand
BRAND_SECONDARY = "#FFD100"
BRAND_DARK = "#0E3B2E"

st.markdown(f"""
<style>
:root {{
  --brand-primary: {BRAND_PRIMARY};
  --brand-secondary: {BRAND_SECONDARY};
  --brand-dark: {BRAND_DARK};
}}
div.stButton > button {{
  background-color: var(--brand-primary);
  color: white;
  border-radius: 10px;
  padding: 0.5rem 1rem;
  border: none;
}}
div.stButton > button:hover {{
  background-color: var(--brand-dark);
}}
[data-testid="stHeader"] {{
  background: linear-gradient(90deg, var(--brand-primary), var(--brand-secondary));
}}
.block-container {{
  padding-top: 1rem;
}}
</style>
""", unsafe_allow_html=True)

# ===============================
# Utility: Banner if running without 'streamlit run'
# ===============================
def _bare_mode_banner():
    # Se for executado com 'python dre_app_single.py', alerta para usar streamlit run
    import sys
    if len(sys.argv) > 0 and sys.argv[0].endswith(".py"):
        # Apenas um aviso simples (não bloqueia)
        st.warning("⚠️ Abra este app com **`streamlit run dre_app_single.py`** para a melhor experiência.")

# ===============================
# Database (SQLite)
# ===============================
DB_PATH = os.environ.get("DRE_DB_PATH", "dre_app.db")

SCHEMA_SQL = """\
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS units (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS imports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  unit_id INTEGER NOT NULL,
  period_year INTEGER NOT NULL,
  period_month INTEGER NOT NULL,
  type TEXT NOT NULL, -- 'notas' | 'detalhamento'
  source_file TEXT,
  uploaded_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (unit_id) REFERENCES units(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS notas_raw (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  import_id INTEGER NOT NULL,
  data TEXT,
  ano INTEGER,
  mes INTEGER,
  arrecadadora TEXT,
  codigo TEXT,
  meio_pagamento TEXT,
  historico TEXT,
  valor REAL,
  source_file TEXT,
  raw_json TEXT,
  row_hash TEXT UNIQUE,
  FOREIGN KEY (import_id) REFERENCES imports(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS detalhamento_raw (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  import_id INTEGER NOT NULL,
  data TEXT,
  ano INTEGER,
  mes INTEGER,
  conta TEXT,
  subconta TEXT,
  historico TEXT,
  valor REAL,
  source_file TEXT,
  raw_json TEXT,
  row_hash TEXT UNIQUE,
  FOREIGN KEY (import_id) REFERENCES imports(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS code_map (
  code TEXT PRIMARY KEY,
  kind TEXT NOT NULL,    -- 'BRUTA' | 'DEDUCAO' | 'OUTRO'
  sign INTEGER NOT NULL  -- 1 or -1
);

CREATE TABLE IF NOT EXISTS account_group_map (
  conta TEXT PRIMARY KEY,
  grupo TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS hist_edit_rules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  unit_id INTEGER, -- nullable = applies to all units
  match_field TEXT NOT NULL,   -- 'historico' | 'subconta' | 'conta'
  match_type TEXT NOT NULL,    -- 'equals' | 'contains' | 'regex'
  match_value TEXT NOT NULL,
  new_historico TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (unit_id) REFERENCES units(id) ON DELETE CASCADE
);

-- Log de ingestão por arquivo (para detectar duplicidades por hash de conteúdo)
CREATE TABLE IF NOT EXISTS ingest_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  unit_id INTEGER NOT NULL,
  period_year INTEGER NOT NULL,
  period_month INTEGER NOT NULL,
  type TEXT NOT NULL, -- 'notas' | 'detalhamento'
  file_name TEXT,
  file_hash TEXT,
  rows_inserted INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE(unit_id, period_year, period_month, type, file_hash),
  FOREIGN KEY (unit_id) REFERENCES units(id) ON DELETE CASCADE
);

"""

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA_SQL)
    seed_code_map()

def ensure_db():
    """Create or recreate the DB if missing or corrupted; always ensure schema."""
    try:
        if not os.path.exists(DB_PATH):
            init_db()
            return
        with get_conn() as conn:
            conn.execute("SELECT 1;")
            conn.executescript(SCHEMA_SQL)
            seed_code_map()
    except Exception:
        try:
            if os.path.exists(DB_PATH):
                os.remove(DB_PATH)
        except Exception:
            pass
        init_db()

def seed_code_map():
    codes = {
        "VAM": ("BRUTA", 1),
        "TAC": ("BRUTA", 1),
        "CAR": ("BRUTA", 1),
        "VRA": ("DEDUCAO", -1),
        "VIR": ("DEDUCAO", -1),
        "DRF": ("DEDUCAO", -1),
        "AIR": ("DEDUCAO", -1),
        "DAR": ("DEDUCAO", -1),
        "DEL": ("DEDUCAO", -1),
        "AJT": ("DEDUCAO", -1),
        "SAF": ("DEDUCAO", -1),
        "DAS": ("DEDUCAO", -1),
        "FPP": ("DEDUCAO", -1),
    }
    with get_conn() as conn:
        for code, (kind, sign) in codes.items():
            conn.execute("""INSERT OR IGNORE INTO code_map (code, kind, sign) VALUES (?, ?, ?)""", (code, kind, sign))
        conn.commit()

def upsert_unit(name):
    with get_conn() as conn:
        conn.execute("INSERT OR IGNORE INTO units (name) VALUES (?)", (name.strip(),))
        conn.commit()
        cur = conn.execute("SELECT id FROM units WHERE name = ?", (name.strip(),))
        row = cur.fetchone()
        return row[0] if row else None

def list_units():
    with get_conn() as conn:
        cur = conn.execute("SELECT id, name, active FROM units ORDER BY name")
        return [{"id": r[0], "name": r[1], "active": r[2]} for r in cur.fetchall()]

def add_import(unit_id, year, month, typ, source_file=None):
    with get_conn() as conn:
        cur = conn.execute("""INSERT INTO imports (unit_id, period_year, period_month, type, source_file)
                              VALUES (?, ?, ?, ?, ?)""", (unit_id, year, month, typ, source_file))
        conn.commit()
        return cur.lastrowid

def _hash_row(d: dict):
    s = json.dumps(d, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def insert_notas_rows(import_id, rows):
    with get_conn() as conn:
        for r in rows:
            row_hash = _hash_row(r)
            try:
                conn.execute("""INSERT INTO notas_raw
                                (import_id, data, ano, mes, arrecadadora, codigo, meio_pagamento, historico, valor, source_file, raw_json, row_hash)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                             (import_id, r.get("data"), r.get("ano"), r.get("mes"),
                              r.get("arrecadadora"), r.get("codigo"), r.get("meio_pagamento"),
                              r.get("historico"), r.get("valor"), r.get("source_file"),
                              json.dumps(r, ensure_ascii=False), row_hash))
            except sqlite3.IntegrityError:
                pass
        conn.commit()

def insert_detalhamento_rows(import_id, rows):
    with get_conn() as conn:
        for r in rows:
            row_hash = _hash_row(r)
            try:
                conn.execute("""INSERT INTO detalhamento_raw
                                (import_id, data, ano, mes, conta, subconta, historico, valor, source_file, raw_json, row_hash)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                             (import_id, r.get("data"), r.get("ano"), r.get("mes"),
                              r.get("conta"), r.get("subconta"), r.get("historico"),
                              r.get("valor"), r.get("source_file"),
                              json.dumps(r, ensure_ascii=False), row_hash))
            except sqlite3.IntegrityError:
                pass
        conn.commit()

def apply_hist_rules(df, unit_id=None):
    with get_conn() as conn:
        if unit_id is None:
            cur = conn.execute("SELECT unit_id, match_field, match_type, match_value, new_historico FROM hist_edit_rules")
        else:
            cur = conn.execute("""SELECT unit_id, match_field, match_type, match_value, new_historico
                                  FROM hist_edit_rules WHERE unit_id IS NULL OR unit_id = ?""", (unit_id,))
        rules = cur.fetchall()
    if not len(rules):
        return df
    df = df.copy()
    for unit_id_rule, mfield, mtype, mvalue, newhist in rules:
        field = mfield if mfield in df.columns else None
        if not field:
            continue
        if mtype == "equals":
            mask = df[field].astype(str) == str(mvalue)
        elif mtype == "contains":
            mask = df[field].astype(str).str.contains(str(mvalue), case=False, na=False)
        elif mtype == "regex":
            mask = df[field].astype(str).str.contains(mvalue, flags=re.I, regex=True, na=False)
        else:
            continue
        df.loc[mask, "historico"] = newhist
    return df

def add_hist_rule(unit_id, match_field, match_type, match_value, new_historico):
    with get_conn() as conn:
        conn.execute("""INSERT INTO hist_edit_rules (unit_id, match_field, match_type, match_value, new_historico)
                        VALUES (?, ?, ?, ?, ?)""", (unit_id, match_field, match_type, match_value, new_historico))
        conn.commit()

def upsert_account_group(conta, grupo):
    with get_conn() as conn:
        conn.execute("""INSERT INTO account_group_map (conta, grupo)
                        VALUES (?, ?)
                        ON CONFLICT(conta) DO UPDATE SET grupo=excluded.grupo, updated_at=datetime('now')""",
                     (conta, grupo))
        conn.commit()

def load_account_groups():
    with get_conn() as conn:
        return pd.read_sql_query("SELECT conta, grupo FROM account_group_map", conn)

def is_duplicate_ingest(unit_id, year, month, typ, file_hash):
    with get_conn() as conn:
        cur = conn.execute("""
            SELECT 1 FROM ingest_log
             WHERE unit_id = ? AND period_year = ? AND period_month = ?
               AND type = ? AND file_hash = ?
            LIMIT 1
        """, (unit_id, year, month, typ, file_hash))
        return cur.fetchone() is not None

def record_ingest(unit_id, year, month, typ, file_name, file_hash, rows_inserted):
    with get_conn() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO ingest_log
            (unit_id, period_year, period_month, type, file_name, file_hash, rows_inserted)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (unit_id, year, month, typ, file_name, file_hash, rows_inserted))
        conn.commit()

def query_kpis(unit_id, year, month):
    with get_conn() as conn:
        q = """
        SELECT n.arrecadadora, n.codigo, cm.kind, cm.sign, SUM(COALESCE(n.valor,0)) as valor
        FROM notas_raw n
        LEFT JOIN code_map cm ON UPPER(n.codigo)=cm.code
        INNER JOIN imports i ON i.id = n.import_id
        WHERE i.unit_id = ? AND i.period_year = ? AND i.period_month = ?
        GROUP BY n.arrecadadora, n.codigo, cm.kind, cm.sign
        """
        df = pd.read_sql_query(q, conn, params=(unit_id, year, month))
        bruta = df.loc[df["kind"]=="BRUTA", "valor"].sum() if not df.empty else 0.0
        deduc = df.loc[df["kind"]=="DEDUCAO", "valor"].sum() if not df.empty else 0.0
        liquida = bruta + deduc

        qd = """
        SELECT SUM(COALESCE(d.valor,0)) as despesas
        FROM detalhamento_raw d
        INNER JOIN imports i ON i.id = d.import_id
        WHERE i.unit_id = ? AND i.period_year = ? AND i.period_month = ?
        """
        dd = pd.read_sql_query(qd, conn, params=(unit_id, year, month))
        if dd.shape[0] == 0:
            despesas = 0.0
        else:
            raw = dd.iloc[0, 0]
            try:
                despesas = float(raw) if raw is not None and not pd.isna(raw) else 0.0
            except Exception:
                despesas = 0.0

        return bruta, deduc, liquida, despesas

def get_nota_period(unit_id, year, month):
    with get_conn() as conn:
        q = """
        SELECT n.*
        FROM notas_raw n
        INNER JOIN imports i ON i.id = n.import_id
        WHERE i.unit_id = ? AND i.period_year = ? AND i.period_month = ?
        """
        return pd.read_sql_query(q, conn, params=(unit_id, year, month))

def get_detalhamento_period(unit_id, year, month):
    with get_conn() as conn:
        q = """
        SELECT d.*
        FROM detalhamento_raw d
        INNER JOIN imports i ON i.id = d.import_id
        WHERE i.unit_id = ? AND i.period_year = ? AND i.period_month = ?
        """
        return pd.read_sql_query(q, conn, params=(unit_id, year, month))

# ===============================
# Helpers UI / Data
# ===============================
def to_decimal(s):
    if pd.isna(s): return np.nan
    s = str(s).strip()
    if s == "": return np.nan
    s2 = s.replace(".", "").replace(",", ".")
    try: return float(s2)
    except:
        try: return float(s)
        except: return np.nan

def normalize_col(c):
    return re.sub(r"\s+", " ", str(c).strip())

def detect_col(df, patterns):
    pats = [re.compile(p, re.I) for p in patterns]
    for c in df.columns:
        for p in pats:
            if p.search(c):
                return c
    return None


def get_file_bytes_and_hash(uploaded_file):
    import io, hashlib
    try:
        data = uploaded_file.getvalue()
    except Exception:
        data = uploaded_file.read()
    # Reset pointer if possible
    try:
        uploaded_file.seek(0)
    except Exception:
        pass
    h = hashlib.sha256(data).hexdigest()
    return data, h

def read_csv_flex(file_obj):
    seps = [None, ";", ",", "\t", "|"]
    encs = ["utf-8", "utf-8-sig", "latin1", "cp1252"]
    last_err = None
    for sep in seps:
        for enc in encs:
            try:
                df = pd.read_csv(file_obj, sep=sep, encoding=enc, engine="python")
                df = df.drop(columns=[c for c in df.columns if str(c).startswith("Unnamed")], errors="ignore")
                return df
            except Exception as e:
                last_err = e
                continue
    raise RuntimeError(f"Falha ao ler CSV: {last_err}")

def hash_row(d):
    s = json.dumps(d, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

# ===============================
# Init DB (first-run) and Title
# ===============================
if "db_inited" not in st.session_state:
    with st.spinner("Inicializando base de dados..."):
        ensure_db()
        st.session_state["db_inited"] = True

_bare_mode_banner()
st.title("DRE — Cartão de TODOS")

# ===============================
# Sidebar - Units & Period
# ===============================
st.sidebar.header("Unidades")
with st.sidebar.expander("Cadastrar nova unidade", expanded=False):
    new_unit_name = st.text_input("Nome da unidade", key="new_unit")
    if st.button("Cadastrar unidade"):
        if new_unit_name.strip():
            uid = upsert_unit(new_unit_name.strip())
            st.success(f"Unidade cadastrada: {new_unit_name}")
            st.rerun()
        else:
            st.warning("Informe um nome válido.")

units = list_units()
if not units:
    default_name = "Unidade Padrão"
    upsert_unit(default_name)
    units = list_units()
    st.sidebar.success(f'Criada automaticamente: **{default_name}**')

unit_names = [u["name"] for u in units]
unit_map = {u["name"]: u["id"] for u in units}
selected_unit_name = st.sidebar.selectbox("Selecionar unidade", unit_names, index=0 if unit_names else None)
if not selected_unit_name and unit_names:
    selected_unit_name = unit_names[0]
selected_unit_id = unit_map.get(selected_unit_name)
if selected_unit_id is None:
    st.sidebar.error("Não foi possível determinar a unidade selecionada. Execute com `streamlit run` e verifique o ambiente.")
    st.stop()

st.sidebar.markdown("---")
st.sidebar.header("Período")
colp1, colp2 = st.sidebar.columns(2)
year = colp1.selectbox("Ano", list(range(2023, 2027)), index=2)
month = colp2.selectbox("Mês", list(range(1,13)), index=5, format_func=lambda m: f"{m:02d}")

# ===============================
# Sidebar - Import & Maintenance
# ===============================
st.sidebar.markdown("---")
st.sidebar.header("Importação de Planilhas")
uploaded_nns = st.sidebar.file_uploader("Notas de Negócio (múltiplos CSVs)", type=["csv"], accept_multiple_files=True, key="nns")
uploaded_det = st.sidebar.file_uploader("Detalhamento Financeiro (CSV)", type=["csv"], accept_multiple_files=False, key="det")

with st.sidebar.expander("Manutenção do Banco", expanded=False):
    if st.button("Resetar Banco (apagar e recriar)", type="secondary"):
        try:
            if os.path.exists(DB_PATH):
                os.remove(DB_PATH)
            ensure_db()
            st.success("Banco resetado com sucesso.")
            st.rerun()
        except Exception as e:
            st.error(f"Falha ao resetar banco: {e}")

# ===============================
# Import functions
# ===============================

def import_notas(files):
    if not files: return None
    imp_id = add_import(selected_unit_id, year, month, "notas", source_file="multiple")
    rows_to_insert = []
    duplicates = []
    processed = 0

    for f in files:
        # Hash by content to detect duplicate uploads even if filename changed
        data, fh = get_file_bytes_and_hash(f)
        if is_duplicate_ingest(selected_unit_id, year, month, "notas", fh):
            duplicates.append(f.name if hasattr(f, "name") else "(arquivo sem nome)")
            continue

        # Parse with a fresh BytesIO to avoid pointer issues
        import io as _io
        df = read_csv_flex(_io.BytesIO(data))
        df.columns = [normalize_col(c) for c in df.columns]
        col_arrec = detect_col(df, [r"arrecad", r"arrecadadora", r"arrecada(dor|dora)"])
        col_codigo = detect_col(df, [r"^cod", r"codigo", r"c[oó]digo", r"tipo.*(repasse|receita|mov)"])
        col_valor = detect_col(df, [r"^valor", r"vlr", r"montante", r"total"])
        col_data = detect_col(df, [r"data", r"compet[eê]ncia", r"emiss[aã]o"])
        col_meio = detect_col(df, [r"meio", r"forma.*pag", r"bandeira", r"canal"])
        col_hist = detect_col(df, [r"hist[oó]rico", r"descri", r"observa", r"memo", r"detalhe"])

        val = df[col_valor].apply(to_decimal) if col_valor else np.nan
        dat = pd.to_datetime(df[col_data], errors="coerce", dayfirst=True, infer_datetime_format=True) if col_data else pd.NaT
        ano = dat.dt.year if col_data else year
        mes = dat.dt.month if col_data else month

        before = len(rows_to_insert)
        for idx, r in df.iterrows():
            row = {
                "data": (dat.iloc[idx].strftime("%Y-%m-%d") if col_data and not pd.isna(dat.iloc[idx]) else f"{year}-{month:02d}-01"),
                "ano": int(ano.iloc[idx]) if col_data and not pd.isna(ano.iloc[idx]) else year,
                "mes": int(mes.iloc[idx]) if col_data and not pd.isna(mes.iloc[idx]) else month,
                "arrecadadora": (r[col_arrec] if col_arrec else None),
                "codigo": (str(r[col_codigo]).upper().strip() if col_codigo and not pd.isna(r[col_codigo]) else None),
                "meio_pagamento": (r[col_meio] if col_meio else None),
                "historico": (r[col_hist] if col_hist else None),
                "valor": float(val.iloc[idx]) if (isinstance(val, pd.Series) and not pd.isna(val.iloc[idx])) else None,
                "source_file": f.name if hasattr(f, "name") else "uploaded"
            }
            row["row_hash"] = _hash_row(row)
            rows_to_insert.append(row)
        inserted_rows = len(rows_to_insert) - before
        record_ingest(selected_unit_id, year, month, "notas", (f.name if hasattr(f, "name") else None), fh, inserted_rows)
        processed += 1

    if rows_to_insert:
        insert_notas_rows(imp_id, rows_to_insert)

    # Feedback
    if duplicates:
            st.warning("Arquivos ignorados por duplicidade ({len(duplicates)}): " + ", ".join(duplicates))
    if len(rows_to_insert) == 0:
        st.warning("Nenhuma linha nova foi inserida. Verifique o formato do arquivo ou se já foi importado antes.")
    else:
        st.success(f"Notas processadas: {processed}. Linhas novas inseridas: {len(rows_to_insert)}.")
    return imp_id



def import_detalhamento(file_obj):
    if not file_obj: return None
    data, fh = get_file_bytes_and_hash(file_obj)
    if is_duplicate_ingest(selected_unit_id, year, month, "detalhamento", fh):
        st.warning(f"Detalhamento ignorado: arquivo duplicado ({getattr(file_obj, 'name', 'sem nome')}).")
        return None

    imp_id = add_import(selected_unit_id, year, month, "detalhamento", source_file=getattr(file_obj, "name", None))
    import io as _io
    df = read_csv_flex(_io.BytesIO(data))
    df.columns = [normalize_col(c) for c in df.columns]
    col_conta = detect_col(df, [r"conta", r"plano.*contas", r"r[oó]tulo.*linha", r"categoria", r"centro.*custo", r"descri"])
    col_valor = detect_col(df, [r"valor", r"vlr", r"montante", r"total"])
    col_data = detect_col(df, [r"data", r"compet", r"m[eê]s", r"periodo"])
    col_hist = detect_col(df, [r"hist[oó]rico", r"obs", r"memo", r"detalhe", r"descri"])
    col_sub = detect_col(df, [r"subconta", r"sub-conta", r"classe", r"subcategoria"])

    val = df[col_valor].apply(to_decimal) if col_valor else np.nan
    dat = pd.to_datetime(df[col_data], errors="coerce", dayfirst=True, infer_datetime_format=True) if col_data else pd.NaT
    ano = dat.dt.year if col_data else year
    mes = dat.dt.month if col_data else month

    rows = []
    for idx, r in df.iterrows():
        row = {
            "data": (dat.iloc[idx].strftime("%Y-%m-%d") if col_data and not pd.isna(dat.iloc[idx]) else f"{year}-{month:02d}-01"),
            "ano": int(ano.iloc[idx]) if col_data and not pd.isna(ano.iloc[idx]) else year,
            "mes": int(mes.iloc[idx]) if col_data and not pd.isna(mes.iloc[idx]) else month,
            "conta": (str(r[col_conta]).strip() if col_conta and not pd.isna(r[col_conta]) else None),
            "subconta": (str(r[col_sub]).strip() if col_sub and not pd.isna(r[col_sub]) else None),
            "historico": (str(r[col_hist]).strip() if col_hist and not pd.isna(r[col_hist]) else None),
            "valor": float(val.iloc[idx]) if (isinstance(val, pd.Series) and not pd.isna(val.iloc[idx])) else None,
            "source_file": getattr(file_obj, "name", None)
        }
        row["row_hash"] = hashlib.sha256(json.dumps(row, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
        rows.append(row)

    if rows:
        insert_detalhamento_rows(imp_id, rows)
    record_ingest(selected_unit_id, year, month, "detalhamento", getattr(file_obj, "name", None), fh, len(rows))
    st.success(f"Detalhamento processado. Linhas novas inseridas: {len(rows)}.")
    return imp_id


# Loading splash
with st.spinner("Carregando..."):
    pass

# Import buttons
col_imp1, col_imp2, col_exp = st.columns([1,1,1])
if col_imp1.button("Importar Notas de Negócio", width='stretch', type="primary"):
    if uploaded_nns:
        imp_id = import_notas(uploaded_nns)
        st.success(f"Notas importadas! Import ID: {imp_id}")
        st.rerun()
    else:
        st.warning("Selecione CSVs de Notas de Negócio na barra lateral.")

if col_imp2.button("Importar Detalhamento Financeiro", width='stretch'):
    if uploaded_det:
        imp_id = import_detalhamento(uploaded_det)
        st.success(f"Detalhamento importado! Import ID: {imp_id}")
        st.rerun()
    else:
        st.warning("Selecione o CSV de Detalhamento Financeiro na barra lateral.")

# ===============================
# KPIs e Waterfall
# ===============================
st.markdown("### KPIs do Período Selecionado")
bruta, deduc, liquida, despesas = query_kpis(selected_unit_id, year, month)
k1, k2, k3, k4 = st.columns(4)
k1.metric("Receita Bruta", f"R$ {bruta:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
k2.metric("Deduções", f"R$ {deduc:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
k3.metric("Receita Líquida", f"R$ {liquida:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
k4.metric("Despesas Totais", f"R$ {despesas:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))

st.markdown("#### Waterfall do Resultado")
wf_categories = ["Bruta", "Deduções", "Despesas", "Resultado"]
wf_values = [bruta, deduc, -despesas, bruta + deduc - despesas]
wfig = go.Figure(go.Waterfall(
    name="Resultado",
    orientation="v",
    measure=["relative", "relative", "relative", "total"],
    x=wf_categories,
    text=[f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X",".") for v in wf_values],
    y=wf_values
))
wfig.update_layout(height=360, showlegend=False, margin=dict(l=10,r=10,t=30,b=10),
                   paper_bgcolor="white", plot_bgcolor="white")
st.plotly_chart(wfig, width='stretch')

with st.expander("Comparação com outro período", expanded=False):
    colc1, colc2 = st.columns(2)
    year_cmp = colc1.selectbox("Ano (comparativo)", list(range(2023, 2027)), index=1)
    month_cmp = colc2.selectbox("Mês (comparativo)", list(range(1,13)), index=0, format_func=lambda m: f"{m:02d}")
    bruta_c, deduc_c, liquida_c, despesas_c = query_kpis(selected_unit_id, year_cmp, month_cmp)
    cc1, cc2, cc3, cc4 = st.columns(4)
    cc1.metric("Bruta (cmp)", f"R$ {bruta_c:,.2f}".replace(",", "X").replace(".", ",").replace("X","."),
               delta=f"{(bruta - bruta_c):,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
    cc2.metric("Deduções (cmp)", f"R$ {deduc_c:,.2f}".replace(",", "X").replace(".", ",").replace("X","."),
               delta=f"{(deduc - deduc_c):,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
    cc3.metric("Líquida (cmp)", f"R$ {liquida_c:,.2f}".replace(",", "X").replace(".", ",").replace("X","."),
               delta=f"{(liquida - liquida_c):,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
    cc4.metric("Despesas (cmp)", f"R$ {despesas_c:,.2f}".replace(",", "X").replace(".", ",").replace("X","."),
               delta=f"{(despesas - despesas_c):,.2f}".replace(",", "X").replace(".", ",").replace("X","."))

# ===============================
# Receitas por Arrecadadora + Gráfico mensal
# ===============================
st.markdown("---")
st.markdown("### Receitas por Arrecadadora")

df_n = get_nota_period(selected_unit_id, year, month)
if df_n.empty:
    st.info("Sem Notas de Negócio importadas para este período.")
else:
    col_arrec = "arrecadadora"
    col_codigo = "codigo"
    col_meio = "meio_pagamento"
    col_valor = "valor"
    col_data = "data"

    arrec_list = sorted(df_n[col_arrec].dropna().unique().tolist())
    sel_arr = st.selectbox("Selecionar arrecadadora para gráfico mensal", arrec_list, index=0 if arrec_list else None)

    if sel_arr:
        series = []
        with get_conn() as conn:
            for i in range(11, -1, -1):
                dt = datetime(year, month, 1) - relativedelta(months=i)
                y, m = dt.year, dt.month
                q = """
                SELECT cm.kind, SUM(COALESCE(n.valor,0)) as valor
                FROM notas_raw n
                LEFT JOIN code_map cm ON UPPER(n.codigo)=cm.code
                INNER JOIN imports i ON i.id = n.import_id
                WHERE i.unit_id = ? AND i.period_year = ? AND i.period_month = ? AND n.arrecadadora = ?
                GROUP BY cm.kind
                """
                d = pd.read_sql_query(q, conn, params=(selected_unit_id, y, m, sel_arr))
                br = d.loc[d["kind"]=="BRUTA", "valor"].sum() if not d.empty else 0.0
                de = d.loc[d["kind"]=="DEDUCAO", "valor"].sum() if not d.empty else 0.0
                liq = br + de
                series.append({"ano": y, "mes": m, "liquida": liq})
        s = pd.DataFrame(series)
        s["compet"] = s["ano"].astype(str) + "-" + s["mes"].apply(lambda x: f"{x:02d}")
        fig_line = go.Figure()
        fig_line.add_trace(go.Scatter(x=s["compet"], y=s["liquida"], mode="lines+markers", name="Receita Líquida"))
        fig_line.update_layout(height=360, margin=dict(l=10,r=10,t=30,b=10), paper_bgcolor="white", plot_bgcolor="white")
        st.plotly_chart(fig_line, width='stretch')

    for arr, dfa in df_n.groupby(col_arrec):
        total_arr = dfa[col_valor].sum()
        with st.expander(f"{arr} — Total R$ {total_arr:,.2f}".replace(",", "X").replace(".", ",").replace("X","."), expanded=False):
            by_code = dfa.groupby(col_codigo)[col_valor].sum().sort_values(ascending=False)
            st.write("**Por código**")
            st.dataframe(by_code.reset_index().rename(columns={col_codigo: "codigo", col_valor: "valor"}))

            if col_meio in dfa.columns:
                st.write("**Por meio de pagamento**")
                by_meio = dfa.groupby(col_meio)[col_valor].sum().sort_values(ascending=False)
                st.dataframe(by_meio.reset_index().rename(columns={col_meio: "meio_pagamento", col_valor: "valor"}))

            with st.expander("Comparar meses nesta arrecadadora"):
                colc1, colc2 = st.columns(2)
                y2 = colc1.selectbox("Ano (cmp)", list(range(2023, 2027)), index=1, key=f"arr_{arr}_y")
                m2 = colc2.selectbox("Mês (cmp)", list(range(1,13)), index=0, format_func=lambda x: f"{x:02d}", key=f"arr_{arr}_m")
                with get_conn() as conn:
                    q1 = """
                    SELECT cm.kind, SUM(COALESCE(n.valor,0)) as valor
                    FROM notas_raw n
                    LEFT JOIN code_map cm ON UPPER(n.codigo)=cm.code
                    INNER JOIN imports i ON i.id = n.import_id
                    WHERE i.unit_id = ? AND i.period_year = ? AND i.period_month = ? AND n.arrecadadora = ?
                    GROUP BY cm.kind
                    """
                    d_this = pd.read_sql_query(q1, conn, params=(selected_unit_id, year, month, arr))
                    d_cmp = pd.read_sql_query(q1, conn, params=(selected_unit_id, y2, m2, arr))
                br0 = d_this.loc[d_this["kind"]=="BRUTA","valor"].sum() if not d_this.empty else 0.0
                de0 = d_this.loc[d_this["kind"]=="DEDUCAO","valor"].sum() if not d_this.empty else 0.0
                br1 = d_cmp.loc[d_cmp["kind"]=="BRUTA","valor"].sum() if not d_cmp.empty else 0.0
                de1 = d_cmp.loc[d_cmp["kind"]=="DEDUCAO","valor"].sum() if not d_cmp.empty else 0.0
                st.metric("Bruta (cmp)", f"R$ {br1:,.2f}".replace(",", "X").replace(".", ",").replace("X","."),
                          delta=f"{(br0 - br1):,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
                st.metric("Deduções (cmp)", f"R$ {de1:,.2f}".replace(",", "X").replace(".", ",").replace("X","."),
                          delta=f"{(de0 - de1):,.2f}".replace(",", "X").replace(".", ",").replace("X","."))

            st.download_button(
                "Baixar detalhamento da arrecadadora",
                data=dfa.to_csv(index=False, encoding="utf-8-sig"),
                file_name=f"notas_{arr}_{year}_{month:02d}.csv",
                mime="text/csv"
            )

# ===============================
# Despesas + Editor de Histórico (Persistência)
# ===============================
st.markdown("---")
st.markdown("### Despesas — Detalhamento Financeiro")

df_d = get_detalhamento_period(selected_unit_id, year, month)
if df_d.empty:
    st.info("Sem Detalhamento Financeiro importado para este período.")
else:
    df_d = apply_hist_rules(df_d, unit_id=selected_unit_id)
    by_conta = df_d.groupby("conta", dropna=False)["valor"].sum().sort_values(ascending=False).to_frame("valor_total")
    st.dataframe(by_conta.reset_index())

    st.write("**Editor do histórico (cria regras persistentes por unidade)**")
    show_cols = ["conta", "subconta", "historico", "valor", "data", "source_file"]
    edit_df = df_d[show_cols].copy()
    edited = st.data_editor(edit_df, num_rows="dynamic", width='stretch')

    if st.button("Salvar alterações do histórico como regras persistentes", type="primary"):
        diffs = 0
        for idx in range(len(edit_df)):
            old = str(edit_df.iloc[idx]["historico"])
            new = str(edited.iloc[idx]["historico"])
            if old != new:
                add_hist_rule(selected_unit_id, match_field="historico", match_type="equals", match_value=old, new_historico=new)
                diffs += 1
        if diffs:
            st.success(f"Regras salvas: {diffs}. Elas serão aplicadas automaticamente nos próximos uploads.")
            st.rerun()
        else:
            st.info("Nenhuma alteração detectada para salvar.")

# ===============================
# Exportar DRE para Excel (layout)
# ===============================
def export_dre_excel(unit_id, year, month):
    # Monta três abas: 1) Resumo DRE, 2) Receitas por Arrecadadora x Código, 3) Despesas por Conta
    # 1) Resumo
    bruta, deduc, liquida, despesas = query_kpis(unit_id, year, month)
    resumo = pd.DataFrame({
        "ITEM": ["RECEITA BRUTA", "DEDUÇÕES", "RECEITA LÍQUIDA", "DESPESAS", "RESULTADO"],
        "VALOR": [bruta, deduc, liquida, despesas, liquida - despesas]
    })
    # 2) Receitas detalhadas
    with get_conn() as conn:
        q = """
        SELECT n.arrecadadora, UPPER(COALESCE(n.codigo,'')) AS codigo, SUM(COALESCE(n.valor,0)) AS valor
        FROM notas_raw n
        INNER JOIN imports i ON i.id = n.import_id
        WHERE i.unit_id = ? AND i.period_year = ? AND i.period_month = ?
        GROUP BY n.arrecadadora, UPPER(COALESCE(n.codigo,''))
        ORDER BY n.arrecadadora, codigo
        """
        receitas = pd.read_sql_query(q, conn, params=(unit_id, year, month))

    # 3) Despesas por conta
    despesas_conta = pd.DataFrame()
    dtmp = get_detalhamento_period(unit_id, year, month)
    if not dtmp.empty:
        dtmp = apply_hist_rules(dtmp, unit_id=unit_id)
        despesas_conta = dtmp.groupby(["conta","subconta"], dropna=False)["valor"].sum().reset_index().sort_values("valor", ascending=False)

    # Escrever em memória
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        resumo.to_excel(writer, sheet_name="RESUMO_DRE", index=False)
        receitas.to_excel(writer, sheet_name="RECEITAS", index=False)
        if not despesas_conta.empty:
            despesas_conta.to_excel(writer, sheet_name="DESPESAS", index=False)
    bio.seek(0)
    return bio

with col_exp:
    if st.button("Exportar DRE para Excel", width='stretch'):
        xls = export_dre_excel(selected_unit_id, year, month)
        st.download_button(
            label="Baixar DRE_{0}_{1:02d}.xlsx".format(year, month),
            data=xls,
            file_name=f"DRE_{year}_{month:02d}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        
