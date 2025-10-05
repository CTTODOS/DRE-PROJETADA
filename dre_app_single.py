import customtkinter as ctk
import os
import datetime
import pandas as pd
from tkinter import messagebox, filedialog, ttk
import re
import sqlite3
import shutil
import json
from typing import List, Dict, Any, Optional, Tuple, cast
import matplotlib
matplotlib.use('TkAgg')
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_RIGHT, TA_CENTER
import io

# ==============================================================================
# --- 1. CLASSE DE CONFIGURAÇÃO (COM MAPEAMENTO CORRIGIDO) ---
# ==============================================================================
class Config:
    """Classe para armazenar todas as configurações e constantes do aplicativo."""
    APP_VERSION = "9.4.0" # Lógica de sinais aprimorada
    CTK_APPEARANCE_MODE = "light" # Tema claro como padrão

    # Paleta de Cores - Cartão de TODOS
    COLOR_PRIMARY_GREEN = "#00A651"
    COLOR_SECONDARY_YELLOW = "#FFD700"
    COLOR_BACKGROUND = "#F7F7F7"
    COLOR_FRAME = "#FFFFFF"
    COLOR_TEXT = "#333333"
    COLOR_TEXT_LIGHT = "#666666"
    COLOR_BUTTON_TEXT_DARK = "#000000"
    COLOR_BUTTON_TEXT_LIGHT = "#FFFFFF"
    COLOR_RED = "#E53935"
    COLOR_BLUE = "#0288D1"
    
    APP_NAME = "Calculadora DRE de TODOS"
    CREATOR_NAME = "Powered by: Lucas Costa"
    CURRENT_YEAR = datetime.datetime.now().year
    CURRENT_YEAR_SHORT = datetime.datetime.now().strftime('%y')
    DB_FOLDER = "database"
    DB_NAME = "dre_database.db"
    DB_PATH = os.path.join(DB_FOLDER, DB_NAME)
    BACKUP_LOG_FILE = os.path.join(DB_FOLDER, "backup_log.json")
    # File to persist user edits to mappings (chart of accounts, description mapping, notas negocio)
    MAPPINGS_FILE = os.path.join(DB_FOLDER, "mappings.json")

    # ==============================================================================
    # --- MAPEAMENTO DE CONTAS CORRIGIDO ---
    # As chaves agora correspondem ao formato 'XX.XX.XXX' do arquivo 'detalhamento financeiro.csv'
    # ==============================================================================
    CHART_OF_ACCOUNTS = {
        # Receitas
        "10.05.002": {"group": "Receitas Operacionais", "subgroup": "Receita de Mensalidade"},
        "11.06.001": {"group": "Receitas Operacionais", "subgroup": "Receita de Carteirinhas"},
        "11.06.002": {"group": "Receitas Operacionais", "subgroup": "Receita de Refiliação"},
        "11.05.002": {"group": "Receitas Operacionais", "subgroup": "Receita de Taxa de Adesão"},

        # Despesas com Pessoal
        "01.01.000": {"group": "Despesas com Pessoal", "subgroup": "Folha de Pagamento"}, # Salários
        "01.03.000": {"group": "Despesas com Pessoal", "subgroup": "Folha de Pagamento"}, # Comissões
        "01.05.001": {"group": "Despesas com Pessoal", "subgroup": "Folha de Pagamento"}, # FGTS
        "01.06.001": {"group": "Despesas com Pessoal", "subgroup": "Folha de Pagamento"}, # Rescisões
        "01.07.000": {"group": "Despesas com Pessoal", "subgroup": "Folha de Pagamento"}, # Férias (código do manual)
        "01.09.002": {"group": "Despesas com Pessoal", "subgroup": "Benefícios"}, # Vale Transporte
        "01.09.003": {"group": "Despesas com Pessoal", "subgroup": "Benefícios"}, # Refeições
        "01.09.004": {"group": "Despesas com Pessoal", "subgroup": "Benefícios"}, # Exames Admissionais
        "01.10.000": {"group": "Despesas com Pessoal", "subgroup": "Folha de Pagamento"}, # Estagiários e Diaristas
        "01.11.004": {"group": "Despesas com Pessoal", "subgroup": "Folha de Pagamento"}, # Pagamento a Terceiros (Vendas)

        # Despesas Administrativas
        "02.02.002": {"group": "Despesas Administrativas", "subgroup": "Serviços de Terceiros"}, # Suporte TI
        "02.02.003": {"group": "Despesas Administrativas", "subgroup": "Serviços de Terceiros"}, # Suporte Qualidade
        "02.04.001": {"group": "Despesas Administrativas", "subgroup": "Serviços de Terceiros"}, # Contabilidade
        "05.01.001": {"group": "Despesas Administrativas", "subgroup": "Infraestrutura"}, # Água
        "05.01.002": {"group": "Despesas Administrativas", "subgroup": "Infraestrutura"}, # Energia Elétrica
        "05.01.004": {"group": "Despesas Administrativas", "subgroup": "Infraestrutura"}, # Telefone Celular
        "05.01.007": {"group": "Despesas Administrativas", "subgroup": "Infraestrutura"}, # Internet
        "05.03.001": {"group": "Despesas Administrativas", "subgroup": "Materiais e Suprimentos"}, # Materiais de Escritório
        "05.03.002": {"group": "Despesas Administrativas", "subgroup": "Materiais e Suprimentos"}, # Materiais de Limpeza
        "05.03.003": {"group": "Despesas Administrativas", "subgroup": "Materiais e Suprimentos"}, # Materiais Descartáveis
        "05.04.001": {"group": "Despesas Administrativas", "subgroup": "Eventos e Confraternizações"}, # Brindes e Donativos
        "05.04.004": {"group": "Despesas Administrativas", "subgroup": "Manutenção"}, # Manutenção de Equipamentos
        "05.04.007": {"group": "Despesas Administrativas", "subgroup": "Viagens e Representação"}, # Viagens e Diárias
        "05.04.016": {"group": "Despesas Administrativas", "subgroup": "Outras Desp. Admin."}, # Devolução de Mensalidades
        "05.04.021": {"group": "Despesas Administrativas", "subgroup": "Eventos e Confraternizações"}, # Lanches
        "05.04.022": {"group": "Despesas Administrativas", "subgroup": "Outras Desp. Admin."}, # Débito Extra Líquido (DEL)
        "05.04.035": {"group": "Despesas Administrativas", "subgroup": "Viagens e Representação"}, # Estacionamento/Pedágio

        # Despesas Comerciais
        "05.02.001": {"group": "Despesas Comerciais", "subgroup": "Aluguéis e Condomínio"}, # Aluguel
        "05.04.010": {"group": "Despesas Comerciais", "subgroup": "Marketing e Publicidade"}, # Propagandas e Anúncios

        # Impostos e Taxas
        "06.01.001": {"group": "Impostos e Taxas", "subgroup": "Impostos"}, # ISSQN
        "01.09.006": {"group": "Impostos e Taxas", "subgroup": "Impostos"}, # IRRF Pessoa Física
        "06.03.001": {"group": "Impostos e Taxas", "subgroup": "Impostos"}, # IRPJ
        "06.03.004": {"group": "Impostos e Taxas", "subgroup": "Impostos"}, # CSLL
        "02.03.002": {"group": "Impostos e Taxas", "subgroup": "Taxas"}, # Assessoria Jurídica Tributária (AJT)
        
        # Despesas Financeiras
        "07.01.001": {"group": "Despesas Financeiras", "subgroup": "Taxas Bancárias"}, # IOF/Taxas
        "07.01.003": {"group": "Despesas Financeiras", "subgroup": "Juros e Empréstimos"}, # Multa e Juros
        "07.02.000": {"group": "Despesas Financeiras", "subgroup": "Juros e Empréstimos"}, # Dívidas com Terceiros
        "07.02.002": {"group": "Despesas Financeiras", "subgroup": "Juros e Empréstimos"}, # Empréstimos com Terceiros

        # Investimentos
        "08.01.001": {"group": "Investimentos", "subgroup": "Equipamentos e Software"}, # Compra de Equipamentos
        "08.02.001": {"group": "Investimentos", "subgroup": "Obras e Reformas"}, # Reforma e Manutenção de Imóveis
    }
    
    # Mapeamento por palavras-chave (fallback) - Aprimorado
    DESCRIPTION_MAPPING = {
        # --- REGRAS DE RECEITA ---
        "RECEBIMENTO": {"group": "Outras Receitas", "subgroup": "Recebimentos Diversos"},
        
        # --- REGRAS DE DESPESA ---
        "REPASSE": {"group": "Despesas Administrativas", "subgroup": "Repasses a Terceiros"},
        "TREINAMENTO": {"group": "Despesas com Pessoal", "subgroup": "Treinamento e Desenvolvimento"},
        "RETENCOES": {"group": "Impostos e Taxas", "subgroup": "Impostos"},
        "DAS": {"group": "Impostos e Taxas", "subgroup": "Impostos"},
        "SALARIO": {"group": "Despesas com Pessoal", "subgroup": "Folha de Pagamento"},
        "FERIAS": {"group": "Despesas com Pessoal", "subgroup": "Folha de Pagamento"},
        "RESCISAO": {"group": "Despesas com Pessoal", "subgroup": "Folha de Pagamento"},
        "FGTS": {"group": "Despesas com Pessoal", "subgroup": "Folha de Pagamento"},
        "INSS": {"group": "Despesas com Pessoal", "subgroup": "Folha de Pagamento"},
        "VALE TRANSPORTE": {"group": "Despesas com Pessoal", "subgroup": "Benefícios"},
        "VALE ALIMENTACAO": {"group": "Despesas com Pessoal", "subgroup": "Benefícios"},
        "VALE REFEICAO": {"group": "Despesas com Pessoal", "subgroup": "Benefícios"},
        "REFEICOES": {"group": "Despesas com Pessoal", "subgroup": "Benefícios"},
        "COMISSAO": {"group": "Despesas com Pessoal", "subgroup": "Folha de Pagamento"},
        "ENERGIA": {"group": "Despesas Administrativas", "subgroup": "Infraestrutura"},
        "AGUA": {"group": "Despesas Administrativas", "subgroup": "Infraestrutura"},
        "TELEFONE": {"group": "Despesas Administrativas", "subgroup": "Infraestrutura"},
        "INTERNET": {"group": "Despesas Administrativas", "subgroup": "Infraestrutura"},
        "CONTABILIDADE": {"group": "Despesas Administrativas", "subgroup": "Serviços de Terceiros"},
        "ALUGUEL": {"group": "Despesas Comerciais", "subgroup": "Aluguéis e Condomínio"},
        "CONDOMINIO": {"group": "Despesas Comerciais", "subgroup": "Aluguéis e Condomínio"},
        "MARKETING": {"group": "Despesas Comerciais", "subgroup": "Marketing e Publicidade"},
        "PUBLICIDADE": {"group": "Despesas Comerciais", "subgroup": "Marketing e Publicidade"},
        "PROPAGANDA": {"group": "Despesas Comerciais", "subgroup": "Marketing e Publicidade"},
        "ANUNCIO": {"group": "Despesas Comerciais", "subgroup": "Marketing e Publicidade"},
        "FACEBOOK": {"group": "Despesas Comerciais", "subgroup": "Marketing e Publicidade"},
        "ISSQN": {"group": "Impostos e Taxas", "subgroup": "Impostos"},
        "ISS": {"group": "Impostos e Taxas", "subgroup": "Impostos"},
        "IPTU": {"group": "Impostos e Taxas", "subgroup": "Impostos"},
        "ALVARA": {"group": "Impostos e Taxas", "subgroup": "Taxas"},
        "EMPRESTIMO": {"group": "Despesas Financeiras", "subgroup": "Juros e Empréstimos"},
        "JUROS": {"group": "Despesas Financeiras", "subgroup": "Juros e Empréstimos"},
        "MULTA": {"group": "Despesas Financeiras", "subgroup": "Juros e Empréstimos"},
        "DIVIDENDOS": {"group": "Dividendos e Pro-labore", "subgroup": "Dividendos"},
        "PRO-LABORE": {"group": "Dividendos e Pro-labore", "subgroup": "Pró-labore"},
        "REFORMA": {"group": "Investimentos", "subgroup": "Obras e Reformas"},
        "PINTOR": {"group": "Investimentos", "subgroup": "Obras e Reformas"},
        "SUPORTE": {"group": "Despesas Administrativas", "subgroup": "Serviços de Terceiros"},
        "ESTAGIARIO": {"group": "Despesas com Pessoal", "subgroup": "Folha de Pagamento"},
        "DIARISTA": {"group": "Despesas com Pessoal", "subgroup": "Folha de Pagamento"},
        "BRINDES": {"group": "Despesas Administrativas", "subgroup": "Eventos e Confraternizações"},
        "LANCHES": {"group": "Despesas Administrativas", "subgroup": "Eventos e Confraternizações"},
        "MANUTENCAO": {"group": "Despesas Administrativas", "subgroup": "Manutenção"},
        "EQUIPAMENTO": {"group": "Investimentos", "subgroup": "Equipamentos e Software"},
    }

    # Mapeamento para indicadores específicos do arquivo "Nota de Negócio"
    NOTAS_NEGOCIO_MAPPING = {
        # Receitas
        "VAM": {"group": "Receitas da Franqueadora", "subgroup": "Arrecadação Mensal (VAM)"},
        "TAC": {"group": "Receitas da Franqueadora", "subgroup": "Taxa de Adesão (TAC)"},
        "CAR": {"group": "Receitas da Franqueadora", "subgroup": "Créditos Diversos"},
        "COM": {"group": "Receitas da Franqueadora", "subgroup": "Comissões"},
        # Despesas
        "VRA": {"group": "Despesas da Franqueadora", "subgroup": "Taxa de Arrecadação (VRA)"},
        "DRF": {"group": "Despesas da Franqueadora", "subgroup": "Royalties (DRF)"},
        "AIR": {"group": "Despesas da Franqueadora", "subgroup": "Taxa de Intermediação (AIR)"},
        "DAR": {"group": "Despesas da Franqueadora", "subgroup": "Débitos Diversos"},
        "DEL": {"group": "Despesas da Franqueadora", "subgroup": "Débito Extra Líquido (DEL)"},
        "AJT": {"group": "Impostos e Taxas", "subgroup": "Assessoria Jurídica Tributária (AJT)"},
        "SAF": {"group": "Despesas da Franqueadora", "subgroup": "Seguro Auxílio Funerário (SAF)"},
        "DAS": {"group": "Impostos e Taxas", "subgroup": "Retenções (DAS)"},
        "FPP": {"group": "Despesas Comerciais", "subgroup": "Fundo de Publicidade (FPP)"},
        # Neutro / Depende do sinal
        "VIR": {"group": "Ajustes da Franqueadora", "subgroup": "Itens Refaturados (VIR)"},
    }

    DRE_GROUP_ORDER = [
        "Receitas Operacionais",
        "Receitas da Franqueadora",
        "Outras Receitas",
        "Despesas com Pessoal",
        "Despesas Administrativas",
        "Despesas Comerciais",
        "Despesas da Franqueadora",
        "Impostos e Taxas",
        "Despesas Financeiras",
        "Dividendos e Pro-labore",
        "Investimentos",
        "Ajustes da Franqueadora",
        "Outros"
    ]
    @staticmethod
    def load_mappings() -> None:
        """Load user-edited mappings from Config.MAPPINGS_FILE if present.
        This will update the in-memory mapping dictionaries on Config.
        """
        try:
            if os.path.exists(Config.MAPPINGS_FILE):
                with open(Config.MAPPINGS_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                # Merge loaded mappings, but keep defaults for missing entries
                if isinstance(data.get('CHART_OF_ACCOUNTS'), dict):
                    Config.CHART_OF_ACCOUNTS.update(data['CHART_OF_ACCOUNTS'])
                if isinstance(data.get('DESCRIPTION_MAPPING'), dict):
                    Config.DESCRIPTION_MAPPING.update(data['DESCRIPTION_MAPPING'])
                if isinstance(data.get('NOTAS_NEGOCIO_MAPPING'), dict):
                    Config.NOTAS_NEGOCIO_MAPPING.update(data['NOTAS_NEGOCIO_MAPPING'])
        except Exception:
            # Fail silently; fall back to built-in mappings
            pass

    @staticmethod
    def save_mappings() -> bool:
        """Persist the current mapping dicts to Config.MAPPINGS_FILE.
        Returns True on success.
        """
        try:
            os.makedirs(os.path.dirname(Config.MAPPINGS_FILE), exist_ok=True)
            payload = {
                'CHART_OF_ACCOUNTS': Config.CHART_OF_ACCOUNTS,
                'DESCRIPTION_MAPPING': Config.DESCRIPTION_MAPPING,
                'NOTAS_NEGOCIO_MAPPING': Config.NOTAS_NEGOCIO_MAPPING
            }
            with open(Config.MAPPINGS_FILE, 'w', encoding='utf-8') as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            return True
        except Exception:
            return False

# ==============================================================================
# --- 2. GERENCIADOR DE BANCO DE DADOS (Sem alterações) ---
# ==============================================================================
class DatabaseManager:
    """Gerencia todas as interações com o banco de dados SQLite."""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._setup_database()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.execute('PRAGMA foreign_keys = ON;')
        return conn

    def _setup_database(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS analysis_summary (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, unit_name TEXT NOT NULL, period TEXT NOT NULL,
                    generation_date TEXT NOT NULL, collector TEXT, source_file TEXT NOT NULL,
                    total_revenue REAL NOT NULL, total_expense REAL NOT NULL, net_result REAL NOT NULL,
                    UNIQUE(unit_name, period, source_file)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS analysis_details (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, summary_id INTEGER NOT NULL,
                    group_name TEXT NOT NULL, subgroup_name TEXT NOT NULL,
                    indicator TEXT NOT NULL, value REAL NOT NULL,
                    FOREIGN KEY (summary_id) REFERENCES analysis_summary (id) ON DELETE CASCADE
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS unit_goals (
                    unit_name TEXT PRIMARY KEY, monthly_goal REAL
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS action_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    details TEXT
                )
            ''')
            conn.commit()

    def log_action(self, action_type: str, details: str = ""):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._get_connection() as conn:
            conn.execute(
                "INSERT INTO action_logs (timestamp, action_type, details) VALUES (?, ?, ?)",
                (timestamp, action_type, details)
            )
            conn.commit()

    def save_imported_data(self, unit_name: str, month: int, source_file: str, all_details: List[Dict[str, Any]], collector: Optional[str] = 'N/A') -> bool:
        total_revenue = sum(item['value'] for item in all_details if item['value'] > 0)
        total_expense = sum(item['value'] for item in all_details if item['value'] < 0)
        net_result = total_revenue + total_expense

        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Remove existing data for the same consolidated file to avoid duplicates
                cursor.execute(
                    "DELETE FROM analysis_details WHERE summary_id IN (SELECT id FROM analysis_summary WHERE unit_name = ? AND period = ? AND source_file = ?)",
                    (unit_name, f"{month:02d}/{Config.CURRENT_YEAR}", source_file)
                )
                cursor.execute(
                    "DELETE FROM analysis_summary WHERE unit_name = ? AND period = ? AND source_file = ?",
                    (unit_name, f"{month:02d}/{Config.CURRENT_YEAR}", source_file)
                )

                # Insert new summary
                cursor.execute(
                    'INSERT INTO analysis_summary (unit_name, period, source_file, generation_date, collector, total_revenue, total_expense, net_result) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                    (unit_name, f"{month:02d}/{Config.CURRENT_YEAR}", source_file, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), collector, total_revenue, total_expense, net_result)
                )
                
                summary_id = cursor.lastrowid

                # Insert new details
                detail_values = [
                    (summary_id, item['group'], item['subgroup'], item['indicator'], item['value'])
                    for item in all_details
                ]
                cursor.executemany('INSERT INTO analysis_details (summary_id, group_name, subgroup_name, indicator, value) VALUES (?, ?, ?, ?, ?)', detail_values)
                
                conn.commit()
                self.log_action("IMPORT_SUCCESS", f"Dados para '{source_file}' importados para '{unit_name}'.")
                return True
            except Exception as e:
                conn.rollback()
                self.log_action("IMPORT_ERROR", f"Erro ao importar '{source_file}' para '{unit_name}': {e}")
                messagebox.showerror("Erro no Banco de Dados", f"Erro ao salvar dados do arquivo {source_file}.\n{e}")
                return False
    
    def get_detailed_results(self, unit_name: str, start_month: int, end_month: int) -> pd.DataFrame:
        with self._get_connection() as conn:
            periods = [f"{month:02d}/{Config.CURRENT_YEAR}" for month in range(start_month, end_month + 1)]
            placeholders = ','.join('?' for _ in periods)
            query = f"""
                SELECT d.group_name, d.subgroup_name, d.indicator, SUM(d.value) as total_value
                FROM analysis_details d
                JOIN analysis_summary s ON d.summary_id = s.id
                WHERE s.unit_name = ? AND s.period IN ({placeholders})
                GROUP BY d.group_name, d.subgroup_name, d.indicator
                ORDER BY d.group_name, d.subgroup_name, d.indicator
            """
            # pandas typing expects a sequence/tuple; use tuple to avoid type complaints
            params = tuple([unit_name] + periods)
            return pd.read_sql_query(query, conn, params=params)

    def get_global_kpis_for_current_month(self) -> Dict[str, Any]:
        current_month_period = f"{datetime.datetime.now().month:02d}/{Config.CURRENT_YEAR}"
        with self._get_connection() as conn:
            query = "SELECT SUM(net_result) as total_net, SUM(total_revenue) as total_revenue FROM analysis_summary WHERE period = ?"
            df = pd.read_sql_query(query, conn, params=(current_month_period,))
            
            top_units_query = """
                SELECT unit_name, SUM(net_result) as monthly_net
                FROM analysis_summary
                WHERE period = ?
                GROUP BY unit_name
                ORDER BY monthly_net DESC
                LIMIT 3
            """
            top_units_df = pd.read_sql_query(top_units_query, conn, params=(current_month_period,))

            return {
                "total_net": df['total_net'].iloc[0] or 0.0,
                "total_revenue": df['total_revenue'].iloc[0] or 0.0,
                "top_units": top_units_df.to_dict('records')
            }

    def get_comparison_data(self, unit_names: List[str], start_month: int, end_month: int) -> pd.DataFrame:
        with self._get_connection() as conn:
            periods = [f"{month:02d}/{Config.CURRENT_YEAR}" for month in range(start_month, end_month + 1)]
            placeholders_units = ','.join('?' for _ in unit_names)
            placeholders_periods = ','.join('?' for _ in periods)
            
            query = f"""
                SELECT unit_name, SUM(total_revenue) as total_revenue, SUM(net_result) as net_result
                FROM analysis_summary
                WHERE unit_name IN ({placeholders_units}) AND period IN ({placeholders_periods})
                GROUP BY unit_name
            """
            params = tuple(unit_names + periods)
            return pd.read_sql_query(query, conn, params=params)

    def get_annual_dashboard_data(self, unit_name: str) -> pd.DataFrame:
        with self._get_connection() as conn:
            query = "SELECT period, SUM(net_result) as total_net FROM analysis_summary WHERE unit_name = ? AND period LIKE ? GROUP BY period ORDER BY period ASC"
            return pd.read_sql_query(query, conn, params=(unit_name, f'%/{Config.CURRENT_YEAR}'))

    def get_unit_goal(self, unit_name: str) -> float:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT monthly_goal FROM unit_goals WHERE unit_name = ?", (unit_name,))
            result = cursor.fetchone()
            return result[0] if result else 0.0

    def set_unit_goal(self, unit_name: str, goal: float):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO unit_goals (unit_name, monthly_goal) VALUES (?, ?)", (unit_name, goal))
            conn.commit()

    def rename_unit_data(self, old_name: str, new_name: str):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE analysis_summary SET unit_name = ? WHERE unit_name = ?", (new_name, old_name))
            cursor.execute("UPDATE unit_goals SET unit_name = ? WHERE unit_name = ?", (new_name, old_name))
            conn.commit()

    def delete_unit_data(self, unit_name: str):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM analysis_summary WHERE unit_name = ?", (unit_name,))
            cursor.execute("DELETE FROM unit_goals WHERE unit_name = ?", (unit_name,))
            conn.commit()

    def get_imported_files_summary(self, unit_name: str, search_term: Optional[str] = None) -> pd.DataFrame:
        with self._get_connection() as conn:
            query = "SELECT id, period, source_file, collector, net_result FROM analysis_summary WHERE unit_name = ?"
            if search_term:
                query += " AND source_file LIKE ?"
                params = (unit_name, f"%{search_term}%")
            else:
                params = (unit_name,)
            query += " ORDER BY period DESC, source_file ASC"
            return pd.read_sql_query(query, conn, params=params)

    def get_file_details(self, summary_id: int) -> pd.DataFrame:
        with self._get_connection() as conn:
            query = "SELECT group_name, subgroup_name, indicator, value FROM analysis_details WHERE summary_id = ? ORDER BY id ASC"
            return pd.read_sql_query(query, conn, params=(summary_id,))

    def get_distinct_collectors(self) -> List[str]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT collector FROM analysis_summary WHERE collector IS NOT NULL AND collector != 'N/A' ORDER BY collector")
            return [row[0] for row in cursor.fetchall()]

    def update_collector_name(self, old_name: str, new_name: str):
        with self._get_connection() as conn:
            conn.execute("UPDATE analysis_summary SET collector = ? WHERE collector = ?", (new_name, old_name))
            conn.commit()
            self.log_action("UPDATE_COLLECTOR", f"Renomeado '{old_name}' para '{new_name}'.")

    def merge_collectors(self, collectors_to_merge: List[str], final_name: str):
        with self._get_connection() as conn:
            placeholders = ','.join('?' for _ in collectors_to_merge)
            query = f"UPDATE analysis_summary SET collector = ? WHERE collector IN ({placeholders})"
            params = [final_name] + collectors_to_merge
            conn.execute(query, params)
            conn.commit()
            self.log_action("MERGE_COLLECTORS", f"Arrecadadoras {collectors_to_merge} mescladas em '{final_name}'.")

    def get_all_logs(self) -> pd.DataFrame:
        with self._get_connection() as conn:
            query = "SELECT timestamp, action_type, details FROM action_logs ORDER BY timestamp DESC"
            return pd.read_sql_query(query, conn)

# ==============================================================================
# --- 3. GERENCIADOR DE ARQUIVOS (Sem alterações) ---
# ==============================================================================
class FileManager:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def create_unit_folders(self, unit_name: str) -> bool:
        if not unit_name or not unit_name.strip():
            messagebox.showwarning("Aviso", "O nome da unidade não pode ser vazio.")
            return False
        if os.path.exists(unit_name):
            messagebox.showerror("Erro", f"A unidade '{unit_name}' já existe.")
            return False
        try:
            os.makedirs(unit_name)
            self.db_manager.log_action("CREATE_UNIT", f"Unidade '{unit_name}' criada com sucesso.")
            return True
        except Exception as e:
            self.db_manager.log_action("CREATE_UNIT_ERROR", f"Erro ao criar unidade '{unit_name}': {e}")
            messagebox.showerror("Erro de Sistema", f"Ocorreu um erro ao criar as pastas: {e}")
            return False

    def get_existing_units(self) -> List[str]:
        return sorted([d for d in os.listdir() if os.path.isdir(d) and d not in [Config.DB_FOLDER, "__pycache__"]])

    def rename_unit(self, old_name: str, new_name: str) -> bool:
        try:
            os.rename(old_name, new_name)
            self.db_manager.rename_unit_data(old_name, new_name)
            return True
        except Exception as e:
            messagebox.showerror("Erro", f"Não foi possível renomear a unidade.\n{e}")
            if os.path.exists(new_name):
                os.rename(new_name, old_name)
            return False

    def delete_unit(self, unit_name: str) -> bool:
        if messagebox.askyesno("Confirmar Exclusão", f"Tem certeza que deseja excluir '{unit_name}'?\nTODOS os dados e pastas serão apagados PERMANENTEMENTE."):
            try:
                shutil.rmtree(unit_name)
                self.db_manager.delete_unit_data(unit_name)
                self.db_manager.log_action("DELETE_UNIT", f"Unidade '{unit_name}' foi excluída.")
                return True
            except Exception as e:
                self.db_manager.log_action("DELETE_UNIT_ERROR", f"Erro ao excluir unidade '{unit_name}': {e}")
                messagebox.showerror("Erro", f"Não foi possível excluir a unidade.\n{e}")
                return False
        return False

    def backup_database(self):
        if not os.path.exists(Config.DB_PATH):
            messagebox.showerror("Erro", "Banco de dados não encontrado.")
            return
        backup_path = filedialog.asksaveasfilename(
            defaultextension=".db",
            filetypes=[("SQLite Database", "*.db")],
            initialfile=f"backup_dre_{datetime.datetime.now().strftime('%Y%m%d')}.db"
        )
        if backup_path:
            try:
                shutil.copyfile(Config.DB_PATH, backup_path)
                self._update_backup_log()
                self.db_manager.log_action("BACKUP_SUCCESS", f"Backup criado em: {backup_path}")
                messagebox.showinfo("Sucesso", f"Backup salvo em:\n{backup_path}")
            except Exception as e:
                self.db_manager.log_action("BACKUP_ERROR", f"Erro ao criar backup: {e}")
                messagebox.showerror("Erro de Backup", f"Não foi possível criar o backup.\nErro: {e}")

    def restore_database(self):
        if not messagebox.askyesno("Atenção", "Restaurar um backup substituirá TODOS os dados atuais. Esta ação não pode ser desfeita. Deseja continuar?"):
            return
        restore_path = filedialog.askopenfilename(filetypes=[("SQLite Database", "*.db")], title="Selecionar Arquivo de Backup")
        if restore_path:
            try:
                shutil.copyfile(restore_path, Config.DB_PATH)
                messagebox.showinfo("Sucesso", "Backup restaurado com sucesso. Reinicie o aplicativo para ver as mudanças.")
            except Exception as e:
                messagebox.showerror("Erro na Restauração", f"Não foi possível restaurar o backup.\nErro: {e}")

    def _get_last_backup_date(self) -> Optional[datetime.date]:
        try:
            with open(Config.BACKUP_LOG_FILE, 'r') as f:
                log_data = json.load(f)
                return datetime.datetime.strptime(log_data.get("last_backup"), "%Y-%m-%d").date()
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            return None

    def _update_backup_log(self):
        with open(Config.BACKUP_LOG_FILE, 'w') as f:
            json.dump({"last_backup": datetime.date.today().isoformat()}, f)

    def check_and_prompt_for_backup(self):
        last_backup = self._get_last_backup_date()
        if not last_backup or (datetime.date.today() - last_backup).days > 7:
            if messagebox.askyesno("Backup Recomendado", "Já faz mais de 7 dias desde o último backup. Deseja fazer um agora?"):
                self.backup_database()

# ==============================================================================
# --- 4. PROCESSADOR DE DADOS (COM LÓGICA DE CATEGORIZAÇÃO CORRIGIDA) ---
# ==============================================================================
class DataProcessor:
    def _parse_value(self, value: Any) -> float:
        """Converte um valor (string ou número) para float, tratando R$, parênteses e outros formatos."""
        if isinstance(value, (int, float)):
            return float(value)
        if not isinstance(value, str):
            return 0.0
        
        value_str = value.strip().replace("R$", "").strip()
        
        is_negative = '(' in value_str and ')' in value_str
        
        # Remove pontos de milhar e substitui vírgula decimal por ponto
        num_str = value_str.replace('.', '').replace(',', '.')
        # Remove caracteres não numéricos, exceto o sinal negativo inicial
        num_str = re.sub(r'[^\d.-]', '', num_str)
        
        try:
            number = float(num_str)
            # Se o original tinha parênteses, garante que seja negativo
            if is_negative:
                return -abs(number)
            return number
        except (ValueError, TypeError):
            return 0.0

    def extract_from_notas_negocio(self, filepath: str) -> Tuple[Optional[str], Optional[List[Dict]], Optional[str]]:
        """Extrai e classifica dados de arquivos CSV de Nota de Negócio."""
        try:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()
            
            collector_name = "Não Identificada"
            header_row_index = -1
            
            for i, line in enumerate(lines):
                if 'Arrecadadora:' in line:
                    match = re.search(r'Arrecadadora:\s*([^;]+)', line, re.IGNORECASE)
                    if match:
                        collector_name = match.group(1).strip()
                if 'Indicadores' in line and 'Valores' in line:
                    header_row_index = i
            
            if header_row_index == -1:
                return None, None, f"Cabeçalho 'Indicadores'/'Valores' não encontrado em '{os.path.basename(filepath)}'."

            csv_data = io.StringIO("".join(lines[header_row_index:]))
            df = pd.read_csv(csv_data, sep=';', header=0)
            df.dropna(how='all', inplace=True)
            
            df.columns = [str(c).strip() for c in df.columns]
            indicadores_col = next((col for col in df.columns if 'Indicadores' in col), None)
            valores_col = next((col for col in df.columns if 'Valores' in col), None)
            
            if not indicadores_col or not valores_col:
                return None, None, f"Não foi possível localizar as colunas 'Indicadores'/'Valores'. Cabeçalho lido: {list(df.columns)}"

            details = []
            for _, row in df.iterrows():
                indicator = str(row[indicadores_col]).strip()
                if not indicator or pd.isna(row[valores_col]) or indicator.lower() == 'nan':
                    continue
                
                value = self._parse_value(row[valores_col])
                
                if "SLR" in indicator.upper() or value == 0:
                    continue

                indicator_key = indicator.split(' ')[0].strip().upper()
                account_info = Config.NOTAS_NEGOCIO_MAPPING.get(indicator_key)

                if not account_info:
                    account_info = {"group": "Ajustes da Franqueadora", "subgroup": "Não Mapeado"}

                details.append({
                    "group": account_info["group"],
                    "subgroup": account_info.get("subgroup", collector_name),
                    "indicator": indicator,
                    "value": value
                })
            return collector_name, details, None
        except Exception as e:
            return None, None, f"Erro inesperado ao processar '{os.path.basename(filepath)}': {e}"

    def extract_from_detalhamento(self, filepath: str) -> Tuple[Optional[List[Dict]], Optional[str]]:
        """Extrai dados de arquivos CSV de Detalhamento Financeiro com a nova lógica de mapeamento."""
        try:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()

            header_row_index = -1
            for i, line in enumerate(lines):
                if all(keyword in line for keyword in ['SubConta', 'Descrição', 'Valor']):
                    header_row_index = i
                    break
            
            if header_row_index == -1:
                return None, f"Linha de cabeçalho com 'SubConta', 'Descrição', 'Valor' não encontrada em '{os.path.basename(filepath)}'."

            csv_data = io.StringIO("".join(lines[header_row_index:]))
            df = pd.read_csv(csv_data, sep=';', header=0)
            
            df.dropna(how='all', axis=1, inplace=True)
            df.dropna(subset=[df.columns[0]], inplace=True)

            subconta_col, desc_col, valor_col = None, None, None
            for col in df.columns:
                col_str = str(col).strip()
                if 'SubConta' in col_str: subconta_col = col
                if 'Descrição' in col_str: desc_col = col
                if 'Valor' in col_str: valor_col = col
            
            if not all([subconta_col, desc_col, valor_col]):
                return None, f"Colunas esperadas não encontradas. Colunas lidas: {list(df.columns)}"

            df = df[[subconta_col, desc_col, valor_col]].copy()
            df.columns = ['SubConta', 'Descrição', 'Valor']
            df.dropna(subset=['SubConta', 'Valor'], inplace=True)

            details = []
            for _, row in df.iterrows():
                sub_conta = str(row['SubConta']).strip()
                descricao = str(row['Descrição']).strip().upper()
                valor = self._parse_value(str(row['Valor']))
                
                if valor == 0:
                    continue
                
                # --- LÓGICA DE CATEGORIZAÇÃO ATUALIZADA ---
                # 1. Tenta mapear pelo código 'SubConta' (ex: '01.01.000')
                account_info = Config.CHART_OF_ACCOUNTS.get(sub_conta)
                
                # 2. Se não encontrar, tenta mapear por palavra-chave na descrição
                if not account_info:
                    for key, info in Config.DESCRIPTION_MAPPING.items():
                        if key in descricao:
                            account_info = info
                            break
                
                # 3. Se ainda não encontrar, classifica como 'Outros'
                if not account_info:
                    if "TRANSFERENCIA ENTRE CONTAS" in descricao:
                        continue
                    account_info = {"group": "Outros", "subgroup": "Não Categorizado"}

                # --- LÓGICA DE SINAL CORRIGIDA E MAIS EXPLÍCITA ---
                final_value = valor

                # Regra 1: Itens em grupos de "Receita" DEVEM ser positivos.
                if "Receita" in account_info["group"]:
                    final_value = abs(valor)
                # Regra 2: Itens em grupos de "Despesa" (e similares) DEVEM ser negativos.
                elif any(keyword in account_info["group"] for keyword in ["Despesas", "Impostos", "Investimentos", "Dividendos"]):
                    final_value = -abs(valor)
                # Regra 3: Para outros casos (ex: "Outros", "Ajustes"), confia no sinal do arquivo.
                # Nenhuma ação é necessária aqui, pois final_value já é igual a 'valor'.

                details.append({
                    "group": account_info["group"],
                    "subgroup": account_info["subgroup"],
                    "indicator": str(row['Descrição']).strip(),
                    "value": final_value
                })
            return details, None
        except Exception as e:
            return None, f"Erro inesperado ao processar o detalhamento '{os.path.basename(filepath)}': {e}"


# ==============================================================================
# --- 5. EXPORTADORES (Sem alterações) ---
# ==============================================================================
class PDFExporter:
    def export(self, unit_name: str, period_title: str, results_data: Dict[str, Dict[str, float]]):
        filepath = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            initialfile=f"DRE_{unit_name.replace(' ', '_')}_{period_title.replace('/', '-')}.pdf"
        )
        if not filepath: return
        
        doc = SimpleDocTemplate(filepath, pagesize=(8.5*inch, 11*inch), topMargin=inch, bottomMargin=inch)
        styles = getSampleStyleSheet()
        elements = []

        styles.add(ParagraphStyle(name='Right', alignment=TA_RIGHT))
        styles.add(ParagraphStyle(name='Center', alignment=TA_CENTER))
        styles.add(ParagraphStyle(name='ResultGreen', parent=styles['h3'], textColor=colors.darkgreen, alignment=TA_RIGHT))
        styles.add(ParagraphStyle(name='ResultRed', parent=styles['h3'], textColor=colors.red, alignment=TA_RIGHT))

        elements.append(Paragraph(f"<b>Relatório DRE - {unit_name}</b>", styles['h1']))
        elements.append(Paragraph(f"Período de Análise: {period_title}", styles['h2']))
        elements.append(Spacer(1, 0.3*inch))

        total_geral = {"receitas": 0.0, "despesas": 0.0}
        
        for collector, data in results_data.items():
            total_geral["receitas"] += data["receitas"]
            total_geral["despesas"] += data["despesas"]
            net_result = data["receitas"] + data["despesas"]

            table_data = [
                [Paragraph(f"<b>{collector}</b>", styles['h4']), ""],
                ["Total de Receitas:", Paragraph(f"R$ {data['receitas']:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), styles['Right'])],
                ["Total de Despesas:", Paragraph(f"R$ {abs(data['despesas']):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), styles['Right'])],
                [Paragraph("<b>Resultado:</b>", styles['Normal']), Paragraph(f"<b>R$ {net_result:,.2f}</b>".replace(",", "X").replace(".", ",").replace("X", "."), styles['ResultGreen'] if net_result >= 0 else styles['ResultRed'])],
            ]
            
            tbl = Table(table_data, colWidths=[3*inch, 2*inch])
            style = TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('SPAN', (0, 0), (1, 0)),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
                ('TOPPADDING', (0, 0), (-1, -1), 12),
            ])
            tbl.setStyle(style)
            elements.append(tbl)
            elements.append(Spacer(1, 0.2*inch))

        resultado_geral = total_geral["receitas"] + total_geral["despesas"]
        total_table_data = [
            [Paragraph("<b>Resultado Geral do Período</b>", styles['h3']), Paragraph(f"<b>R$ {resultado_geral:,.2f}</b>".replace(",", "X").replace(".", ",").replace("X", "."), styles['ResultGreen'] if resultado_geral >= 0 else styles['ResultRed'])]
        ]
        total_tbl = Table(total_table_data, colWidths=[3*inch, 2*inch])
        elements.append(Spacer(1, 0.3*inch))
        elements.append(total_tbl)

        try:
            doc.build(elements)
            messagebox.showinfo("Sucesso", f"Relatório salvo em:\n{filepath}")
        except Exception as e:
            messagebox.showerror("Erro ao Salvar PDF", f"Não foi possível gerar o PDF.\nErro: {e}")

class ExcelExporter:
    def export(self, period_title: str, results_data: Dict[str, Any]):
        filepath = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            initialfile=f"DRE_Consolidado_{period_title.replace('/', '-')}.xlsx"
        )
        if not filepath: return

        try:
            df = pd.DataFrame.from_dict(results_data, orient='index')
            df.index.name = "Arrecadadora"
            df.rename(columns={"receitas": "Total Receitas", "despesas": "Total Despesas"}, inplace=True)
            df["Resultado"] = df["Total Receitas"] + df["Total Despesas"]
            
            total_row = df.sum(numeric_only=True)
            total_row.name = "TOTAL GERAL"
            df = pd.concat([df, pd.DataFrame(total_row).T])

            df.to_excel(filepath, sheet_name="DRE_Consolidado")
            messagebox.showinfo("Sucesso", f"Relatório salvo em:\n{filepath}")
        except Exception as e:
            messagebox.showerror("Erro ao Salvar Excel", f"Não foi possível gerar o arquivo.\nErro: {e}")

# ==============================================================================
# --- 6. APLICATIVO PRINCIPAL E GERENCIADOR DE TELAS (Sem alterações) ---
# ==============================================================================
class App(ctk.CTk):
    def __init__(self, db_manager: DatabaseManager, file_manager: FileManager, data_processor: DataProcessor, pdf_exporter: PDFExporter, excel_exporter: ExcelExporter):
        super().__init__()
        self.db_manager = db_manager
        self.file_manager = file_manager
        self.data_processor = data_processor
        self.pdf_exporter = pdf_exporter
        self.excel_exporter = excel_exporter

        self.title(Config.APP_NAME)
        self.geometry("1200x800")
        self.minsize(1100, 750)

        self.grid_rowconfigure(2, weight=1)
        self.grid_columnconfigure(0, weight=1)

        self.header_frame = ctk.CTkFrame(self, height=80, corner_radius=0)
        self.header_frame.grid(row=0, column=0, sticky="ew")
        self.header_frame.grid_columnconfigure(1, weight=1)
        
        self.breadcrumb_frame = ctk.CTkFrame(self, height=30, corner_radius=0)
        self.breadcrumb_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=(0,5))
        self.breadcrumbs = []

        self.help_button = ctk.CTkButton(self.header_frame, text="Ajuda / Sobre", command=self.show_help_screen, width=120)
        self.help_button.grid(row=0, column=0, padx=20, sticky="w")
        self.header_label = ctk.CTkLabel(self.header_frame, text="", font=ctk.CTkFont(size=24, weight="bold"))
        self.header_label.grid(row=0, column=1, pady=20, sticky="w", padx=20)
        
        self.content_container = ctk.CTkFrame(self, fg_color="transparent")
        self.content_container.grid(row=2, column=0, sticky="nsew", padx=20, pady=20)
        
        self.current_frame_class = None
        self.current_frame_kwargs = {}
        self.current_frame = None
        
        self.update_theme()
        self.show_frame(SplashScreen, breadcrumb_path=[("Início", SplashScreen)])
        self.after(500, file_manager.check_and_prompt_for_backup)

    def show_frame(self, frame_class, breadcrumb_path: List[Tuple[str, type]], **kwargs):
        self.current_frame_class = frame_class
        self.current_frame_kwargs = {k: v for k, v in kwargs.items() if k not in ['parent', 'controller', 'breadcrumb_path']}

        if self.current_frame: self.current_frame.destroy()
        
        base_kwargs = {
            "db_manager": self.db_manager, "file_manager": self.file_manager,
            "data_processor": self.data_processor, "pdf_exporter": self.pdf_exporter,
            "excel_exporter": self.excel_exporter
        }
        base_kwargs.update(kwargs)
        
        self.current_frame = frame_class(parent=self.content_container, controller=self, breadcrumb_path=breadcrumb_path, **base_kwargs)
        self.current_frame.pack(fill="both", expand=True)
        self.update_breadcrumbs(breadcrumb_path)
        self.update_theme()

    def update_breadcrumbs(self, path: List[Tuple[str, type]]):
        for widget in self.breadcrumb_frame.winfo_children():
            widget.destroy()
        
        self.breadcrumbs = path
        
        if path:
            self.header_label.configure(text=path[-1][0])

        for i, (text, frame_class) in enumerate(path):
            is_last = i == len(path) - 1
            nav_path = path[:i+1]
            
            btn = ctk.CTkButton(
                self.breadcrumb_frame,
                text=text,
                command=lambda p=nav_path: self.navigate_back(p),
                state="disabled" if is_last else "normal",
                fg_color="transparent",
                text_color=(Config.COLOR_PRIMARY_GREEN if not is_last else Config.COLOR_TEXT_LIGHT),
                hover=not is_last
            )
            btn.pack(side="left", padx=(0,0))
            
            if not is_last:
                ctk.CTkLabel(self.breadcrumb_frame, text=" > ", text_color=Config.COLOR_TEXT_LIGHT).pack(side="left")

    def navigate_back(self, path):
        frame_class = path[-1][1]
        kwargs_for_frame = {}
        if frame_class == UnitDashboard:
            unit_name = path[-1][0].replace("Painel: ", "")
            kwargs_for_frame['unit_name'] = unit_name
        
        self.show_frame(frame_class, breadcrumb_path=path, **kwargs_for_frame)

    def update_theme(self):
        self.configure(fg_color=Config.COLOR_BACKGROUND)
        self.header_frame.configure(fg_color=Config.COLOR_PRIMARY_GREEN)
        self.header_label.configure(text_color=Config.COLOR_BUTTON_TEXT_LIGHT)
        self.breadcrumb_frame.configure(fg_color=Config.COLOR_FRAME)
        
        self.help_button.configure(
            fg_color=Config.COLOR_FRAME, 
            text_color=Config.COLOR_PRIMARY_GREEN,
            hover_color="#EFEFEF"
        )

        if self.current_frame_class == SplashScreen:
            self.header_frame.grid_remove()
            self.breadcrumb_frame.grid_remove()
        else:
            self.header_frame.grid()
            self.breadcrumb_frame.grid()

    def show_help_screen(self):
        path = self.breadcrumbs + [("Ajuda e Sobre", HelpScreen)]
        self.show_frame(HelpScreen, breadcrumb_path=path)

# ==============================================================================
# --- 7. TELAS DO APLICATIVO (Sem alterações) ---
# ==============================================================================
class BaseFrame(ctk.CTkFrame):
    # declare collaborators for static analysis
    db_manager: Optional[DatabaseManager]
    file_manager: Optional[FileManager]
    data_processor: Optional[DataProcessor]
    pdf_exporter: Optional[PDFExporter]
    excel_exporter: Optional[ExcelExporter]

    def __init__(self, parent, controller, **kwargs):
        super().__init__(parent, fg_color="transparent")
        self.controller = controller
        self.breadcrumb_path = kwargs.pop('breadcrumb_path')
        # Expect the app to pass shared collaborators; fail early if missing to avoid confusing None usage later
        self.db_manager = kwargs.get("db_manager")
        self.file_manager = kwargs.get("file_manager")
        self.data_processor = kwargs.get("data_processor")
        self.pdf_exporter = kwargs.get("pdf_exporter")
        self.excel_exporter = kwargs.get("excel_exporter")
        if self.db_manager is None:
            raise RuntimeError("BaseFrame requires a db_manager instance (was None)")
        if self.file_manager is None:
            raise RuntimeError("BaseFrame requires a file_manager instance (was None)")
        # tell static type checker these are concrete types
        self.db_manager = cast(DatabaseManager, self.db_manager)
        self.file_manager = cast(FileManager, self.file_manager)

        self.theme_colors = {
            "bg": Config.COLOR_BACKGROUND,
            "frame": Config.COLOR_FRAME,
            "text": Config.COLOR_TEXT,
            "text_light": Config.COLOR_TEXT_LIGHT,
            "primary": Config.COLOR_PRIMARY_GREEN,
            "secondary": Config.COLOR_SECONDARY_YELLOW,
            "button_primary_fg": Config.COLOR_PRIMARY_GREEN,
            "button_primary_text": Config.COLOR_BUTTON_TEXT_LIGHT,
            "button_secondary_fg": Config.COLOR_SECONDARY_YELLOW,
            "button_secondary_text": Config.COLOR_BUTTON_TEXT_DARK,
        }
    def db(self) -> DatabaseManager:
        """Return db_manager with correct type for callers."""
        return cast(DatabaseManager, self.db_manager)

    def fm(self) -> FileManager:
        """Return file_manager with correct type for callers."""
        return cast(FileManager, self.file_manager)

class SplashScreen(BaseFrame):
    def __init__(self, parent, controller, **kwargs):
        super().__init__(parent, controller, **kwargs)
        self.configure(fg_color=self.theme_colors["bg"])
        splash_frame = ctk.CTkFrame(self, fg_color="transparent")
        splash_frame.place(relx=0.5, rely=0.5, anchor="center")
        ctk.CTkLabel(splash_frame, text=Config.APP_NAME, font=ctk.CTkFont(size=40, weight="bold"), text_color=self.theme_colors["primary"]).pack(pady=20)
        ctk.CTkLabel(splash_frame, text=Config.CREATOR_NAME, font=ctk.CTkFont(size=16), text_color=self.theme_colors["text"]).pack(pady=10)
        self.progressbar = ctk.CTkProgressBar(splash_frame, progress_color=self.theme_colors["primary"], mode="indeterminate")
        self.progressbar.pack(pady=20, padx=50, fill="x")
        self.progressbar.start()
        self.after(2000, lambda: controller.show_frame(MainMenu, breadcrumb_path=[("Dashboard Central", MainMenu)]))

class MainMenu(BaseFrame):
    def __init__(self, parent, controller, **kwargs):
        super().__init__(parent, controller, **kwargs)
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=3)
        self.grid_rowconfigure(0, weight=1)

        actions_frame = ctk.CTkFrame(self, fg_color=self.theme_colors["frame"])
        actions_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        actions_frame.pack_propagate(False)

        ctk.CTkLabel(actions_frame, text="Ações Principais", font=ctk.CTkFont(size=18, weight="bold")).pack(pady=20)
        
        ctk.CTkButton(actions_frame, text="Acessar Unidades", command=self.go_to_units, height=50, fg_color=self.theme_colors["button_primary_fg"], text_color=self.theme_colors["button_primary_text"]).pack(fill="x", padx=20, pady=10)
        ctk.CTkButton(actions_frame, text="Cadastrar Nova Unidade", command=self.cadastrar_unidade, height=50, fg_color=self.theme_colors["button_secondary_fg"], text_color=self.theme_colors["button_secondary_text"]).pack(fill="x", padx=20, pady=10)
        ctk.CTkButton(actions_frame, text="Gerenciamento", command=self.go_to_management, height=50).pack(fill="x", padx=20, pady=10)
        ctk.CTkButton(actions_frame, text="Editar Mapeamentos", command=self.go_to_mappings, height=50, fg_color=Config.COLOR_BLUE, text_color=Config.COLOR_BUTTON_TEXT_LIGHT).pack(fill="x", padx=20, pady=10)
        ctk.CTkButton(actions_frame, text="Sair", command=self.controller.destroy, height=50, fg_color=Config.COLOR_RED).pack(fill="x", padx=20, pady=(40,10))

        kpi_frame = ctk.CTkFrame(self, fg_color="transparent")
        kpi_frame.grid(row=0, column=1, sticky="nsew", padx=(10, 0))
        kpi_frame.grid_columnconfigure((0,1), weight=1)
        kpi_frame.grid_rowconfigure(1, weight=1)

        ctk.CTkLabel(kpi_frame, text=f"Resumo de {datetime.datetime.now():%B de %Y}", font=ctk.CTkFont(size=20, weight="bold")).grid(row=0, column=0, columnspan=2, pady=(0, 20), sticky="w")

        kpis = self.db().get_global_kpis_for_current_month()

        net_result_card = ctk.CTkFrame(kpi_frame, fg_color=self.theme_colors["frame"])
        net_result_card.grid(row=1, column=0, sticky="nsew", padx=(0,10))
        ctk.CTkLabel(net_result_card, text="Resultado Líquido Total", font=ctk.CTkFont(size=16)).pack(pady=(20,5))
        res_color = self.theme_colors["primary"] if kpis["total_net"] >= 0 else Config.COLOR_RED
        ctk.CTkLabel(net_result_card, text=f"R$ {kpis['total_net']:,.2f}", font=ctk.CTkFont(size=28, weight="bold"), text_color=res_color).pack(pady=(0,20))

        revenue_card = ctk.CTkFrame(kpi_frame, fg_color=self.theme_colors["frame"])
        revenue_card.grid(row=1, column=1, sticky="nsew", padx=(10,0))
        ctk.CTkLabel(revenue_card, text="Receita Bruta Total", font=ctk.CTkFont(size=16)).pack(pady=(20,5))
        ctk.CTkLabel(revenue_card, text=f"R$ {kpis['total_revenue']:,.2f}", font=ctk.CTkFont(size=28, weight="bold"), text_color=Config.COLOR_BLUE).pack(pady=(0,20))

        ranking_frame = ctk.CTkFrame(kpi_frame, fg_color=self.theme_colors["frame"])
        ranking_frame.grid(row=2, column=0, columnspan=2, sticky="nsew", pady=(20,0))
        ctk.CTkLabel(ranking_frame, text="Top 3 Unidades do Mês", font=ctk.CTkFont(size=18, weight="bold")).pack(pady=15)

        if not kpis["top_units"]:
            ctk.CTkLabel(ranking_frame, text="Nenhum dado para o mês atual.").pack(pady=20)
        else:
            for i, unit in enumerate(kpis["top_units"]):
                rank_text = f"{i+1}º. {unit['unit_name']}: R$ {unit['monthly_net']:,.2f}"
                ctk.CTkLabel(ranking_frame, text=rank_text, font=ctk.CTkFont(size=16)).pack(anchor="w", padx=30, pady=5)

    def go_to_units(self):
        path = self.breadcrumb_path + [("Seleção de Unidades", UnitSelectionScreen)]
        self.controller.show_frame(UnitSelectionScreen, breadcrumb_path=path)
    
    def cadastrar_unidade(self):
        dialog = ctk.CTkInputDialog(text="Digite o nome da nova unidade:", title="Cadastrar Unidade")
        unit_name = dialog.get_input()
        if unit_name:
            if self.fm().create_unit_folders(unit_name):
                messagebox.showinfo("Sucesso", f"Unidade '{unit_name}' criada com sucesso!")

    def go_to_management(self):
        path = self.breadcrumb_path + [("Gerenciamento", ManagementScreen)]
        self.controller.show_frame(ManagementScreen, breadcrumb_path=path)

    def go_to_mappings(self):
        path = self.breadcrumb_path + [("Editar Mapeamentos", ManagementScreen)]
        self.controller.show_frame(ManagementScreen, breadcrumb_path=path)
# --- ManagementScreen: tela para editar mapeamentos de grupo/subgrupo/nome ---
class ManagementScreen(BaseFrame):
    def __init__(self, parent, controller, **kwargs):
        super().__init__(parent, controller, **kwargs)
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(0, weight=1)
        self.mapping_type = tk.StringVar(value="CHART_OF_ACCOUNTS")
        self.selected_key = tk.StringVar()
        self.edit_fields = {}
        self.build_ui()

    def build_ui(self):
        frame = ctk.CTkFrame(self, fg_color=self.theme_colors["frame"])
        frame.grid(row=0, column=0, sticky="nsew", padx=30, pady=30)
        ctk.CTkLabel(frame, text="Editar Mapeamentos", font=ctk.CTkFont(size=22, weight="bold"), text_color=self.theme_colors["primary"]).pack(pady=10)

        # Mapping type selector
        type_frame = ctk.CTkFrame(frame, fg_color="transparent")
        type_frame.pack(pady=10)
        for label, key in [("Contas (CHART_OF_ACCOUNTS)", "CHART_OF_ACCOUNTS"), ("Descrição (DESCRIPTION_MAPPING)", "DESCRIPTION_MAPPING"), ("Nota Negócio (NOTAS_NEGOCIO_MAPPING)", "NOTAS_NEGOCIO_MAPPING")]:
            ctk.CTkRadioButton(type_frame, text=label, variable=self.mapping_type, value=key, command=self.refresh_list).pack(side="left", padx=10)

        # Listbox for keys
        self.listbox = tk.Listbox(frame, width=40, height=12)
        self.listbox.pack(pady=10)
        self.listbox.bind("<<ListboxSelect>>", self.on_select)

        # Edit fields
        self.fields_frame = ctk.CTkFrame(frame, fg_color="transparent")
        self.fields_frame.pack(pady=10)

        # Buttons
        btn_frame = ctk.CTkFrame(frame, fg_color="transparent")
        btn_frame.pack(pady=10)
        ctk.CTkButton(btn_frame, text="Salvar", command=self.save_mapping, fg_color=self.theme_colors["button_primary_fg"], text_color=self.theme_colors["button_primary_text"]).pack(side="left", padx=10)
        ctk.CTkButton(btn_frame, text="Adicionar Novo", command=self.add_new, fg_color=self.theme_colors["button_secondary_fg"], text_color=self.theme_colors["button_secondary_text"]).pack(side="left", padx=10)
        ctk.CTkButton(btn_frame, text="Excluir", command=self.delete_selected, fg_color=Config.COLOR_RED, text_color=Config.COLOR_BUTTON_TEXT_LIGHT).pack(side="left", padx=10)

        self.refresh_list()

    def get_mapping(self):
        t = self.mapping_type.get()
        if t == "CHART_OF_ACCOUNTS":
            return Config.CHART_OF_ACCOUNTS
        elif t == "DESCRIPTION_MAPPING":
            return Config.DESCRIPTION_MAPPING
        elif t == "NOTAS_NEGOCIO_MAPPING":
            return Config.NOTAS_NEGOCIO_MAPPING
        return {}

    def refresh_list(self):
        mapping = self.get_mapping()
        self.listbox.delete(0, tk.END)
        for k in sorted(mapping.keys()):
            self.listbox.insert(tk.END, k)
        self.selected_key.set("")
        self.clear_fields()

    def on_select(self, event):
        sel = self.listbox.curselection()
        if not sel:
            self.selected_key.set("")
            self.clear_fields()
            return
        key = self.listbox.get(sel[0])
        self.selected_key.set(key)
        mapping = self.get_mapping()
        value = mapping.get(key, {})
        self.show_fields(value)

    def show_fields(self, value):
        for widget in self.fields_frame.winfo_children():
            widget.destroy()
        self.edit_fields = {}
        # For CHART_OF_ACCOUNTS: group, subgroup
        # For DESCRIPTION_MAPPING: group, subgroup
        # For NOTAS_NEGOCIO_MAPPING: group, subgroup
        for field in ["group", "subgroup"]:
            val = value.get(field, "")
            entry = tk.Entry(self.fields_frame, width=40)
            entry.insert(0, val)
            entry.pack(pady=5)
            self.edit_fields[field] = entry
        # For CHART_OF_ACCOUNTS, also show nome (key)
        if self.mapping_type.get() == "CHART_OF_ACCOUNTS":
            ctk.CTkLabel(self.fields_frame, text=f"Código: {self.selected_key.get()}", font=ctk.CTkFont(size=14)).pack(pady=5)
        else:
            ctk.CTkLabel(self.fields_frame, text=f"Palavra-chave: {self.selected_key.get()}", font=ctk.CTkFont(size=14)).pack(pady=5)

    def clear_fields(self):
        for widget in self.fields_frame.winfo_children():
            widget.destroy()
        self.edit_fields = {}

    def save_mapping(self):
        key = self.selected_key.get()
        if not key:
            messagebox.showwarning("Seleção", "Selecione um item para editar.")
            return
        mapping = self.get_mapping()
        for field, entry in self.edit_fields.items():
            mapping.setdefault(key, {})[field] = entry.get()
        Config.save_mappings()
        self.refresh_list()
        messagebox.showinfo("Salvo", "Mapeamento salvo com sucesso.")

    def add_new(self):
        key = simpledialog.askstring("Novo Mapeamento", "Digite o código ou palavra-chave:")
        if not key:
            return
        mapping = self.get_mapping()
        if key in mapping:
            messagebox.showerror("Erro", "Já existe esse código/palavra-chave.")
            return
        mapping[key] = {"group": "", "subgroup": ""}
        Config.save_mappings()
        self.refresh_list()
        self.selected_key.set(key)
        self.show_fields(mapping[key])

    def delete_selected(self):
        key = self.selected_key.get()
        if not key:
            messagebox.showwarning("Seleção", "Selecione um item para excluir.")
            return
        mapping = self.get_mapping()
        if key in mapping:
            del mapping[key]
            Config.save_mappings()
            self.refresh_list()
            messagebox.showinfo("Excluído", "Mapeamento removido.")

class UnitSelectionScreen(BaseFrame):
    def __init__(self, parent, controller, **kwargs):
        super().__init__(parent, controller, **kwargs)
        self.populate_units()

    def populate_units(self):
        for widget in self.winfo_children():
            widget.destroy()

        units = self.fm().get_existing_units()
        if not units:
            ctk.CTkLabel(self, text="Nenhuma unidade cadastrada.\nCadastre uma no menu principal.", font=ctk.CTkFont(size=18), text_color=self.theme_colors["text"]).pack(expand=True)
        else:
            scroll_frame = ctk.CTkScrollableFrame(self, label_text="Unidades Cadastradas", label_text_color=self.theme_colors["text"])
            scroll_frame.pack(fill="both", expand=True, padx=20, pady=10)
            for unit_name in units:
                card = ctk.CTkFrame(scroll_frame, fg_color=self.theme_colors["frame"], corner_radius=10)
                card.pack(fill="x", padx=10, pady=8)
                ctk.CTkButton(card, text=unit_name, font=ctk.CTkFont(size=16, weight="bold"), height=60, command=lambda u=unit_name: self.select_unit(u), fg_color=self.theme_colors["button_primary_fg"], text_color=self.theme_colors["button_primary_text"]).pack(side="left", fill="x", expand=True, padx=(10,5), pady=10)
                ctk.CTkButton(card, text="Renomear", width=80, command=lambda u=unit_name: self.rename_unit(u)).pack(side="left", padx=5, pady=10)
                ctk.CTkButton(card, text="Excluir", width=80, fg_color=Config.COLOR_RED, command=lambda u=unit_name: self.delete_unit(u)).pack(side="left", padx=5, pady=10)

    def select_unit(self, unit_name: str):
        path = self.breadcrumb_path + [(f"Painel: {unit_name}", UnitDashboard)]
        self.controller.show_frame(UnitDashboard, breadcrumb_path=path, unit_name=unit_name)

    def rename_unit(self, old_name: str):
        dialog = ctk.CTkInputDialog(text=f"Novo nome para '{old_name}':", title="Renomear Unidade")
        new_name = dialog.get_input()
        if new_name and new_name.strip() and new_name != old_name:
            if self.fm().rename_unit(old_name, new_name):
                self.populate_units()

    def delete_unit(self, unit_name: str):
        if self.fm().delete_unit(unit_name):
            self.populate_units()

class UnitDashboard(BaseFrame):
    def __init__(self, parent, controller, unit_name: str, **kwargs):
        self.unit_name = unit_name
        super().__init__(parent, controller, **kwargs)
        self.grid_columnconfigure((0, 1), weight=1)
        self.grid_rowconfigure(0, weight=1)

        data_card = ctk.CTkFrame(self, fg_color=self.theme_colors["frame"], corner_radius=10)
        data_card.grid(row=0, column=0, padx=(0, 10), pady=10, sticky="nsew")
        ctk.CTkLabel(data_card, text="Gestão de Dados", font=ctk.CTkFont(size=18, weight="bold"), text_color=self.theme_colors["text"]).pack(pady=(20, 15), padx=20)
        ctk.CTkButton(data_card, text="Importar Dados do Mês", command=self.open_import_window, height=45, fg_color=self.theme_colors["button_primary_fg"], text_color=self.theme_colors["button_primary_text"]).pack(fill="x", pady=8, padx=20)
        ctk.CTkButton(data_card, text="Navegar nos Detalhes", command=self.browse_details, height=45).pack(fill="x", pady=8, padx=20)
        
        report_card = ctk.CTkFrame(self, fg_color=self.theme_colors["frame"], corner_radius=10)
        report_card.grid(row=0, column=1, padx=(10, 0), pady=10, sticky="nsew")
        ctk.CTkLabel(report_card, text="Relatórios e Análises", font=ctk.CTkFont(size=18, weight="bold"), text_color=self.theme_colors["text"]).pack(pady=(20, 15), padx=20)
        ctk.CTkButton(report_card, text="DRE Mensal", command=self.emitir_dre_mensal, height=45).pack(fill="x", pady=8, padx=20)
        ctk.CTkButton(report_card, text="DRE Trimestral", command=self.emitir_dre_trimestral, height=45).pack(fill="x", pady=8, padx=20)
        ctk.CTkButton(report_card, text="DRE Anual", command=self.emitir_dre_anual, height=45).pack(fill="x", pady=8, padx=20)
        ctk.CTkButton(report_card, text="Dashboard Anual", command=self.show_dashboard, height=45).pack(fill="x", pady=8, padx=20)
        ctk.CTkButton(report_card, text="Metas e Projeções", command=self.manage_goals, height=45).pack(fill="x", pady=8, padx=20)
        ctk.CTkButton(report_card, text="Análise Comparativa", command=self.compare_units, height=45, fg_color=self.theme_colors["button_secondary_fg"], text_color=self.theme_colors["button_secondary_text"]).pack(fill="x", pady=8, padx=20)

    def open_import_window(self):
        ImportDataWindow(parent=self, unit_name=self.unit_name, data_processor=self.data_processor, db_manager=self.db())

    def emitir_dre(self, start_month, end_month, period_text):
        results_df = self.db().get_detailed_results(self.unit_name, start_month, end_month)
        if not results_df.empty:
            path = self.breadcrumb_path + [(f"DRE: {period_text}", InteractiveDREScreen)]
            self.controller.show_frame(InteractiveDREScreen, breadcrumb_path=path, unit_name=self.unit_name, period_title=period_text, data_df=results_df)
        else:
            messagebox.showinfo("Sem Dados", f"Não há dados importados para {period_text}.")

    def emitir_dre_mensal(self):
        dialog = ctk.CTkInputDialog(text="Digite o mês da análise (ex: 01):", title="Mês da Análise")
        month_str = dialog.get_input()
        if month_str and month_str.isdigit() and 1 <= int(month_str) <= 12:
            month = int(month_str)
            self.emitir_dre(month, month, f"{month:02d}/{Config.CURRENT_YEAR}")
        elif month_str is not None: messagebox.showerror("Erro", "Entrada inválida.")

    def emitir_dre_trimestral(self):
        dialog = ctk.CTkInputDialog(text="Digite o trimestre (1, 2, 3 ou 4):", title="Trimestre")
        q_str = dialog.get_input()
        if q_str and q_str.isdigit() and 1 <= int(q_str) <= 4:
            q = int(q_str); start_month = (q - 1) * 3 + 1; end_month = q * 3
            self.emitir_dre(start_month, end_month, f"{q}º Trimestre {Config.CURRENT_YEAR}")
        elif q_str is not None: messagebox.showerror("Erro", "Entrada inválida.")

    def emitir_dre_anual(self):
        self.emitir_dre(1, 12, f"Ano de {Config.CURRENT_YEAR}")

    def show_dashboard(self):
        data = self.db().get_annual_dashboard_data(self.unit_name)
        if not data.empty:
            path = self.breadcrumb_path + [("Dashboard Anual", DashboardScreen)]
            self.controller.show_frame(DashboardScreen, breadcrumb_path=path, unit_name=self.unit_name, data=data)
        else:
            messagebox.showinfo("Sem Dados", "Não há dados para gerar o dashboard.")

    def browse_details(self):
        path = self.breadcrumb_path + [("Detalhes de Arquivos", DetailsBrowserScreen)]
        self.controller.show_frame(DetailsBrowserScreen, breadcrumb_path=path, unit_name=self.unit_name)

    def manage_goals(self):
        path = self.breadcrumb_path + [("Metas e Projeções", ProjectionScreen)]
        self.controller.show_frame(ProjectionScreen, breadcrumb_path=path, unit_name=self.unit_name)

    def compare_units(self):
        path = self.breadcrumb_path + [("Configurar Comparação", UnitComparisonSetupScreen)]
        self.controller.show_frame(UnitComparisonSetupScreen, breadcrumb_path=path, current_unit=self.unit_name)

class ImportDataWindow(ctk.CTkToplevel):
    def __init__(self, parent, unit_name, data_processor, db_manager):
        super().__init__(parent)
        self.unit_name = unit_name
        self.data_processor = data_processor
        self.db_manager = db_manager
        
        self.notas_files = []
        self.detalhamento_files = []

        self.title("Importar Dados do Mês")
        self.geometry("600x450")
        self.transient(parent)
        self.grab_set()

        ctk.CTkLabel(self, text="Mês da Importação (ex: 08):").pack(pady=(10,0))
        self.month_entry = ctk.CTkEntry(self)
        self.month_entry.pack(pady=5)

        ctk.CTkButton(self, text="Selecionar Notas de Negócio (.csv)", command=self.select_notas).pack(fill="x", padx=20, pady=10)
        self.notas_label = ctk.CTkLabel(self, text="Nenhum arquivo selecionado.")
        self.notas_label.pack()

        ctk.CTkButton(self, text="Selecionar Detalhamento Financeiro (.csv)", command=self.select_detalhamento).pack(fill="x", padx=20, pady=10)
        self.detalhamento_label = ctk.CTkLabel(self, text="Nenhum arquivo selecionado.")
        self.detalhamento_label.pack()

        ctk.CTkButton(self, text="Processar e Salvar", command=self.process, height=40, fg_color=Config.COLOR_PRIMARY_GREEN, text_color=Config.COLOR_BUTTON_TEXT_LIGHT).pack(fill="x", padx=20, pady=20)
        
        self.protocol("WM_DELETE_WINDOW", self.destroy)

    def select_notas(self):
        files = filedialog.askopenfilenames(title="Selecione as Notas de Negócio", filetypes=[("CSV files", "*.csv")])
        if files:
            self.notas_files = list(files)
            self.notas_label.configure(text=f"{len(self.notas_files)} arquivo(s) selecionado(s).")

    def select_detalhamento(self):
        files = filedialog.askopenfilenames(title="Selecione os Detalhamentos", filetypes=[("CSV files", "*.csv")])
        if files:
            self.detalhamento_files = list(files)
            self.detalhamento_label.configure(text=f"{len(self.detalhamento_files)} arquivo(s) selecionado(s).")

    def process(self):
        month_str = self.month_entry.get()
        if not (month_str and month_str.isdigit() and 1 <= int(month_str) <= 12):
            messagebox.showerror("Erro", "Mês inválido. Por favor, digite um número de 1 a 12.", parent=self)
            return
        
        if not self.notas_files and not self.detalhamento_files:
            messagebox.showwarning("Aviso", "Nenhum arquivo foi selecionado para importação.", parent=self)
            return

        loading_window = ctk.CTkToplevel(self)
        loading_window.title("Processando")
        loading_window.geometry("300x100")
        loading_window.transient(self)
        loading_window.grab_set()
        loading_window.protocol("WM_DELETE_WINDOW", lambda: None)
        ctk.CTkLabel(loading_window, text="Importando planilhas...\nPor favor, aguarde.", font=ctk.CTkFont(size=14)).pack(expand=True)
        self.update_idletasks()

        all_details = []
        processed_files = []
        errors = []
        
        for filepath in self.notas_files:
            collector, details, error = self.data_processor.extract_from_notas_negocio(filepath)
            if error:
                errors.append(f"Erro em '{os.path.basename(filepath)}': {error}")
                continue
            if details:
                all_details.extend(details)
                processed_files.append(os.path.basename(filepath))
        
        for filepath in self.detalhamento_files:
            details, error = self.data_processor.extract_from_detalhamento(filepath)
            if error:
                errors.append(f"Erro em '{os.path.basename(filepath)}': {error}")
                continue
            if details:
                all_details.extend(details)
                processed_files.append(os.path.basename(filepath))

        loading_window.destroy()

        if errors:
            messagebox.showerror("Erros na Importação", "\n\n".join(errors), parent=self)

        if not all_details:
            messagebox.showwarning("Aviso", "Nenhum dado válido foi extraído dos arquivos selecionados.", parent=self)
            return

        source_file_name = f"Consolidado_{int(month_str):02d}-{Config.CURRENT_YEAR}"
        self.db_manager.save_imported_data(self.unit_name, int(month_str), source_file_name, all_details)
        
        messagebox.showinfo("Concluído", f"Processo de importação finalizado.\nArquivos processados:\n" + "\n".join(processed_files))
        self.destroy()

class InteractiveDREScreen(BaseFrame):
    def __init__(self, parent, controller, unit_name: str, period_title: str, data_df: pd.DataFrame, **kwargs):
        self.unit_name = unit_name
        self.period_title = period_title
        self.data_df = data_df
        super().__init__(parent, controller, **kwargs)

        summary_frame = ctk.CTkFrame(self, fg_color=self.theme_colors["frame"], corner_radius=10)
        summary_frame.pack(fill="x", padx=10, pady=(0, 10))
        summary_frame.grid_columnconfigure((0, 1, 2), weight=1)

        total_receitas = self.data_df[self.data_df['total_value'] > 0]['total_value'].sum()
        total_despesas = self.data_df[self.data_df['total_value'] < 0]['total_value'].sum()
        resultado_liquido = total_receitas + total_despesas

        receitas_card = ctk.CTkFrame(summary_frame, fg_color="transparent")
        receitas_card.grid(row=0, column=0, pady=10)
        ctk.CTkLabel(receitas_card, text="Total de Receitas", font=ctk.CTkFont(size=14, weight="bold")).pack()
        ctk.CTkLabel(receitas_card, text=f"R$ {total_receitas:,.2f}", font=ctk.CTkFont(size=20, weight="bold"), text_color=self.theme_colors["primary"]).pack()

        despesas_card = ctk.CTkFrame(summary_frame, fg_color="transparent")
        despesas_card.grid(row=0, column=1, pady=10)
        ctk.CTkLabel(despesas_card, text="Total de Despesas", font=ctk.CTkFont(size=14, weight="bold")).pack()
        ctk.CTkLabel(despesas_card, text=f"R$ {abs(total_despesas):,.2f}", font=ctk.CTkFont(size=20, weight="bold"), text_color=Config.COLOR_RED).pack()

        resultado_card = ctk.CTkFrame(summary_frame, fg_color="transparent")
        resultado_card.grid(row=0, column=2, pady=10)
        ctk.CTkLabel(resultado_card, text="Resultado Líquido", font=ctk.CTkFont(size=14, weight="bold")).pack()
        res_color = self.theme_colors["primary"] if resultado_liquido >= 0 else Config.COLOR_RED
        ctk.CTkLabel(resultado_card, text=f"R$ {resultado_liquido:,.2f}", font=ctk.CTkFont(size=20, weight="bold"), text_color=res_color).pack()

        scroll_frame = ctk.CTkScrollableFrame(self)
        scroll_frame.pack(fill="both", expand=True, padx=10, pady=10)

        grouped_data = self.data_df.groupby("group_name")
        group_dfs = {name: df for name, df in grouped_data}

        for group_name in Config.DRE_GROUP_ORDER:
            if group_name in group_dfs:
                group_df = group_dfs[group_name]
                card = CollapsibleCard(scroll_frame, group_name=str(group_name), data_df=group_df, theme_colors=self.theme_colors)
                card.pack(fill="x", pady=5, padx=5)
        
        for group_name, group_df in grouped_data:
            if group_name not in Config.DRE_GROUP_ORDER:
                card = CollapsibleCard(scroll_frame, group_name=str(group_name), data_df=group_df, theme_colors=self.theme_colors)
                card.pack(fill="x", pady=5, padx=5)

class CollapsibleCard(ctk.CTkFrame):
    def __init__(self, parent, group_name: str, data_df: pd.DataFrame, theme_colors: dict):
        super().__init__(parent, fg_color=theme_colors["frame"], corner_radius=10)
        self.theme_colors = theme_colors
        self.is_expanded = False

        total_group_value = data_df['total_value'].sum()
        
        is_expense_group = any(keyword in group_name for keyword in ["Despesas", "Impostos", "Investimentos", "Dividendos", "Ajustes"])
        header_color = Config.COLOR_RED if is_expense_group and total_group_value < 0 else theme_colors["primary"]
        
        self.header_frame = ctk.CTkFrame(self, fg_color="transparent", cursor="hand2")
        self.header_frame.pack(fill="x", padx=10, pady=5)
        self.header_frame.bind("<Button-1>", self.toggle_expand)

        self.toggle_icon = ctk.CTkLabel(self.header_frame, text="▶", font=ctk.CTkFont(size=14))
        self.toggle_icon.pack(side="left", padx=(5, 10))
        
        ctk.CTkLabel(self.header_frame, text=group_name, font=ctk.CTkFont(size=16, weight="bold"), text_color=theme_colors["text"]).pack(side="left", expand=True, anchor="w")
        ctk.CTkLabel(self.header_frame, text=f"R$ {total_group_value:,.2f}", font=ctk.CTkFont(size=16, weight="bold"), text_color=header_color).pack(side="right", padx=10)

        self.content_frame = ctk.CTkFrame(self, fg_color="transparent")
        
        subgrouped_data = data_df.groupby("subgroup_name")
        for subgroup_name, subgroup_df in subgrouped_data:
            ctk.CTkLabel(self.content_frame, text=f"  • {subgroup_name}", font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=20, pady=(5,0))
            for _, row in subgroup_df.iterrows():
                item_frame = ctk.CTkFrame(self.content_frame, fg_color="transparent")
                item_frame.pack(fill="x", padx=40)
                ctk.CTkLabel(item_frame, text=row['indicator'], justify="left", wraplength=400).pack(side="left", expand=True, anchor="w")
                ctk.CTkLabel(item_frame, text=f"R$ {row['total_value']:,.2f}").pack(side="right")

    def toggle_expand(self, event=None):
        self.is_expanded = not self.is_expanded
        if self.is_expanded:
            self.content_frame.pack(fill="x", expand=True, pady=5, padx=10)
            self.toggle_icon.configure(text="▼")
        else:
            self.content_frame.pack_forget()
            self.toggle_icon.configure(text="▶")

class DashboardScreen(BaseFrame):
    def __init__(self, parent, controller, unit_name: str, data: pd.DataFrame, **kwargs):
        self.unit_name = unit_name
        self.data = data
        super().__init__(parent, controller, **kwargs)

        top_frame = ctk.CTkFrame(self, fg_color="transparent")
        top_frame.pack(fill="x", padx=0, pady=(0, 10))
        top_frame.grid_columnconfigure((0, 1, 2), weight=1)

        results = self.data['total_net']
        best_month_val = results.max() if not results.empty else 0
        worst_month_val = results.min() if not results.empty else 0
        avg_month_val = results.mean() if not results.empty else 0
        
        best_month_card = ctk.CTkFrame(top_frame, fg_color=self.theme_colors["frame"]); best_month_card.grid(row=0, column=0, padx=(0,5), sticky="ew")
        ctk.CTkLabel(best_month_card, text="Melhor Mês", font=ctk.CTkFont(size=14, weight="bold"), text_color=self.theme_colors["text"]).pack(pady=(10,2))
        ctk.CTkLabel(best_month_card, text=f"R$ {best_month_val:,.2f}", font=ctk.CTkFont(size=20, weight="bold"), text_color=self.theme_colors["primary"]).pack(pady=(0,10))

        worst_month_card = ctk.CTkFrame(top_frame, fg_color=self.theme_colors["frame"]); worst_month_card.grid(row=0, column=1, padx=5, sticky="ew")
        ctk.CTkLabel(worst_month_card, text="Pior Mês", font=ctk.CTkFont(size=14, weight="bold"), text_color=self.theme_colors["text"]).pack(pady=(10,2))
        ctk.CTkLabel(worst_month_card, text=f"R$ {worst_month_val:,.2f}", font=ctk.CTkFont(size=20, weight="bold"), text_color=Config.COLOR_RED).pack(pady=(0,10))
        
        avg_month_card = ctk.CTkFrame(top_frame, fg_color=self.theme_colors["frame"]); avg_month_card.grid(row=0, column=2, padx=(5,0), sticky="ew")
        ctk.CTkLabel(avg_month_card, text="Resultado Médio", font=ctk.CTkFont(size=14, weight="bold"), text_color=self.theme_colors["text"]).pack(pady=(10,2))
        ctk.CTkLabel(avg_month_card, text=f"R$ {avg_month_val:,.2f}", font=ctk.CTkFont(size=20, weight="bold"), text_color=Config.COLOR_BLUE).pack(pady=(0,10))

        self.data['month'] = self.data['period'].apply(lambda x: int(x.split('/')[0]))
        self.data = self.data.sort_values('month')
        months = [f"{m:02d}/{Config.CURRENT_YEAR_SHORT}" for m in self.data['month']]
        
        fig = Figure(figsize=(8, 4), dpi=100, facecolor=self.theme_colors["frame"])
        ax = fig.add_subplot(111, facecolor=self.theme_colors["frame"])
        ax.bar(months, self.data['total_net'], color=[self.theme_colors["primary"] if x >= 0 else Config.COLOR_RED for x in self.data['total_net']])

        goal = self.db().get_unit_goal(self.unit_name)
        if goal > 0:
            ax.axhline(y=goal, color=Config.COLOR_SECONDARY_YELLOW, linestyle='--', linewidth=2, label=f'Meta: R$ {goal:,.2f}')
            ax.legend()

        ax.set_title(f"Resultado Líquido Mensal - {Config.CURRENT_YEAR}", color=self.theme_colors["text"])
        ax.set_ylabel("Resultado (R$)", color=self.theme_colors["text"])
        ax.tick_params(colors=self.theme_colors["text"])
        for spine in ax.spines.values():
            spine.set_edgecolor(self.theme_colors["text"])
        fig.tight_layout()

        canvas = FigureCanvasTkAgg(fig, master=self)
        canvas.draw()
        canvas.get_tk_widget().pack(side=ctk.TOP, fill=ctk.BOTH, expand=True, padx=0, pady=10)

class DetailsBrowserScreen(BaseFrame):
    def __init__(self, parent, controller, unit_name: str, **kwargs):
        self.unit_name = unit_name
        super().__init__(parent, controller, **kwargs)
        
        filter_frame = ctk.CTkFrame(self, fg_color="transparent")
        filter_frame.pack(fill="x", padx=10, pady=(0, 10))
        ctk.CTkLabel(filter_frame, text="Buscar por nome do arquivo:").pack(side="left")
        self.search_entry = ctk.CTkEntry(filter_frame)
        self.search_entry.pack(side="left", fill="x", expand=True, padx=(5,0))
        self.search_entry.bind("<KeyRelease>", self.filter_treeview)

        self.tree_container = ctk.CTkFrame(self, fg_color="transparent")
        self.tree_container.pack(fill="both", expand=True)
        self.tree = None
        self.no_files_label = None
        
        self.populate_treeview()

    def populate_treeview(self, search_term: Optional[str] = None):
        if self.tree: self.tree.destroy()
        if self.no_files_label: self.no_files_label.destroy()

        self.files_df = self.db().get_imported_files_summary(self.unit_name, search_term)

        if self.files_df.empty:
            text = "Nenhum arquivo encontrado." if search_term else "Nenhum arquivo importado."
            self.no_files_label = ctk.CTkLabel(self.tree_container, text=text, font=ctk.CTkFont(size=16))
            self.no_files_label.pack(expand=True)
            return
        
        style = ttk.Style()
        style.theme_use("default")
        style.configure("Treeview", background=self.theme_colors["frame"], foreground=self.theme_colors["text"], fieldbackground=self.theme_colors["frame"], rowheight=25, borderwidth=0)
        style.map('Treeview', background=[('selected', self.theme_colors["primary"])])
        style.configure("Treeview.Heading", background=self.theme_colors["primary"], foreground=self.theme_colors["button_primary_text"], font=('Arial', 10, 'bold'))
        
        self.tree = ttk.Treeview(self.tree_container, columns=("Periodo", "Arquivo", "Resultado"), show="headings")
        self.tree.heading("Periodo", text="Período"); self.tree.heading("Arquivo", text="Arquivo de Origem")
        self.tree.heading("Resultado", text="Resultado Líquido")
        self.tree.column("Periodo", width=100, anchor="center"); self.tree.column("Arquivo", width=300)
        self.tree.column("Resultado", width=150, anchor="e")
        
        for _, row in self.files_df.iterrows():
            res_str = f"R$ {row['net_result']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            self.tree.insert("", "end", values=(row['period'], row['source_file'], res_str), iid=row['id'])
        
        self.tree.pack(fill="both", expand=True, padx=10, pady=0)
        self.tree.bind("<Double-1>", self.on_double_click)

    def filter_treeview(self, event=None):
        self.populate_treeview(self.search_entry.get())

    def on_double_click(self, event):
        if not self.tree:
            return
        sel = self.tree.selection()
        if not sel:
            return
        item_id = sel[0]
        selected_row = self.files_df[self.files_df['id'] == int(item_id)].iloc[0]
        path = self.breadcrumb_path + [(f"Detalhes: {selected_row['source_file'][:20]}...", FileDetailsScreen)]
        self.controller.show_frame(FileDetailsScreen, breadcrumb_path=path, unit_name=self.unit_name, summary_id=int(item_id))

class FileDetailsScreen(BaseFrame):
    def __init__(self, parent, controller, unit_name: str, summary_id: int, **kwargs):
        self.unit_name = unit_name
        self.summary_id = summary_id
        super().__init__(parent, controller, **kwargs)
        
        df = self.db().get_file_details(self.summary_id)

        style = ttk.Style()
        style.theme_use("default")
        style.configure("Treeview", background=self.theme_colors["frame"], foreground=self.theme_colors["text"], fieldbackground=self.theme_colors["frame"], rowheight=25, borderwidth=0)
        style.map('Treeview', background=[('selected', self.theme_colors["primary"])])
        style.configure("Treeview.Heading", background=self.theme_colors["primary"], foreground=self.theme_colors["button_primary_text"], font=('Arial', 10, 'bold'))

        tree = ttk.Treeview(self, columns=("Grupo", "Subgrupo", "Indicador", "Valor"), show="headings")
        tree.heading("Grupo", text="Grupo"); tree.heading("Subgrupo", text="Subgrupo")
        tree.heading("Indicador", text="Indicador"); tree.heading("Valor", text="Valor")
        tree.column("Valor", anchor="e")
        for _, row in df.iterrows():
            val_str = f"R$ {row['value']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            tree.insert("", "end", values=(row['group_name'], row['subgroup_name'], row['indicator'], val_str))
        tree.pack(fill="both", expand=True, padx=10, pady=10)

class ProjectionScreen(BaseFrame):
    def __init__(self, parent, controller, unit_name: str, **kwargs):
        self.unit_name = unit_name
        super().__init__(parent, controller, **kwargs)
        
        current_goal = self.db().get_unit_goal(self.unit_name)

        main_frame = ctk.CTkFrame(self, fg_color=self.theme_colors["frame"])
        main_frame.pack(expand=True, padx=100, pady=100)

        ctk.CTkLabel(main_frame, text="Defina uma meta de lucro líquido mensal.", font=ctk.CTkFont(size=18), text_color=self.theme_colors["text"]).pack(pady=20, padx=40)
        
        entry_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        entry_frame.pack(pady=10, padx=40)
        ctk.CTkLabel(entry_frame, text="R$", font=ctk.CTkFont(size=18), text_color=self.theme_colors["text"]).pack(side="left")
        self.goal_entry = ctk.CTkEntry(entry_frame, font=ctk.CTkFont(size=18), width=200)
        self.goal_entry.insert(0, f"{current_goal:.2f}")
        self.goal_entry.pack(side="left", padx=10)

        ctk.CTkButton(main_frame, text="Salvar Meta", command=self.save_goal, height=40, fg_color=self.theme_colors["button_primary_fg"], text_color=self.theme_colors["button_primary_text"]).pack(pady=20, padx=40)

    def save_goal(self):
        try:
            goal_value = float(self.goal_entry.get().replace(",", "."))
            self.db().set_unit_goal(self.unit_name, goal_value)
            messagebox.showinfo("Sucesso", "Meta salva com sucesso!")
            self.controller.navigate_back(self.breadcrumb_path[:-1])
        except ValueError:
            messagebox.showerror("Erro", "Por favor, insira um valor numérico válido para a meta.")

class HelpScreen(BaseFrame):
    def __init__(self, parent, controller, **kwargs):
        super().__init__(parent, controller, **kwargs)

        scroll_frame = ctk.CTkScrollableFrame(self, fg_color=self.theme_colors["frame"])
        scroll_frame.pack(fill="both", expand=True, padx=10, pady=10)

        ctk.CTkLabel(scroll_frame, text="Como Usar o Aplicativo", font=ctk.CTkFont(size=20, weight="bold"), text_color=self.theme_colors["text"]).pack(anchor="w", padx=20, pady=(10, 15))

        help_text = """
1. Cadastrar uma Unidade:
   - No Dashboard Central, clique em "Cadastrar Nova Unidade".
   - Digite o nome da nova unidade.

2. Importar os Dados:
   - No aplicativo, acesse a unidade desejada.
   - Clique em "Importar Dados do Mês".
   - Digite o número do mês que você quer importar (ex: 7 para Julho).
   - Selecione todos os arquivos CSV de "Nota de Negócio" daquele mês.
   - Selecione o arquivo CSV de "Detalhamento Financeiro" daquele mês.
   - Clique em "Processar e Salvar". O sistema lerá os arquivos e os salvará no banco de dados.

3. Gerar Relatórios e Análises:
   - Após importar, use os botões no painel da unidade para gerar DREs interativas,
     visualizar o Dashboard Anual ou comparar a performance com outras unidades.
"""
        ctk.CTkLabel(scroll_frame, text=help_text, font=ctk.CTkFont(size=14), text_color=self.theme_colors["text"], justify="left").pack(anchor="w", padx=20, pady=5)

        ctk.CTkLabel(scroll_frame, text="Sobre o Aplicativo", font=ctk.CTkFont(size=20, weight="bold"), text_color=self.theme_colors["text"]).pack(anchor="w", padx=20, pady=(20, 15))
        
        about_text = f"""
{Config.APP_NAME}
Versão: {Config.APP_VERSION}
Desenvolvido por: {Config.CREATOR_NAME}

Este aplicativo foi criado para simplificar a consolidação e análise de Demonstrativos de Resultados (DRE), oferecendo uma visão clara e objetiva da saúde financeira de cada unidade.
"""
        ctk.CTkLabel(scroll_frame, text=about_text, font=ctk.CTkFont(size=14), text_color=self.theme_colors["text"], justify="left").pack(anchor="w", padx=20, pady=5)

class UnitComparisonSetupScreen(BaseFrame):
    def __init__(self, parent, controller, current_unit: str, **kwargs):
        self.current_unit = current_unit
        super().__init__(parent, controller, **kwargs)
        self.unit_vars = {}

        main_frame = ctk.CTkFrame(self, fg_color=self.theme_colors["frame"])
        main_frame.pack(expand=True, padx=50, pady=50)

        ctk.CTkLabel(main_frame, text="Selecione as Unidades para Comparar", font=ctk.CTkFont(size=18, weight="bold")).pack(pady=20)

        units_frame = ctk.CTkScrollableFrame(main_frame, label_text="Unidades Disponíveis")
        units_frame.pack(fill="both", expand=True, padx=20, pady=10)
        all_units = self.fm().get_existing_units()
        for unit in all_units:
            var = ctk.StringVar(value="on" if unit == current_unit else "off")
            cb = ctk.CTkCheckBox(units_frame, text=unit, variable=var, onvalue="on", offvalue="off")
            cb.pack(anchor="w", padx=10, pady=5)
            self.unit_vars[unit] = var

        ctk.CTkLabel(main_frame, text="Selecione o Período", font=ctk.CTkFont(size=16)).pack(pady=(20,5))
        self.period_var = ctk.StringVar(value="Mês Atual")
        period_menu = ctk.CTkOptionMenu(main_frame, variable=self.period_var, values=["Mês Atual", "Último Trimestre", "Ano Inteiro"])
        period_menu.pack(pady=10)

        ctk.CTkButton(main_frame, text="Gerar Comparativo", command=self.generate_comparison, height=40).pack(pady=20)

    def generate_comparison(self):
        selected_units = [unit for unit, var in self.unit_vars.items() if var.get() == "on"]
        if len(selected_units) < 2:
            messagebox.showwarning("Seleção Inválida", "Por favor, selecione pelo menos duas unidades para comparar.")
            return

        now = datetime.datetime.now()
        current_month = now.month
        
        if current_month <= 3: q_start, q_end = 1, 3
        elif current_month <= 6: q_start, q_end = 4, 6
        elif current_month <= 9: q_start, q_end = 7, 9
        else: q_start, q_end = 10, 12

        period_map = {
            "Mês Atual": (current_month, current_month),
            "Último Trimestre": (q_start, q_end),
            "Ano Inteiro": (1, 12)
        }
        start, end = period_map[self.period_var.get()]

        data = self.db().get_comparison_data(selected_units, start, end)

        if data.empty:
            messagebox.showinfo("Sem Dados", "Nenhuma das unidades selecionadas possui dados para o período escolhido.")
            return
        
        path = self.breadcrumb_path + [("Resultado da Comparação", UnitComparisonResultScreen)]
        self.controller.show_frame(UnitComparisonResultScreen, breadcrumb_path=path, data=data, period_title=self.period_var.get())

class UnitComparisonResultScreen(BaseFrame):
    def __init__(self, parent, controller, data: pd.DataFrame, period_title: str, **kwargs):
        self.data = data
        self.period_title = period_title
        super().__init__(parent, controller, **kwargs)
        
        self.grid_rowconfigure(0, weight=2)
        self.grid_rowconfigure(1, weight=1)
        self.grid_columnconfigure(0, weight=1)

        fig = Figure(figsize=(8, 4), dpi=100, facecolor=self.theme_colors["frame"])
        ax = fig.add_subplot(111, facecolor=self.theme_colors["frame"])
        
        self.data.plot(kind='bar', x='unit_name', y=['total_revenue', 'net_result'], ax=ax, color=[Config.COLOR_BLUE, self.theme_colors["primary"]])
        
        ax.set_title(f"Comparativo de Performance - {self.period_title}", color=self.theme_colors["text"])
        ax.set_xlabel("Unidades", color=self.theme_colors["text"])
        ax.set_ylabel("Valor (R$)", color=self.theme_colors["text"])
        ax.tick_params(axis='x', labelrotation=0, colors=self.theme_colors["text"])
        ax.tick_params(axis='y', colors=self.theme_colors["text"])
        ax.legend(["Receita Total", "Resultado Líquido"])
        fig.tight_layout()

        canvas = FigureCanvasTkAgg(fig, master=self)
        canvas.draw()
        canvas.get_tk_widget().grid(row=0, column=0, sticky="nsew", padx=10, pady=10)

        table_frame = ctk.CTkFrame(self, fg_color=self.theme_colors["frame"])
        table_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=10)
        
        style = ttk.Style()
        style.theme_use("default")
        style.configure("Treeview", background=self.theme_colors["frame"], foreground=self.theme_colors["text"], fieldbackground=self.theme_colors["frame"], rowheight=25, borderwidth=0)
        style.map('Treeview', background=[('selected', self.theme_colors["primary"])])
        style.configure("Treeview.Heading", background=self.theme_colors["primary"], foreground=self.theme_colors["button_primary_text"], font=('Arial', 10, 'bold'))
        
        tree = ttk.Treeview(table_frame, columns=("Unidade", "Receita", "Resultado"), show="headings")
        tree.heading("Unidade", text="Unidade")
        tree.heading("Receita", text="Receita Total")
        tree.heading("Resultado", text="Resultado Líquido")
        tree.column("Receita", anchor="e"); tree.column("Resultado", anchor="e")

        for _, row in self.data.iterrows():
            tree.insert("", "end", values=(row['unit_name'], f"R$ {row['total_revenue']:,.2f}", f"R$ {row['net_result']:,.2f}"))
        
        tree.pack(fill="both", expand=True, padx=10, pady=10)

class ManagementScreen(BaseFrame):
    def __init__(self, parent, controller, **kwargs):
        super().__init__(parent, controller, **kwargs)
        self.grid_columnconfigure((0, 1), weight=1)
        self.grid_rowconfigure(0, weight=1)

        collector_card = ctk.CTkFrame(self, fg_color=self.theme_colors["frame"], corner_radius=10)
        collector_card.grid(row=0, column=0, sticky="nsew", padx=(0, 10), pady=10)
        ctk.CTkLabel(collector_card, text="Gerenciador de Arrecadadoras", font=ctk.CTkFont(size=18, weight="bold")).pack(pady=20)
        ctk.CTkLabel(collector_card, text="Padronize nomes e mescle entradas duplicadas.", wraplength=300).pack(pady=10, padx=20)
        ctk.CTkButton(collector_card, text="Acessar", command=self.go_to_collector_manager, height=45).pack(pady=20, padx=20)

        log_card = ctk.CTkFrame(self, fg_color=self.theme_colors["frame"], corner_radius=10)
        log_card.grid(row=0, column=1, sticky="nsew", padx=(10, 0), pady=10)
        ctk.CTkLabel(log_card, text="Visualizador de Logs", font=ctk.CTkFont(size=18, weight="bold")).pack(pady=20)
        ctk.CTkLabel(log_card, text="Audite todas as ações importantes realizadas no sistema.", wraplength=300).pack(pady=10, padx=20)
        ctk.CTkButton(log_card, text="Acessar", command=self.go_to_log_viewer, height=45).pack(pady=20, padx=20)

    def go_to_collector_manager(self):
        path = self.breadcrumb_path + [("Gerenciar Arrecadadoras", CollectorManagerScreen)]
        self.controller.show_frame(CollectorManagerScreen, breadcrumb_path=path)

    def go_to_log_viewer(self):
        path = self.breadcrumb_path + [("Logs de Atividade", LogViewerScreen)]
        self.controller.show_frame(LogViewerScreen, breadcrumb_path=path)

class CollectorManagerScreen(BaseFrame):
    def __init__(self, parent, controller, **kwargs):
        super().__init__(parent, controller, **kwargs)
        self.collector_vars = {}
        self.populate_collectors()

    def populate_collectors(self):
        for widget in self.winfo_children():
            widget.destroy()

        self.collector_vars = {}
        
        merge_frame = ctk.CTkFrame(self, fg_color=self.theme_colors["frame"])
        merge_frame.pack(fill="x", padx=10, pady=10)
        ctk.CTkLabel(merge_frame, text="Mesclar Selecionadas:").pack(side="left", padx=10)
        self.merge_button = ctk.CTkButton(merge_frame, text="Mesclar", command=self.merge_selected)
        self.merge_button.pack(side="left", padx=10)

        scroll_frame = ctk.CTkScrollableFrame(self, label_text="Arrecadadoras Registradas")
        scroll_frame.pack(fill="both", expand=True, padx=10, pady=10)

        collectors = self.db().get_distinct_collectors()
        for collector in collectors:
            card = ctk.CTkFrame(scroll_frame, fg_color=self.theme_colors["frame"])
            card.pack(fill="x", pady=5)
            
            var = ctk.StringVar(value="off")
            cb = ctk.CTkCheckBox(card, text="", variable=var, onvalue="on", offvalue="off")
            cb.pack(side="left", padx=10)
            self.collector_vars[collector] = var

            ctk.CTkLabel(card, text=collector, font=ctk.CTkFont(size=14)).pack(side="left", fill="x", expand=True)
            ctk.CTkButton(card, text="Renomear", command=lambda c=collector: self.rename_collector(c), width=100).pack(side="right", padx=10, pady=5)

    def rename_collector(self, old_name):
        dialog = ctk.CTkInputDialog(text=f"Novo nome para '{old_name}':", title="Renomear Arrecadadora")
        new_name = dialog.get_input()
        if new_name and new_name.strip() and new_name != old_name:
            self.db().update_collector_name(old_name, new_name)
            self.populate_collectors()

    def merge_selected(self):
        selected = [c for c, v in self.collector_vars.items() if v.get() == "on"]
        if len(selected) < 2:
            messagebox.showwarning("Seleção Inválida", "Selecione pelo menos duas arrecadadoras para mesclar.")
            return

        dialog = ctk.CTkInputDialog(text=f"Digite o nome final para mesclar {selected}:", title="Mesclar Arrecadadoras")
        final_name = dialog.get_input()
        if final_name and final_name.strip():
            self.db().merge_collectors(selected, final_name)
            self.populate_collectors()

class LogViewerScreen(BaseFrame):
    def __init__(self, parent, controller, **kwargs):
        super().__init__(parent, controller, **kwargs)
        
        logs_df = self.db().get_all_logs()

        style = ttk.Style()
        style.theme_use("default")
        style.configure("Treeview", background=self.theme_colors["frame"], foreground=self.theme_colors["text"], fieldbackground=self.theme_colors["frame"], rowheight=25, borderwidth=0)
        style.map('Treeview', background=[('selected', self.theme_colors["primary"])])
        style.configure("Treeview.Heading", background=self.theme_colors["primary"], foreground=self.theme_colors["button_primary_text"], font=('Arial', 10, 'bold'))

        tree = ttk.Treeview(self, columns=("Timestamp", "Ação", "Detalhes"), show="headings")
        tree.heading("Timestamp", text="Data e Hora")
        tree.heading("Ação", text="Tipo de Ação")
        tree.heading("Detalhes", text="Detalhes")
        tree.column("Timestamp", width=150); tree.column("Ação", width=150)

        for _, row in logs_df.iterrows():
            tree.insert("", "end", values=(row['timestamp'], row['action_type'], row['details']))
        
        tree.pack(fill="both", expand=True, padx=10, pady=10)

# ==============================================================================
# --- 8. PONTO DE ENTRADA DO PROGRAMA ---
# ==============================================================================
if __name__ == "__main__":
    ctk.set_appearance_mode(Config.CTK_APPEARANCE_MODE)

    db_manager = DatabaseManager(Config.DB_PATH)
    file_manager = FileManager(db_manager)
    data_processor = DataProcessor()
    pdf_exporter = PDFExporter()
    excel_exporter = ExcelExporter()

    app = App(db_manager, file_manager, data_processor, pdf_exporter, excel_exporter)
    app.mainloop()
