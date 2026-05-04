"""
╔══════════════════════════════════════════════════════════════╗
║  DAG: livestock_intelligence_etl                            ║
║  Livestock Intelligence — Airflow DAG (WSL)                 ║
║                                                              ║
║  Letakkan file ini di: $AIRFLOW_HOME/dags/                  ║
║                                                              ║
║  Struktur task:                                              ║
║    extract_bps ──┐                                          ║
║    extract_isikhnas ──┤──→ transform ──→ load ──→ validate  ║
║    extract_pihps ──┘                                        ║
╚══════════════════════════════════════════════════════════════╝
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ─────────────────────────────────────────────────────────────
# PATH KONFIGURASI
# ─────────────────────────────────────────────────────────────

# Path project di WSL (Windows path di-mount ke /mnt/c/...)
PROJECT_ROOT = (
    "/mnt/c/Users/anggi/Documents/KULIAH/TINGKAT 3/SEMESTER 6"
    "/TPD/PROJECT UTS/UTS-TPD-KELOMPOK-1"
)
SCRIPTS_DIR     = f"{PROJECT_ROOT}/airflow_scripts"
DATA_DIR        = f"{PROJECT_ROOT}/DATA"
TRANSFORM_OUT   = f"{DATA_DIR}/TRANSFORM_OUTPUT"
PIHPS_FILE      = f"{DATA_DIR}/PIHPS/final_data.xlsx"

# Tambahkan scripts dir ke sys.path agar bisa import
sys.path.insert(0, SCRIPTS_DIR)

# Python executable di WSL (bukan Python Windows!)
# Install dulu di WSL: sudo apt install python3-pip && pip install pyspark pandas sqlalchemy psycopg2-binary pymysql openpyxl
PYTHON_BIN = "python3"

# ─────────────────────────────────────────────────────────────
# KONFIGURASI DATABASE
# ─────────────────────────────────────────────────────────────

PG_USER      = "postgres"
PG_PASS      = "mydatabase"
PG_HOST      = "localhost"   # Dari WSL2, localhost = Windows host (port forwarding otomatis)
PG_PORT      = "5432"
STAGING_DB   = "staging_db"
DW_DB        = "datawarehouse_db"

MYSQL_USER   = "root"
MYSQL_PASS   = "mydatabase"
MYSQL_HOST   = "localhost"
MYSQL_PORT   = "3306"
ISIKHNAS_DB  = "isikhnas_db"

# ─────────────────────────────────────────────────────────────
# DEFAULT ARGS
# ─────────────────────────────────────────────────────────────

default_args = {
    "owner":            "kelompok1_tpd",
    "depends_on_past":  False,
    "start_date":       datetime(2025, 1, 1),
    "retries":          1,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": False,
}

# ─────────────────────────────────────────────────────────────
# TASK FUNCTIONS — EXTRACT
# Semua fungsi extract dipanggil dari extract_functions.py
# ─────────────────────────────────────────────────────────────

def task_extract_bps(**context):
    """
    Extract BPS API (scraping SIMDASI) + Generate BPS dummy
    → staging_db.staging_bps_api_raw
    → staging_db.staging_bps_dummy_raw
    """
    from extract_functions import run_extract_bps
    result = run_extract_bps(
        pg_url=f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{STAGING_DB}"
    )
    print(f"[BPS] Selesai: {result}")
    context["ti"].xcom_push(key="bps_result", value=result)


def task_extract_isikhnas(**context):
    """
    Extract iSIKHNAS dari MySQL → staging_db (6 tabel)
    """
    from extract_functions import run_extract_isikhnas
    result = run_extract_isikhnas(
        mysql_url=f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{ISIKHNAS_DB}",
        pg_url=f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{STAGING_DB}",
    )
    print(f"[iSIKHNAS] Selesai: {result}")
    context["ti"].xcom_push(key="isikhnas_result", value=result)


def task_extract_pihps(**context):
    """
    Extract PIHPS dari Excel → staging_db.staging_pihps_raw
    """
    from extract_functions import run_extract_pihps
    result = run_extract_pihps(
        excel_path=PIHPS_FILE,
        pg_url=f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{STAGING_DB}",
    )
    print(f"[PIHPS] Selesai: {result}")
    context["ti"].xcom_push(key="pihps_result", value=result)


def task_validate_dwh(**context):
    """
    Validasi pasca-load: cek row count dan FK null di DWH.
    """
    from sqlalchemy import create_engine, text

    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{DW_DB}"
    )

    checks = {
        "FK null prov_key":
            "SELECT COUNT(*) FROM fact_supply_resilience WHERE prov_key IS NULL",
        "FK null waktu_key":
            "SELECT COUNT(*) FROM fact_supply_resilience WHERE waktu_key IS NULL",
        "FK null komoditas_key":
            "SELECT COUNT(*) FROM fact_supply_resilience WHERE komoditas_key IS NULL",
        "Duplikat composite PK": """
            SELECT COUNT(*) FROM (
                SELECT prov_key, waktu_key, komoditas_key, COUNT(*) c
                FROM fact_supply_resilience
                GROUP BY prov_key, waktu_key, komoditas_key
                HAVING COUNT(*) > 1
            ) sub
        """,
    }

    print("=" * 50)
    print("VALIDASI POST-LOAD DWH")
    print("=" * 50)

    issues = []
    with engine.connect() as conn:
        # Row counts
        for tbl in ["dim_prov", "dim_komoditas", "dim_waktu", "fact_supply_resilience"]:
            n = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar()
            print(f"  {tbl:<35} : {n:>5} baris")

        print()
        for check_name, sql in checks.items():
            n = conn.execute(text(sql)).scalar()
            if n > 0:
                issues.append(f"{check_name}: {n} pelanggaran")
                print(f"  FAIL {check_name}: {n}")
            else:
                print(f"  OK   {check_name}")

        # Top 5 risiko tertinggi
        print("\nTop 5 supply_risk_index tertinggi:")
        rows = conn.execute(text("""
            SELECT p.nama_provinsi, w.tahun, w.bulan,
                   k.nama_komoditas,
                   ROUND(f.supply_risk_index::NUMERIC, 4) AS risk
            FROM fact_supply_resilience f
            JOIN dim_prov      p ON f.prov_key      = p.prov_key
            JOIN dim_waktu     w ON f.waktu_key     = w.waktu_key
            JOIN dim_komoditas k ON f.komoditas_key = k.komoditas_key
            ORDER BY f.supply_risk_index DESC
            LIMIT 5
        """)).fetchall()
        for r in rows:
            print(f"  {r[0]:<25} {r[1]}-{r[2]:02d} {r[3]:<5} → {r[4]}")

    if issues:
        raise ValueError(f"Validasi gagal: {issues}")
    print("\n✅ Validasi DWH lulus semua pemeriksaan.")


# ─────────────────────────────────────────────────────────────
# DEFINISI DAG
# ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="livestock_intelligence_etl",
    default_args=default_args,
    description="ETL: BPS + iSIKHNAS + PIHPS → Staging → Transform (PySpark) → DWH",
    schedule_interval="0 2 * * 1",  # Tiap Senin jam 02.00
    catchup=False,
    tags=["livestock", "etl", "pyspark"],
) as dag:

    # ── Extract (paralel) ─────────────────────────────────────
    t_extract_bps = PythonOperator(
        task_id="extract_bps",
        python_callable=task_extract_bps,
        doc_md="BPS API scraping + dummy → staging_db",
    )

    t_extract_isikhnas = PythonOperator(
        task_id="extract_isikhnas",
        python_callable=task_extract_isikhnas,
        doc_md="iSIKHNAS MySQL (6 tabel) → staging_db",
    )

    t_extract_pihps = PythonOperator(
        task_id="extract_pihps",
        python_callable=task_extract_pihps,
        doc_md="PIHPS Excel → staging_db.staging_pihps_raw",
    )

    # ── Transform (PySpark) ──────────────────────────────────
    # Jalankan spark_transform.py sebagai script mandiri via BashOperator
    # Di WSL/Linux, PySpark tidak butuh winutils — jalankan langsung.
    t_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            f'cd "{PROJECT_ROOT}" && '
            f'{PYTHON_BIN} "{SCRIPTS_DIR}/spark_transform.py"'
        ),
        doc_md=(
            "PySpark ETL: staging_db → "
            "cleaning, merge, normalisasi, agregasi, hitung supply_risk_index "
            "→ simpan ke Parquet"
        ),
    )

    # ── Load ke DWH (PySpark JDBC) ──────────────────────────
    t_load = BashOperator(
        task_id="spark_load",
        bash_command=(
            f'cd "{PROJECT_ROOT}" && '
            f'{PYTHON_BIN} "{SCRIPTS_DIR}/spark_load.py"'
        ),
        doc_md="PySpark: baca Parquet → load ke datawarehouse_db via JDBC",
    )

    # ── Validasi DWH ─────────────────────────────────────────
    t_validate = PythonOperator(
        task_id="validate_dwh",
        python_callable=task_validate_dwh,
        doc_md="Cek FK null, duplikat PK, row count di DWH",
    )

    # ── Dependency graph ─────────────────────────────────────
    #
    #  extract_bps      ─┐
    #  extract_isikhnas ─┤──→ spark_transform ──→ spark_load ──→ validate_dwh
    #  extract_pihps    ─┘
    #
    [t_extract_bps, t_extract_isikhnas, t_extract_pihps] >> t_transform >> t_load >> t_validate
