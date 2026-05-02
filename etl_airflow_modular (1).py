from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine

# ==============================
# KONFIGURASI DATABASE
# ==============================
# Menggunakan kredensial dari file dw_etl_final.py
SRC_A_URL = "mysql+pymysql://root:mydatabase@localhost:3306/source_a"
SRC_B_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/source_b"
DWH_URL   = "mysql+pymysql://root:admin123@localhost:3306/dwh"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)    
}

# ==============================
# FUNGSI ETL PER TASK
# ==============================

def extract_source_a(**kwargs):
    """Task 1: Ekstraksi data dari MySQL Source A"""
    engine = create_engine(SRC_A_URL)
    tables = ["dim_product", "dim_store", "dim_date", "fact_sales"]
    data = {t: pd.read_sql(f"SELECT * FROM {t}", engine) for t in tables}
    
    # Simpan ke XCom agar bisa dibaca task selanjutnya (sebagai dictionary)
    # Catatan: Untuk data besar, sebaiknya simpan ke CSV sementara/S3/GCS
    return {t: df.to_json() for t, df in data.items()}

def extract_source_b(**kwargs):
    """Task 2: Ekstraksi data dari PostgreSQL Source B"""
    engine = create_engine(SRC_B_URL)
    tables = ["dim_product", "dim_store", "dim_date", "fact_sales"]
    data = {t: pd.read_sql(f"SELECT * FROM {t}", engine) for t in tables}
    return {t: df.to_json() for t, df in data.items()}

def transform_data(**kwargs):
    """Task 3: Transformasi, Union, dan Surrogate Key"""
    ti = kwargs['ti']
    # Ambil data dari task sebelumnya via XCom
    json_a = ti.xcom_pull(task_ids='extract_mysql_a')
    json_b = ti.xcom_pull(task_ids='extract_postgres_b')
    
    dfs_a = {t: pd.read_json(js) for t, js in json_a.items()}
    dfs_b = {t: pd.read_json(js) for t, js in json_b.items()}

    # Tambahkan Source System
    for t in dfs_a: dfs_a[t]['source_system'] = 'A'
    for t in dfs_b: dfs_b[t]['source_system'] = 'B'

    # Union Data
    df_product = pd.concat([dfs_a["dim_product"], dfs_b["dim_product"]], ignore_index=True)
    df_store   = pd.concat([dfs_a["dim_store"], dfs_b["dim_store"]], ignore_index=True)
    df_sales   = pd.concat([dfs_a["fact_sales"], dfs_b["fact_sales"]], ignore_index=True)
    df_date    = dfs_a["dim_date"]

    # Standardisasi Kategori
    cat_map = {'Makanan': 'Food', 'Food': 'Food', 'Minuman': 'Beverage', 'Drink': 'Beverage'}
    df_product['category'] = df_product['category'].map(cat_map).fillna(df_product['category'])

    # Drop Duplicates
    df_product = df_product.drop_duplicates(['product_id', 'source_system'])
    df_store   = df_store.drop_duplicates(['store_id', 'source_system'])

    # Surrogate Key (Simulasi row_number)
    df_product['product_key'] = range(1, len(df_product) + 1)
    df_store['store_key'] = range(1, len(df_store) + 1)

    # Fact Enrichment (Mapping ID ke Key)
    df_fact = df_sales.merge(df_product[['product_id', 'source_system', 'product_key']], on=['product_id', 'source_system'], how='left')
    df_fact = df_fact.merge(df_store[['store_id', 'source_system', 'store_key']], on=['store_id', 'source_system'], how='left')
    
    df_fact_final = df_fact[['product_key', 'store_key', 'date_id', 'quantity', 'revenue', 'source_system']]
    df_fact_final = df_fact_final.rename(columns={'date_id': 'date_key'})

    return {
        "dim_product": df_product.to_json(),
        "dim_store": df_store.to_json(),
        "dim_date": df_date.to_json(),
        "fact_sales": df_fact_final.to_json()
    }

def load_to_dwh(**kwargs):
    """Task 4: Load hasil akhir ke MySQL DWH"""
    ti = kwargs['ti']
    final_data_json = ti.xcom_pull(task_ids='transform_logic')
    engine_dwh = create_engine(DWH_URL)
    
    for table_name, json_str in final_data_json.items():
        df = pd.read_json(json_str)
        # Load ke database (Mode overwrite/replace)
        df.to_sql(table_name, engine_dwh, if_exists='replace', index=False)
    
    print("Load ke DWH Selesai!")

# ==============================
# DEFINISI DAG & WORKFLOW
# ==============================
with DAG(
    'etl_modular_no_spark',
    default_args=default_args,
    schedule='@hourly',
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='extract_mysql_a', python_callable=extract_source_a)
    t2 = PythonOperator(task_id='extract_postgres_b', python_callable=extract_source_b)
    t3 = PythonOperator(task_id='transform_logic', python_callable=transform_data)
    t4 = PythonOperator(task_id='load_to_dwh', python_callable=load_to_dwh)

    # Visualisasi DAG: T1 & T2 jalan paralel, lalu T3, lalu T4
    [t1, t2] >> t3 >> t4