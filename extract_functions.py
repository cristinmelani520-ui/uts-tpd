"""
╔══════════════════════════════════════════════════════════════╗
║  FILE: airflow_scripts/extract_functions.py                 ║
║  Dipanggil oleh DAG via PythonOperator                      ║
║                                                              ║
║  Isi notebook Extract (Fase 1) dalam bentuk fungsi:         ║
║  • run_extract_bps()      — BPS API + dummy                 ║
║  • run_extract_isikhnas() — MySQL iSIKHNAS → staging        ║
║  • run_extract_pihps()    — Excel PIHPS → staging           ║
╚══════════════════════════════════════════════════════════════╝
"""

import time
import random
import os

import requests
import pandas as pd
from sqlalchemy import create_engine, text

random.seed(42)

# ─────────────────────────────────────────────────────────────
# KONFIGURASI BPS
# ─────────────────────────────────────────────────────────────

API_KEY = "a2e282c97496f56f82b1755004805a8a"
YEARS   = [2020, 2021, 2022, 2023, 2024, 2025]

COMMODITIES_TO_SCRAPE = [
    {
        "name"     : "Jumlah Penduduk",
        "table_ids": ["WVRlTTcySlZDa3lUcFp6czNwbHl4QT09"],
        "keywords" : ["jumlah", "penduduk"],
        "col"      : "jumlah_penduduk",
    },
    {
        "name"     : "Populasi Sapi Potong",
        "table_ids": ["S2ViU1dwVTlpSXRwU1MvendHN05Cdz09"],
        "keywords" : ["populasi", "sapi", "potong"],
        "col"      : "populasi_sapi",
    },
    {
        "name"     : "Populasi Ayam Pedaging",
        "table_ids": ["ckJyVXRMT05MWTNpaW9mdnVseFk0Zz09"],
        "keywords" : ["populasi", "ayam", "pedaging"],
        "col"      : "populasi_ayam",
    },
    {
        "name"     : "Produksi Daging Ayam Pedaging",
        "table_ids": ["dWhmNFl6WXYyR093R2NjTGM3NG9idz09"],
        "keywords" : ["produksi", "daging", "ayam", "pedaging"],
        "col"      : "produksi_daging_ayam",
    },
]

PROVINSI_LIST = [
    "Aceh","Sumatera Utara","Sumatera Barat","Riau","Kepulauan Riau","Jambi",
    "Sumatera Selatan","Bangka Belitung","Bengkulu","Lampung","DKI Jakarta",
    "Jawa Barat","Jawa Tengah","DI Yogyakarta","Jawa Timur","Banten","Bali",
    "Nusa Tenggara Barat","Nusa Tenggara Timur","Kalimantan Barat",
    "Kalimantan Tengah","Kalimantan Selatan","Kalimantan Timur",
    "Kalimantan Utara","Sulawesi Utara","Sulawesi Tengah","Sulawesi Selatan",
    "Sulawesi Tenggara","Gorontalo","Sulawesi Barat","Maluku","Maluku Utara",
    "Papua","Papua Barat","Papua Selatan","Papua Tengah",
    "Papua Pegunungan","Papua Barat Daya",
]

ISIKHNAS_TABLES = [
    "ref_hewan", "ref_wilayah", "tr_mutasi",
    "tr_laporan_sakit", "tr_hasil_lab", "tr_rph",
]


# ─────────────────────────────────────────────────────────────
# HELPER: BPS API
# ─────────────────────────────────────────────────────────────

def _get(year: int, table_id: str, kode_wilayah: str = "0000000"):
    url = (
        f"https://webapi.bps.go.id/v1/api/interoperabilitas/datasource/simdasi/"
        f"id/25/tahun/{year}/id_tabel/{table_id}/wilayah/{kode_wilayah}/key/{API_KEY}"
    )
    try:
        r = requests.get(url, timeout=30)
        return r.json()
    except Exception:
        return None


def _find_var_id(main_data: dict, keywords: list):
    for var_id, info in main_data.get("kolom", {}).items():
        nama = str(info.get("nama_variabel", "")).lower()
        if all(kw in nama for kw in keywords):
            return var_id
    return None


def scrape_commodity_all_provinces(commodity: dict, year: int) -> pd.DataFrame:
    rows = []
    for tbl_id in commodity["table_ids"]:
        raw = _get(year, tbl_id)
        if not raw or raw.get("status") != "OK":
            continue
        main_data = raw.get("data", [{}, {}])[1]
        var_id    = _find_var_id(main_data, commodity["keywords"])
        if not var_id:
            continue
        for item in main_data.get("data", []):
            nama_prov = item.get("label", "").strip()
            kode      = item.get("kode_wilayah", "")
            if not kode or nama_prov.lower() in ("indonesia", ""):
                continue
            val = None
            vars_data = item.get("variables", {})
            if var_id in vars_data:
                val = vars_data[var_id].get("value_raw")
            rows.append({"provinsi": nama_prov, "tahun": year, commodity["col"]: val})
        break
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────
# EXTRACT 1: BPS API + DUMMY
# ─────────────────────────────────────────────────────────────

def run_extract_bps(pg_url: str) -> dict:
    """
    1. Scraping 4 variabel dari BPS SIMDASI API
    2. Generate dummy BPS (variabel yang tidak tersedia di API)
    3. Push ke staging_db: staging_bps_api_raw, staging_bps_dummy_raw
    """
    print("=" * 55)
    print("EXTRACT BPS — API + DUMMY")
    print("=" * 55)

    # ── 1A. Scraping API ──────────────────────────────────────
    print("\n[1/2] Scraping BPS API...")
    commodity_dfs = {}

    for comm in COMMODITIES_TO_SCRAPE:
        print(f"  • {comm['name']}")
        yearly_dfs = []
        for year in YEARS:
            df_year = scrape_commodity_all_provinces(comm, year)
            if not df_year.empty:
                yearly_dfs.append(df_year)
            time.sleep(0.5)

        if yearly_dfs:
            commodity_dfs[comm["col"]] = pd.concat(yearly_dfs, ignore_index=True)
            print(f"    ✅ {len(commodity_dfs[comm['col']])} baris")
        else:
            print(f"    ❌ Gagal, skip.")

    if commodity_dfs:
        keys     = list(commodity_dfs.keys())
        df_scraped = commodity_dfs[keys[0]]
        for col in keys[1:]:
            df_scraped = df_scraped.merge(commodity_dfs[col], on=["provinsi", "tahun"], how="outer")
        df_scraped["provinsi"] = df_scraped["provinsi"].str.strip()
    else:
        df_scraped = pd.DataFrame(columns=["provinsi", "tahun"])
        print("  ⚠ Tidak ada data BPS API. Lanjut dengan empty DataFrame.")

    # ── 1B. Generate Dummy BPS ───────────────────────────────
    print("\n[2/2] Generate dummy BPS...")

    def generate_bps_dummy():
        data = []
        for prov in PROVINSI_LIST:
            base_pop = random.randint(500_000, 50_000_000)
            for tahun in YEARS:
                growth        = round(random.uniform(-0.02, 0.04), 4)
                jumlah_penduduk = int(base_pop * (1 + growth))
                populasi_sapi  = random.randint(10_000, 500_000)
                populasi_ayam  = random.randint(50_000, 5_000_000)
                potong_sapi    = int(populasi_sapi * random.uniform(0.4, 0.7))
                potong_ayam    = int(populasi_ayam * random.uniform(0.5, 0.8))
                produksi_sapi  = round(potong_sapi * random.uniform(0.2, 0.3), 2)
                produksi_ayam  = round(potong_ayam * random.uniform(0.1, 0.2), 2)
                konsumsi_sapi  = round(random.uniform(1.5, 3.5), 2)
                konsumsi_ayam  = round(random.uniform(8, 15), 2)
                permintaan_sapi = round(jumlah_penduduk * konsumsi_sapi / 1000, 2)
                permintaan_ayam = round(jumlah_penduduk * konsumsi_ayam / 1000, 2)
                harga_sapi     = random.randint(90_000, 130_000)
                harga_ayam     = random.randint(20_000, 40_000)

                row = {
                    "provinsi": prov, "tahun": tahun,
                    "jumlah_penduduk_dummy":        jumlah_penduduk,
                    "populasi_sapi_dummy":           populasi_sapi,
                    "populasi_ayam_dummy":           populasi_ayam,
                    "produksi_daging_sapi_dummy":    produksi_sapi,
                    "produksi_daging_ayam_dummy":    produksi_ayam,
                    "konsumsi_daging_sapi_dummy":    konsumsi_sapi,
                    "konsumsi_daging_ayam_dummy":    konsumsi_ayam,
                    "permintaan_daging_sapi_dummy":  permintaan_sapi,
                    "permintaan_daging_ayam_dummy":  permintaan_ayam,
                    "jumlah_ternak_sapi_potong_dummy": potong_sapi,
                    "jumlah_ternak_ayam_potong_dummy": potong_ayam,
                    "harga_baseline_sapi_dummy":     harga_sapi,
                    "harga_baseline_ayam_dummy":     harga_ayam,
                    "growth_populasi_dummy":         growth,
                }
                # 5% chance missing value
                if random.random() < 0.05:
                    row["produksi_daging_sapi_dummy"] = None
                if random.random() < 0.05:
                    row["permintaan_daging_ayam_dummy"] = None
                data.append(row)
        return pd.DataFrame(data)

    df_dummy = generate_bps_dummy()
    print(f"  ✅ {len(df_dummy)} baris dummy dihasilkan")

    # ── Push ke staging ──────────────────────────────────────
    print("\nPush ke staging_db...")
    engine = create_engine(pg_url)
    if not df_scraped.empty:
        df_scraped.to_sql("staging_bps_api_raw", engine, if_exists="replace", index=False)
        print(f"  ✅ staging_bps_api_raw   : {len(df_scraped)} baris")
    df_dummy.to_sql("staging_bps_dummy_raw", engine, if_exists="replace", index=False)
    print(f"  ✅ staging_bps_dummy_raw : {len(df_dummy)} baris")

    return {
        "api_rows":   len(df_scraped),
        "dummy_rows": len(df_dummy),
    }


# ─────────────────────────────────────────────────────────────
# EXTRACT 2: iSIKHNAS (MySQL → PostgreSQL staging)
# ─────────────────────────────────────────────────────────────

def run_extract_isikhnas(mysql_url: str, pg_url: str) -> dict:
    """
    Baca 6 tabel iSIKHNAS dari MySQL → push ke staging_db
    dengan prefix staging_isikhnas_
    """
    print("=" * 55)
    print("EXTRACT iSIKHNAS — MySQL → staging_db")
    print("=" * 55)

    src_engine     = create_engine(mysql_url)
    staging_engine = create_engine(pg_url)
    results        = {}

    for table in ISIKHNAS_TABLES:
        try:
            df = pd.read_sql(f"SELECT * FROM {table}", src_engine)
            staging_name = f"staging_isikhnas_{table}"
            df.to_sql(staging_name, staging_engine, if_exists="replace", index=False)
            results[table] = len(df)
            print(f"  ✅ {table:<22} → {staging_name}  ({len(df)} baris)")
        except Exception as e:
            results[table] = 0
            print(f"  ❌ {table}: {e}")

    total = sum(results.values())
    print(f"\nTotal: {total} baris dari {len(ISIKHNAS_TABLES)} tabel")
    return results


# ─────────────────────────────────────────────────────────────
# EXTRACT 3: PIHPS (Excel → PostgreSQL staging)
# ─────────────────────────────────────────────────────────────

def run_extract_pihps(excel_path: str, pg_url: str) -> dict:
    """
    Baca file Excel PIHPS → push ke staging_db.staging_pihps_raw
    """
    print("=" * 55)
    print("EXTRACT PIHPS — Excel → staging_db")
    print("=" * 55)

    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"File PIHPS tidak ditemukan: {excel_path}")

    print(f"Membaca file: {excel_path}")
    df = pd.read_excel(excel_path)
    print(f"  Terbaca: {len(df)} baris × {len(df.columns)} kolom")

    engine = create_engine(pg_url)
    df.to_sql("staging_pihps_raw", engine, if_exists="replace", index=False, chunksize=1000)
    print(f"  ✅ staging_pihps_raw: {len(df)} baris")

    return {"rows": len(df)}
