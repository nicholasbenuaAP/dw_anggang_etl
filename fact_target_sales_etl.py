import pandas as pd
import numpy as np
from prefect import task, flow
import psycopg2
from psycopg2 import extras
import os

# =====================================================================
# KONFIGURASI KONEKSI DATABASE & FILE
# =====================================================================
DW_DB_CONFIG = {
    "host": "localhost",
    "database": "dw_enggang",
    "user": "postgres",
    "password": "",
    "port": "5432"
}

# Path ke file CSV sumber
CSV_FILE_PATH = "Target_Sales_2024.csv"

@task(name="1. Extract (CSV Target Sales)", retries=2, retry_delay_seconds=5)
def extract_target_sales_csv() -> pd.DataFrame:
    """Membaca data target pendapatan dari file CSV."""
    print(f"Membaca data dari file: {CSV_FILE_PATH}...")
    
    try:
        df = pd.read_csv(CSV_FILE_PATH)
        print(f"Berhasil menarik {len(df)} baris data dari CSV.")
        return df
    except FileNotFoundError:
        print("File CSV tidak ditemukan. Membuat data dummy untuk keperluan kelas...")
        return pd.DataFrame([
            {"Tahun": 2024, "Bulan": 1, "ID_Toko": "STR-PTK-01", "Target_Pendapatan_Rp": 150000000},
            {"Tahun": 2024, "Bulan": 1, "ID_Toko": "STR-PTK-02", "Target_Pendapatan_Rp": 120000000},
            {"Tahun": 2024, "Bulan": 1, "ID_Toko": "STR-STG-01", "Target_Pendapatan_Rp": 85000000}
        ])

@task(name="2. Extract (Lookup Dimensi Toko DW)")
def extract_dim_toko_lookup() -> pd.DataFrame:
    """Mengambil Natural Key dan Surrogate Key dari dimensi toko."""
    print("Mengambil data dimensi toko dari Data Warehouse untuk proses Lookup...")
    
    try:
        conn = psycopg2.connect(**DW_DB_CONFIG)
        # Kita hanya perlu sk_toko dan store_id untuk mapping
        df_toko = pd.read_sql("SELECT sk_toko, store_id FROM dim_toko", conn)
        conn.close()
        return df_toko
    except Exception as e:
        print("Gagal terhubung ke DW. Membuat dummy lookup dimensi...")
        return pd.DataFrame({
            "sk_toko": [1, 2, 3], 
            "store_id": ["STR-PTK-01", "STR-PTK-02", "STR-STG-01"]
        })

@task(name="3. Transform (Resolusi Granularitas & Lookup)")
def transform_fact_target_sales(df_csv: pd.DataFrame, df_toko: pd.DataFrame) -> pd.DataFrame:
    """Menangani perbedaan granularitas waktu dan melakukan pencarian SK Toko."""
    print("Memulai transformasi Data Fakta Target Sales...")
    
    # ATURAN 1: Resolusi Granularitas Waktu
    # Menggabungkan Tahun (2024) dan Bulan (1) menjadi format tanggal YYYY-MM-DD
    # Kita menggunakan "01" sebagai representasi hari pertama di bulan target tersebut
    df_csv['tanggal_str'] = df_csv['Tahun'].astype(str) + '-' + df_csv['Bulan'].astype(str).str.zfill(2) + '-01'
    
    # ATURAN 2: Konversi ke sk_waktu (Integer YYYYMMDD)
    df_csv['tanggal_dt'] = pd.to_datetime(df_csv['tanggal_str'])
    df_csv['sk_waktu'] = df_csv['tanggal_dt'].dt.strftime('%Y%m%d').astype(int)
    
    # ATURAN 3: Lookup (Merge) SK Toko
    # Pastikan format teks (NK) sesuai (huruf kapital) sebelum di-merge
    df_csv['ID_Toko'] = df_csv['ID_Toko'].astype(str).str.upper()
    df_toko['store_id'] = df_toko['store_id'].astype(str).str.upper()
    
    # Lakukan left join (seperti VLOOKUP di Excel)
    df_merged = df_csv.merge(df_toko, left_on='ID_Toko', right_on='store_id', how='left')
    
    # ATURAN 4: Casting target_pendapatan_rp ke format numerik (Float/Decimal)
    df_merged['target_pendapatan_rp'] = df_merged['Target_Pendapatan_Rp'].astype(float)
    
    # Merapikan struktur kolom sesuai dengan schema DDL fact_target_sales
    kolom_urut = ['sk_waktu', 'sk_toko', 'target_pendapatan_rp']
    df_fakta = df_merged[kolom_urut]
    
    # PERBAIKAN: Ubah tipe data seluruh dataframe menjadi tipe 'object' (native Python),
    # baru kemudian kita ganti NaN menjadi None. Ini mencegah error 'schema np does not exist' 
    # karena psycopg2 kini akan menerima pure None, bukan numpy.nan atau numpy.float64
    df_fakta = df_fakta.astype(object).where(pd.notnull(df_fakta), None)

    print("Transformasi selesai. Pratinjau hasil fact_target_sales:")
    print(df_fakta.head())
    
    return df_fakta

@task(name="4. Load (Data Warehouse fact_target_sales)")
def load_fact_target_sales(df: pd.DataFrame):
    """Memuat data akhir ke tabel fact_target_sales."""
    print("Membuka koneksi ke Data Warehouse (PostgreSQL)...")
    
    try:
        conn = psycopg2.connect(**DW_DB_CONFIG)
        cur = conn.cursor()
        
        # Menyiapkan data tuple untuk bulk insert
        data_tuples = [tuple(x) for x in df.to_numpy()]
        kolom = ','.join(list(df.columns))
        
        # Bulk Insert Data Fakta
        query = f"INSERT INTO fact_target_sales({kolom}) VALUES %s"
        extras.execute_values(cur, query, data_tuples)
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"Berhasil memuat {len(df)} baris ke tabel 'fact_target_sales' di DW.")
    except Exception as e:
        print(f"Gagal memuat data fakta target ke DW: {e}")

@flow(name="ETL Flow: Fact Target Sales", description="Pipeline target penjualan dari file CSV")
def etl_fact_target_sales_flow():
    """Flow utama untuk memproses target pendapatan per bulan."""
    
    # 1. Tarik data target dari file CSV
    df_csv = extract_target_sales_csv()
    
    # 2. Tarik peta dimensi toko dari DW
    df_toko = extract_dim_toko_lookup()
    
    # 3. Transformasi dan konversi tingkat granularitas
    df_fakta = transform_fact_target_sales(df_csv, df_toko)
    
    # 4. Simpan ke Data Warehouse
    load_fact_target_sales(df_fakta)


if __name__ == "__main__":
    etl_fact_target_sales_flow()