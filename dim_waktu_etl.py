"""
Script ETL: Generate dan Load Dimensi Waktu (dim_waktu)
Studi Kasus: PT Enggang Ritel Nusantara
Orkestrasi: Prefect
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from prefect import task, flow
from datetime import datetime


DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "dw_enggang"
DB_USER = "postgres"
DB_PASS = ""

@task(name="Extract/Generate Date Range", retries=2)
def generate_date_range(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fungsi untuk membuat rentang tanggal (Extract/Generate)
    """
    print(f"Meng-generate tanggal dari {start_date} sampai {end_date}...")
    # Membuat date range harian menggunakan Pandas
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Masukkan ke dalam DataFrame
    df = pd.DataFrame({'tanggal': dates})
    return df

@task(name="Transform Dimensi Waktu")
def transform_dim_waktu(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fungsi untuk mengekstrak atribut waktu (Transform)
    """
    print("Memulai proses transformasi atribut waktu...")
    
    # 1. Generate sk_waktu (Format YYYYMMDD bertipe Integer)
    df['sk_waktu'] = df['tanggal'].dt.strftime('%Y%m%d').astype(int)
    
    # 2. Ekstrak atribut tanggal dasar
    df['bulan'] = df['tanggal'].dt.month
    df['kuartal'] = df['tanggal'].dt.quarter
    df['tahun'] = df['tanggal'].dt.year
    
    # 3. Mapping nama bulan dalam Bahasa Indonesia
    bulan_map = {
        1: 'Januari', 2: 'Februari', 3: 'Maret', 4: 'April',
        5: 'Mei', 6: 'Juni', 7: 'Juli', 8: 'Agustus',
        9: 'September', 10: 'Oktober', 11: 'November', 12: 'Desember'
    }
    df['nama_bulan'] = df['bulan'].map(bulan_map)
    
    # Ubah format 'tanggal' ke date (string YYYY-MM-DD) agar mudah masuk ke PostgreSQL
    df['tanggal'] = df['tanggal'].dt.strftime('%Y-%m-%d')
    
    # Urutkan kolom sesuai DDL database
    df = df[['sk_waktu', 'tanggal', 'bulan', 'nama_bulan', 'kuartal', 'tahun']]
    return df

@task(name="Load ke PostgreSQL", retries=3, retry_delay_seconds=5)
def load_to_postgres(df: pd.DataFrame):
    """
    Fungsi untuk memasukkan data ke tabel target di DWH (Load)
    """
    print(f"Melakukan load {len(df)} baris ke tabel dim_waktu...")
    
    # Mengubah DataFrame menjadi list of tuples untuk execute_values
    data_tuples = [tuple(x) for x in df.to_numpy()]
    
    # Query Insert dengan penanganan konflik (Idempotent)
    # Jika sk_waktu sudah ada, lewati (DO NOTHING)
    insert_query = """
        INSERT INTO dim_waktu (sk_waktu, tanggal, bulan, nama_bulan, kuartal, tahun)
        VALUES %s
        ON CONFLICT (sk_waktu) DO NOTHING;
    """
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cursor = conn.cursor()
        
        # eksekusi bulk insert yang jauh lebih cepat daripada insert satu per satu
        execute_values(cursor, insert_query, data_tuples)
        conn.commit()
        
        print("Load data berhasil!")
        
    except Exception as e:
        print(f"Terjadi error saat Load: {e}")
        conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

@flow(name="Flow ETL: Dimensi Waktu Enggang Ritel")
def flow_etl_dim_waktu():
    """
    Fungsi Utama (Flow) yang mengorkestrasi seluruh Task
    """
    start_date = "2024-01-01"
    end_date = "2024-12-31"
    
    # 1. Extract
    df_raw = generate_date_range(start_date, end_date)
    
    # 2. Transform
    df_transformed = transform_dim_waktu(df_raw)
    
    # 3. Load
    load_to_postgres(df_transformed)

if __name__ == "__main__":
    # Menjalankan flow secara lokal
    flow_etl_dim_waktu()