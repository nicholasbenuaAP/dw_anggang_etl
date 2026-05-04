"""
Script ETL: Extract, Transform, Load Dimensi Toko (dim_toko)
Studi Kasus: PT Enggang Ritel Nusantara
Orkestrasi: Prefect
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from prefect import task, flow

DB_HOST = "localhost"
DB_PORT = "5432"
DW_NAME = "dw_enggang"
OLTP_NAME = "oltp_enggang_khatulistiwa"
DB_USER = "postgres"
DB_PASS = ""

def get_dw_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DW_NAME, user=DB_USER, password=DB_PASS
    )

def get_oltp_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=OLTP_NAME, user=DB_USER, password=DB_PASS
    )

@task(name="Extract Source: tb_stores (OLTP)", retries=2)
def extract_source_stores() -> pd.DataFrame:
    """
    EXTRACT: Mengambil data master toko dari sistem OLTP
    """
    print("Mengekstrak data dari tb_stores (OLTP)...")
    query = "SELECT store_id, nama_toko, kota, tipe_toko FROM tb_stores;"
    
    conn_oltp = get_oltp_connection()
    df_source = pd.read_sql(query, conn_oltp)
    conn_oltp.close()
    
    print(f"Berhasil mengekstrak {len(df_source)} baris toko dari OLTP.")
    return df_source

@task(name="Extract Target: dim_toko (DWH)")
def extract_existing_dim_toko() -> pd.DataFrame:
    """
    EXTRACT: Mengambil Natural Key (store_id) yang sudah ada di DWH
    Tujuannya agar kita tidak memasukkan data yang sama dua kali (mencegah duplikat)
    """
    print("Mengekstrak data eksisting dari dim_toko (DWH)...")
    query = "SELECT store_id FROM dim_toko;"
    
    conn_dw = get_dw_connection()
    df_existing = pd.read_sql(query, conn_dw)
    conn_dw.close()
    
    return df_existing

@task(name="Transform: Filter Data Baru")
def transform_dim_toko(df_source: pd.DataFrame, df_existing: pd.DataFrame) -> pd.DataFrame:
    """
    TRANSFORM: Membandingkan source dan target. 
    Hanya meloloskan store_id yang belum ada di DWH.
    """
    print("Memulai proses transformasi...")
    
    # Menjadikan store_id sebagai Natural Key (NK)
    # Kita cari mana store_id di source yang TIDAK ADA di existing
    
    if df_existing.empty:
        # Jika DWH masih kosong, berarti semua data source adalah data baru
        df_new_records = df_source
    else:
        # Filter: ambil baris di df_source yang store_id-nya tidak ada di df_existing
        existing_store_ids = df_existing['store_id'].tolist()
        df_new_records = df_source[~df_source['store_id'].isin(existing_store_ids)]
        
    print(f"Ditemukan {len(df_new_records)} toko baru yang akan di-insert.")
    
    # Pastikan urutan kolom sesuai dengan tabel dim_toko (tanpa sk_toko)
    # sk_toko tidak kita sertakan karena PostgreSQL (tipe SERIAL) akan mengisinya otomatis (Auto Increment)
    df_final = df_new_records[['store_id', 'nama_toko', 'kota', 'tipe_toko']]
    
    return df_final

@task(name="Load ke PostgreSQL: dim_toko", retries=2)
def load_dim_toko(df: pd.DataFrame):
    """
    LOAD: Memasukkan data baru ke dalam Data Warehouse
    """
    if df.empty:
        print("Tidak ada data toko baru untuk di-load. Melewati proses Load.")
        return
        
    print(f"Melakukan load {len(df)} baris ke tabel dim_toko...")
    
    # Konversi DataFrame ke list of tuples
    data_tuples = [tuple(x) for x in df.to_numpy()]
    
    # Query Insert (Tanpa sk_toko)
    insert_query = """
        INSERT INTO dim_toko (store_id, nama_toko, kota, tipe_toko)
        VALUES %s;
    """
    
    conn_dw = get_dw_connection()
    cursor = conn_dw.cursor()
    
    try:
        # execute_values untuk bulk insert (jauh lebih cepat)
        execute_values(cursor, insert_query, data_tuples)
        conn_dw.commit()
        print("Load data berhasil!")
    except Exception as e:
        print(f"Terjadi error saat Load: {e}")
        conn_dw.rollback()
    finally:
        cursor.close()
        conn_dw.close()

@flow(name="Flow ETL: Dimensi Toko Enggang Ritel")
def flow_etl_dim_toko():
    """
    Fungsi Utama (Flow) yang mengorkestrasi Task ETL Dimensi Toko
    """
    # 1. Extract
    df_oltp = extract_source_stores()
    df_dwh = extract_existing_dim_toko()
    
    # 2. Transform
    df_ready_to_load = transform_dim_toko(df_oltp, df_dwh)
    
    # 3. Load
    load_dim_toko(df_ready_to_load)

if __name__ == "__main__":
    flow_etl_dim_toko()