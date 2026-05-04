import pandas as pd
import requests
from prefect import task, flow
import psycopg2
from psycopg2 import extras

# =====================================================================
# KONFIGURASI KONEKSI DATABASE POSTGRESQL (Hanya untuk Load ke DW)
# =====================================================================
DW_DB_CONFIG = {
    "host": "localhost",
    "database": "dw_enggang",
    "user": "postgres",
    "password": "",
    "port": "5432"
}

API_URL = "https://api-enggang.vercel.app/api/deliveries"

@task(name="1. Extract (API JSON)", retries=2, retry_delay_seconds=5)
def extract_api_deliveries() -> pd.DataFrame:
    """Mengambil data dari API pengiriman berbasis JSON."""
    print(f"Menarik data dari API: {API_URL} ...")
    
    try:
        # Mengirim HTTP GET request ke API
        response = requests.get(API_URL)
        response.raise_for_status() # Memastikan tidak ada error HTTP (misal 404, 500)
        
        # Parse response sebagai JSON
        json_data = response.json()
        
        # Mengekstrak list of dictionary yang ada di dalam key 'data'
        records = json_data.get('data', [])
        
        # Memasukkan records ke dalam DataFrame
        df = pd.DataFrame(records)
        print(f"Berhasil menarik {len(df)} baris data mentah dari API.")
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Gagal menarik data dari API: {e}")
        # Fallback dummy jika API sedang bermasalah saat demo kelas
        print("Membuat data dummy API...")
        return pd.DataFrame([
            {"transaction_id": "TRX-10001", "kurir": "Khatulistiwa Cargo", "status_pengiriman": "DELIVERED"},
            {"transaction_id": "TRX-10002", "kurir": "J&T", "status_pengiriman": "DELIVERED"},
            {"transaction_id": "TRX-10003", "kurir": "Khatulistiwa Cargo", "status_pengiriman": "ON PROCESS"}
        ])


@task(name="2. Transform (Deduplikasi In-Memory)")
def transform_dim_kurir(df: pd.DataFrame) -> pd.DataFrame:
    """Mengambil daftar nama kurir yang unik dan men-generate sk_kurir."""
    print("Memulai deduplikasi data kurir...")
    
    # ATURAN 1: Deduplikasi (DISTINCT kurir)
    # Kita hanya mengambil kolom 'kurir', membuang duplikat, dan mengabaikan nilai kosong
    unique_kurir = df['kurir'].drop_duplicates().dropna()
    
    # Membuat DataFrame baru khusus untuk dimensi kurir
    df_dim = pd.DataFrame({'nama_kurir': unique_kurir})
    
    # Opsional: Standardisasi nama kurir agar rapi (Title Case)
    df_dim['nama_kurir'] = df_dim['nama_kurir'].str.title()
    
    # ATURAN 2: Generate sk_kurir (integer)
    # Reset index dulu agar urutannya rapi setelah proses drop_duplicates
    df_dim = df_dim.reset_index(drop=True)
    df_dim['sk_kurir'] = range(1, len(df_dim) + 1)
    
    # Merapikan urutan kolom sesuai DDL (sk_kurir, nama_kurir)
    kolom_urut = ['sk_kurir', 'nama_kurir']
    df_dim = df_dim[kolom_urut]
    
    print("Transformasi selesai. Daftar kurir unik:")
    print(df_dim)
    
    return df_dim


@task(name="3. Load (Data Warehouse dim_kurir)")
def load_dim_kurir(df: pd.DataFrame):
    """Menyimpan data kurir unik ke tabel dim_kurir di DW PostgreSQL."""
    print("Membuka koneksi ke Data Warehouse (PostgreSQL)...")
    
    try:
        conn = psycopg2.connect(**DW_DB_CONFIG)
        cur = conn.cursor()
        
        # Menyiapkan data tuple untuk execute_values
        data_tuples = [tuple(x) for x in df.to_numpy()]
        kolom = ','.join(list(df.columns))
        
        # Bulk Insert
        query = f"INSERT INTO dim_kurir({kolom}) VALUES %s"
        extras.execute_values(cur, query, data_tuples)
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"Berhasil memuat {len(df)} data kurir unik ke tabel 'dim_kurir'.")
    except Exception as e:
        print(f"Gagal memuat data ke DW: {e}")


@flow(name="ETL Flow: Dimensi Kurir", description="Pipeline menarik data kurir unik dari API Pengiriman")
def etl_dim_kurir_flow():
    """Flow utama untuk merangkai Extract (API), Transform, dan Load."""
    
    df_mentah = extract_api_deliveries()
    df_bersih = transform_dim_kurir(df_mentah)
    load_dim_kurir(df_bersih)


if __name__ == "__main__":
    etl_dim_kurir_flow()