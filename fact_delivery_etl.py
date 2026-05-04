import pandas as pd
import requests
from prefect import task, flow
import psycopg2
from psycopg2 import extras

# =====================================================================
# KONFIGURASI KONEKSI DATABASE & API
# =====================================================================
OLTP_DB_CONFIG = {
    "host": "localhost",
    "database": "oltp_enggang_khatulistiwa",
    "user": "postgres",
    "password": "",
    "port": "5432"
}

DW_DB_CONFIG = {
    "host": "localhost",
    "database": "dw_enggang",
    "user": "postgres",
    "password": "",
    "port": "5432"
}

# Memastikan URL API menggunakan endpoint yang benar dan mutakhir
API_URL = "https://api-enggang.vercel.app/api/deliveries"


@task(name="1. Extract (API JSON Delivery)", retries=2, retry_delay_seconds=5)
def extract_api_delivery() -> pd.DataFrame:
    """Mengambil data logistik pengiriman dari API."""
    print(f"Menarik data dari API: {API_URL} ...")
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        records = response.json().get('data', [])
        df = pd.DataFrame(records)
        print(f"Berhasil menarik {len(df)} baris data dari API.")
        return df
    except requests.exceptions.RequestException as e:
        print(f"Gagal memanggil API: {e}. Menggunakan data dummy...")
        return pd.DataFrame([
            {"transaction_id": "TRX-10001", "kurir": "Khatulistiwa Cargo", "status_pengiriman": "DELIVERED", "tgl_sampai": "2024-10-11T14:30:00Z", "biaya_ongkir": 25000, "durasi_hari": 1},
            {"transaction_id": "TRX-10002", "kurir": "J&T", "status_pengiriman": "DELIVERED", "tgl_sampai": "2024-10-12T14:30:00Z", "biaya_ongkir": 40000, "durasi_hari": 3},
            {"transaction_id": "TRX-10003", "kurir": "JNE", "status_pengiriman": "ON PROCESS", "tgl_sampai": None, "biaya_ongkir": 35000, "durasi_hari": None}
        ])


@task(name="2. Extract (OLTP Sales Headers)")
def extract_oltp_store_lookup() -> pd.DataFrame:
    """Mencari tau setiap transaksi itu berasal dari toko mana melalui OLTP."""
    print("Menarik data referensi store_id dari OLTP tb_sales_headers...")
    try:
        conn = psycopg2.connect(**OLTP_DB_CONFIG)
        query = "SELECT transaction_id, store_id FROM tb_sales_headers"
        
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        df_oltp = pd.DataFrame(rows, columns=['transaction_id', 'store_id'])
        
        cur.close()
        conn.close()
        return df_oltp
    except Exception as e:
        print("Gagal akses OLTP. Menggunakan dummy lookup...")
        return pd.DataFrame({
            "transaction_id": ["TRX-10001", "TRX-10002", "TRX-10003"],
            "store_id": ["STR-PTK-01", "STR-STG-01", "STR-PTK-02"]
        })


@task(name="3. Extract (Lookup Dimensi DW)")
def extract_dw_lookups() -> dict:
    """Mengambil SK Toko dan SK Kurir dari Data Warehouse."""
    print("Mengambil dimensi Toko dan Kurir dari DW...")
    dim_dict = {}
    try:
        conn = psycopg2.connect(**DW_DB_CONFIG)
        dim_dict['toko'] = pd.read_sql("SELECT sk_toko, store_id FROM dim_toko", conn)
        dim_dict['kurir'] = pd.read_sql("SELECT sk_kurir, nama_kurir FROM dim_kurir", conn)
        conn.close()
    except Exception as e:
        print("Gagal akses DW. Menggunakan dummy...")
        dim_dict['toko'] = pd.DataFrame({"sk_toko": [1, 2, 3], "store_id": ["STR-PTK-01", "STR-PTK-02", "STR-STG-01"]})
        dim_dict['kurir'] = pd.DataFrame({"sk_kurir": [1, 2, 3], "nama_kurir": ["Khatulistiwa Cargo", "J&T", "JNE"]})
    return dim_dict


@task(name="4. Transform (Complex Cross-DB Lookup & Formatting)")
def transform_fact_delivery(df_api: pd.DataFrame, df_oltp: pd.DataFrame, dim_lookups: dict) -> pd.DataFrame:
    """Melakukan proses pencarian lintas sistem dan standardisasi data."""
    print("Memulai transformasi Data Fakta Pengiriman...")
    
    df = df_api.copy()
    
    # --- TRICKY PART: Resolusi Toko dari OLTP ---
    # Merge dengan data OLTP untuk mendapatkan store_id
    df = df.merge(df_oltp, on='transaction_id', how='left')
    
    # Standardisasi format agar matching saat dilookup ke DW
    df['store_id'] = df['store_id'].astype(str).str.upper()
    df['kurir'] = df['kurir'].astype(str).str.title()
    dim_lookups['toko']['store_id'] = dim_lookups['toko']['store_id'].astype(str).str.upper()
    dim_lookups['kurir']['nama_kurir'] = dim_lookups['kurir']['nama_kurir'].astype(str).str.title()
    
    # --- LOOKUP KE DATA WAREHOUSE ---
    # Dapatkan sk_toko
    df = df.merge(dim_lookups['toko'], on='store_id', how='left')
    
    # Dapatkan sk_kurir
    df = df.merge(dim_lookups['kurir'], left_on='kurir', right_on='nama_kurir', how='left')
    
    # --- STANDARDISASI WAKTU ---
    # Konversi ISO 8601 string ke Datetime object. Jika null (misal masih 'ON PROCESS'), biarkan NaT
    df['tgl_sampai_dt'] = pd.to_datetime(df['tgl_sampai'], errors='coerce')
    # Ubah menjadi format YYYYMMDD (integer), kita gunakan float sementara agar bisa menyimpan NaN
    df['sk_waktu_sampai'] = df['tgl_sampai_dt'].dt.strftime('%Y%m%d').astype(float)
    
    # --- CASTING TIPE NUMERIK ---
    # Pastikan durasi dan ongkir bertipe angka
    df['durasi_hari'] = pd.to_numeric(df['durasi_hari'], errors='coerce')
    df['biaya_ongkir'] = pd.to_numeric(df['biaya_ongkir'], errors='coerce')
    
    # --- PERAPIAN KOLOM ---
    kolom_urut = [
        'sk_waktu_sampai', 'sk_toko', 'sk_kurir', 'transaction_id', 
        'status_pengiriman', 'durasi_hari', 'biaya_ongkir'
    ]
    df_fakta = df[kolom_urut]
    
    # --- PENANGANAN NULL UNTUK PSYCOPG2 ---
    # Cegah error 'schema np does not exist' dengan mengonversi semua ke object
    # lalu mengubah NaN/NaT menjadi pure None milik Python.
    df_fakta = df_fakta.astype(object).where(pd.notnull(df_fakta), None)
    
    print("Transformasi selesai. Pratinjau hasil fact_delivery:")
    print(df_fakta.head())
    
    return df_fakta


@task(name="5. Load (Data Warehouse fact_delivery)")
def load_fact_delivery(df: pd.DataFrame):
    """Memuat data ke tabel fact_delivery di DW."""
    print("Membuka koneksi ke Data Warehouse (PostgreSQL)...")
    try:
        conn = psycopg2.connect(**DW_DB_CONFIG)
        cur = conn.cursor()
        
        data_tuples = [tuple(x) for x in df.to_numpy()]
        kolom = ','.join(list(df.columns))
        
        query = f"INSERT INTO fact_delivery({kolom}) VALUES %s"
        extras.execute_values(cur, query, data_tuples)
        
        conn.commit()
        cur.close()
        conn.close()
        print(f"Berhasil memuat {len(df)} baris ke tabel 'fact_delivery' di DW.")
    except Exception as e:
        print(f"Gagal memuat data fakta delivery ke DW: {e}")


@flow(name="ETL Flow: Fact Delivery", description="Pipeline kompleks menggabungkan API, OLTP, dan DW")
def etl_fact_delivery_flow():
    """Rangkaian utama ETL pengiriman."""
    
    # 1. Ekstraksi dari berbagai sumber
    df_api = extract_api_delivery()
    df_oltp_headers = extract_oltp_store_lookup()
    dim_lookups = extract_dw_lookups()
    
    # 2. Transformasi dan Lookup Lintas Sistem
    df_fakta = transform_fact_delivery(df_api, df_oltp_headers, dim_lookups)
    
    # 3. Load ke DW
    load_fact_delivery(df_fakta)


if __name__ == "__main__":
    etl_fact_delivery_flow()