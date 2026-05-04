import pandas as pd
from prefect import task, flow
import psycopg2
from psycopg2 import extras
import uuid

# =====================================================================
# KONFIGURASI KONEKSI DATABASE POSTGRESQL
# Sesuaikan dengan kredensial database PostgreSQL kampus
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



@task(name="1. Extract (OLTP)", retries=2, retry_delay_seconds=5)
def extract_tb_customers() -> pd.DataFrame:
    """Mengambil data pelanggan dari database OLTP."""
    print("Membuka koneksi ke OLTP (PostgreSQL)...")
    
    # Menyesuaikan query select dengan struktur tabel terbaru
    query = "SELECT customer_id, nama_lengkap, kota_domisili FROM tb_customers"
    
    try:
        # Membuka koneksi menggunakan psycopg2
        conn = psycopg2.connect(**OLTP_DB_CONFIG)
        cur = conn.cursor()
        
        # Mengeksekusi query dan mengambil hasilnya
        cur.execute(query)
        rows = cur.fetchall()
        
        # Mengambil nama kolom dari cursor untuk dijadikan nama kolom DataFrame
        colnames = [desc[0] for desc in cur.description]
        
        # Memasukkan data ke dalam DataFrame Pandas
        df = pd.DataFrame(rows, columns=colnames)
        
        cur.close()
        conn.close()
        print("Data berhasil ditarik dari OLTP PostgreSQL.")
    except Exception as e:
        # Fallback (cadangan) untuk demo jika tabel belum ada di lokal
        print("Tabel tb_customers tidak ditemukan di PostgreSQL.")
        print("Membuat data dummy untuk keperluan demonstrasi ke mahasiswa...")
        df = pd.DataFrame({
            'customer_id': ['c001', 'C002', 'c003', 'C004'],
            'nama_lengkap': ['budi santoso', 'SITI AMINAH', 'andi wijaya', 'dian prawira'],
            'kota_domisili': ['pontianak', 'SINTANG', 'jakarta', 'pontianak']
        })
        
    return df


@task(name="2. Transform (Data Warehouse Rules)")
def transform_dim_pelanggan(df: pd.DataFrame) -> pd.DataFrame:
    """Melakukan pembersihan dan penyesuaian bentuk tabel dimensi."""
    print("Memulai proses transformasi data...")
    
    # ATURAN 1: Standardisasi format huruf (Title Case)
    df['nama_lengkap'] = df['nama_lengkap'].str.title()
    df['kota_domisili'] = df['kota_domisili'].str.title()
    
    # ATURAN 2: Pastikan customer_id huruf kapital semua
    df['customer_id'] = df['customer_id'].str.upper()
    
    # ATURAN 3: Generate sk_pelanggan tipe INTEGER
    # Catatan: Di lingkungan real DW, SK biasanya digenerate otomatis 
    # oleh database (menggunakan tipe SERIAL/IDENTITY) saat di-INSERT.
    # Namun untuk kebutuhan simulasi Pandas ini, kita buat sequence integer manual.
    df['sk_pelanggan'] = range(1, len(df) + 1)
    
    # Merapikan urutan kolom sesuai struktur tabel di Data Warehouse
    kolom_urut = ['sk_pelanggan', 'customer_id', 'nama_lengkap', 'kota_domisili']
    df = df[kolom_urut]
    
    print("Transformasi selesai. Pratinjau hasil:")
    print(df.head())
    
    return df


@task(name="3. Load (Data Warehouse)")
def load_dim_pelanggan(df: pd.DataFrame):
    """Menyimpan data hasil transformasi ke tabel dim_pelanggan di DW PostgreSQL."""
    print("Membuka koneksi ke Data Warehouse (PostgreSQL)...")
    
    try:
        conn = psycopg2.connect(**DW_DB_CONFIG)
        cur = conn.cursor()
        
        # Menyiapkan data tuple dan nama kolom untuk query INSERT
        data_tuples = [tuple(x) for x in df.to_numpy()]
        kolom = ','.join(list(df.columns))
        
        # Query INSERT menggunakan psycopg2.extras.execute_values untuk bulk insert efisien
        query = f"INSERT INTO dim_pelanggan({kolom}) VALUES %s"
        
        extras.execute_values(cur, query, data_tuples)
        
        # Commit perubahan (wajib dilakukan jika mengubah data)
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"Berhasil memuat {len(df)} baris data pelanggan ke tabel 'dim_pelanggan' di DW PostgreSQL.")
    except Exception as e:
        print(f"Gagal memuat data ke DW: {e}")


@flow(name="ETL Flow: Dimensi Pelanggan", description="Pipeline khusus untuk mengisi dim_pelanggan dari tb_customers")
def etl_dim_pelanggan_flow():
    """Flow utama untuk merangkai Extract, Transform, dan Load."""
    
    # Eksekusi berurutan (Bisa dipantau di UI Prefect nanti)
    df_mentah = extract_tb_customers()
    df_bersih = transform_dim_pelanggan(df_mentah)
    load_dim_pelanggan(df_bersih)


if __name__ == "__main__":
    # Menjalankan flow saat file ini dieksekusi via terminal (python etl_dim_pelanggan.py)
    etl_dim_pelanggan_flow()