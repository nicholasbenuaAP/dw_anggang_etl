import pandas as pd
from prefect import task, flow
import psycopg2
from psycopg2 import extras

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


@task(name="1. Extract (OLTP tb_products)", retries=2, retry_delay_seconds=5)
def extract_tb_products() -> pd.DataFrame:
    """Mengambil data produk dari database OLTP."""
    print("Membuka koneksi ke OLTP (PostgreSQL)...")
    
    # Query disesuaikan dengan struktur DDL tujuan
    query = "SELECT product_id, kategori, nama_produk FROM tb_products"
    
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
        print("Tabel tb_products tidak ditemukan di PostgreSQL.")
        print("Membuat data dummy untuk keperluan demonstrasi ke mahasiswa...")
        df = pd.DataFrame({
            'product_id': ['PROD-001', 'PROD-002', 'PROD-003', 'PROD-004'],
            'kategori': ['elektronik', 'pakaian', 'buku', 'elektronik'],
            'nama_produk': ['Laptop Asus', 'Kemeja Flanel', 'Buku Data Warehouse', 'Mouse Wireless']
        })
        
    return df


@task(name="2. Transform (Data Warehouse Rules)")
def transform_dim_produk(df: pd.DataFrame) -> pd.DataFrame:
    """Melakukan pembersihan dan penyesuaian bentuk tabel dimensi produk."""
    print("Memulai proses transformasi data produk...")
    
    # ATURAN 1: Pastikan product_id sebagai Natural Key (NK) 
    # Kita standarisasi agar menggunakan huruf kapital semua
    df['product_id'] = df['product_id'].str.upper()
    
    # ATURAN Tambahan: Merapikan format teks agar seragam (Title Case)
    df['kategori'] = df['kategori'].str.title()
    df['nama_produk'] = df['nama_produk'].str.title()
    
    # ATURAN 2: Generate sk_produk
    # Karena di tabel dim_produk menggunakan tipe SERIAL, kita bisa men-generate
    # urutan secara manual di Pandas untuk disisipkan saat Load.
    df['sk_produk'] = range(1, len(df) + 1)
    
    # Merapikan urutan kolom agar persis sama dengan DDL Data Warehouse
    kolom_urut = ['sk_produk', 'product_id', 'kategori', 'nama_produk']
    df = df[kolom_urut]
    
    print("Transformasi selesai. Pratinjau hasil:")
    print(df.head())
    
    return df


@task(name="3. Load (Data Warehouse dim_produk)")
def load_dim_produk(df: pd.DataFrame):
    """Menyimpan data hasil transformasi ke tabel dim_produk di DW PostgreSQL."""
    print("Membuka koneksi ke Data Warehouse (PostgreSQL)...")
    
    try:
        conn = psycopg2.connect(**DW_DB_CONFIG)
        cur = conn.cursor()
        
        # Menyiapkan data tuple dan nama kolom untuk query INSERT
        data_tuples = [tuple(x) for x in df.to_numpy()]
        kolom = ','.join(list(df.columns))
        
        # Query INSERT menggunakan psycopg2.extras.execute_values untuk bulk insert efisien
        query = f"INSERT INTO dim_produk({kolom}) VALUES %s"
        
        extras.execute_values(cur, query, data_tuples)
        
        # Commit perubahan
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"Berhasil memuat {len(df)} baris data produk ke tabel 'dim_produk' di DW PostgreSQL.")
    except Exception as e:
        print(f"Gagal memuat data ke DW: {e}")


@flow(name="ETL Flow: Dimensi Produk", description="Pipeline khusus untuk mengisi dim_produk dari tb_products")
def etl_dim_produk_flow():
    """Flow utama untuk merangkai Extract, Transform, dan Load."""
    
    # Eksekusi berurutan (Bisa dipantau di UI Prefect)
    df_mentah = extract_tb_products()
    df_bersih = transform_dim_produk(df_mentah)
    load_dim_produk(df_bersih)


if __name__ == "__main__":
    # Menjalankan flow saat file ini dieksekusi via terminal
    etl_dim_produk_flow()