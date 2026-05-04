import pandas as pd
from prefect import task, flow
import psycopg2
from psycopg2 import extras

# =====================================================================
# KONFIGURASI KONEKSI DATABASE
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


@task(name="1. Extract (OLTP Sales Data)", retries=2, retry_delay_seconds=5)
def extract_sales_oltp() -> pd.DataFrame:
    """Mengambil data transaksi kasir dari OLTP (Header + Detail)."""
    print("Membuka koneksi ke OLTP...")
    
    # Melakukan JOIN langsung di level database sumber untuk efisiensi
    query = """
        SELECT 
            h.transaction_id, 
            h.tgl_transaksi, 
            h.customer_id, 
            h.store_id, 
            d.product_id, 
            d.qty, 
            d.subtotal_harga
        FROM tb_sales_headers h
        JOIN tb_sales_details d ON h.transaction_id = d.transaction_id
    """
    
    try:
        conn = psycopg2.connect(**OLTP_DB_CONFIG)
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=colnames)
        
        cur.close()
        conn.close()
        print(f"Berhasil menarik {len(df)} baris data transaksi (OLTP).")
        return df
        
    except Exception as e:
        print("Tabel OLTP tidak ditemukan. Membuat data dummy transaksi...")
        return pd.DataFrame([
            {"transaction_id": "TRX-001", "tgl_transaksi": "2024-10-15", "customer_id": "C001", "store_id": "ST-01", "product_id": "PROD-001", "qty": 2, "subtotal_harga": 15000000},
            {"transaction_id": "TRX-001", "tgl_transaksi": "2024-10-15", "customer_id": "C001", "store_id": "ST-01", "product_id": "PROD-002", "qty": 1, "subtotal_harga": 250000},
            {"transaction_id": "TRX-002", "tgl_transaksi": "2024-10-16", "customer_id": "C002", "store_id": "ST-02", "product_id": "PROD-003", "qty": 5, "subtotal_harga": 500000}
        ])

@task(name="2. Extract (Lookup Dimensi DW)")
def extract_dim_lookups() -> dict:
    """Mengambil Natural Key dan Surrogate Key dari semua dimensi untuk keperluan Lookup."""
    print("Mengambil data dimensi dari Data Warehouse untuk proses Lookup...")
    
    dim_dict = {}
    try:
        conn = psycopg2.connect(**DW_DB_CONFIG)
        
        # Ambil lookup dimensi pelanggan
        df_pelanggan = pd.read_sql("SELECT sk_pelanggan, customer_id FROM dim_pelanggan", conn)
        dim_dict['pelanggan'] = df_pelanggan
        
        # Ambil lookup dimensi produk
        df_produk = pd.read_sql("SELECT sk_produk, product_id FROM dim_produk", conn)
        dim_dict['produk'] = df_produk
        
        # Ambil lookup dimensi toko (Asumsi kolom id aslinya bernama store_id)
        df_toko = pd.read_sql("SELECT sk_toko, store_id FROM dim_toko", conn)
        dim_dict['toko'] = df_toko
        
        conn.close()
        return dim_dict
        
    except Exception as e:
        print("Gagal terhubung ke DW. Membuat dummy lookup dimensi...")
        dim_dict['pelanggan'] = pd.DataFrame({"sk_pelanggan": [1, 2], "customer_id": ["C001", "C002"]})
        dim_dict['produk'] = pd.DataFrame({"sk_produk": [1, 2, 3], "product_id": ["PROD-001", "PROD-002", "PROD-003"]})
        dim_dict['toko'] = pd.DataFrame({"sk_toko": [1, 2], "store_id": ["ST-01", "ST-02"]})
        return dim_dict

@task(name="3. Transform (Lookup & Data Formatting)")
def transform_fact_sales(df_sales: pd.DataFrame, dim_lookups: dict) -> pd.DataFrame:
    """Melakukan konversi waktu dan pencarian SK (Cross-database Lookup)."""
    print("Memulai transformasi Data Fakta...")
    
    # ATURAN 1: Pastikan format teks (NK) sesuai saat di-merge 
    # (Di modul sebelumnya kita menggunakan uppercase untuk product_id dan customer_id)
    df_sales['customer_id'] = df_sales['customer_id'].str.upper()
    df_sales['product_id'] = df_sales['product_id'].str.upper()
    df_sales['store_id'] = df_sales['store_id'].str.upper()

    # ATURAN 2: Lookup SK Waktu (Konversi Tanggal ke Integer YYYYMMDD)
    # Contoh: "2024-10-15" menjadi 20241015
    df_sales['tgl_transaksi'] = pd.to_datetime(df_sales['tgl_transaksi'])
    df_sales['sk_waktu_transaksi'] = df_sales['tgl_transaksi'].dt.strftime('%Y%m%d').astype(int)

    # ATURAN 3: Lookup (Merge) SK Pelanggan
    df_sales = df_sales.merge(dim_lookups['pelanggan'], on='customer_id', how='left')
    
    # ATURAN 4: Lookup (Merge) SK Produk
    df_sales = df_sales.merge(dim_lookups['produk'], on='product_id', how='left')
    
    # ATURAN 5: Lookup (Merge) SK Toko
    df_sales = df_sales.merge(dim_lookups['toko'], on='store_id', how='left')

    # Merapikan struktur kolom sesuai dengan schema DDL fact_sales
    kolom_urut = [
        'sk_waktu_transaksi', 
        'sk_toko', 
        'sk_pelanggan', 
        'sk_produk', 
        'transaction_id',   # Degenerate Dimension
        'qty', 
        'subtotal_harga'
    ]
    df_fakta = df_sales[kolom_urut]
    
    # Pandas akan merubah kolom integer menjadi float jika ada NaN (data gagal lookup).
    # Untuk menghindari error saat di-insert ke PostgreSQL, kita ubah nilai NaN menjadi None
    df_fakta = df_fakta.where(pd.notnull(df_fakta), None)

    print("Transformasi selesai. Pratinjau hasil fact_sales:")
    print(df_fakta.head())
    
    return df_fakta

@task(name="4. Load (Data Warehouse fact_sales)")
def load_fact_sales(df: pd.DataFrame):
    """Memuat data transaksi akhir ke tabel fact_sales."""
    print("Membuka koneksi ke Data Warehouse (PostgreSQL)...")
    
    try:
        conn = psycopg2.connect(**DW_DB_CONFIG)
        cur = conn.cursor()
        
        # Menyiapkan data tuple untuk bulk insert
        data_tuples = [tuple(x) for x in df.to_numpy()]
        kolom = ','.join(list(df.columns))
        
        # Bulk Insert Data Fakta
        query = f"INSERT INTO fact_sales({kolom}) VALUES %s"
        extras.execute_values(cur, query, data_tuples)
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"Berhasil memuat {len(df)} baris ke tabel 'fact_sales' di DW.")
    except Exception as e:
        print(f"Gagal memuat data fakta ke DW: {e}")

@flow(name="ETL Flow: Fact Sales", description="Pipeline tabel fakta dengan proses Lookup silang database")
def etl_fact_sales_flow():
    """Flow utama untuk merangkai tabel fakta penjualan."""
    
    # 1. Tarik transaksi dari sumber (OLTP)
    df_transaksi = extract_sales_oltp()
    
    # 2. Tarik peta dimensi dari target (DW)
    dim_lookups = extract_dim_lookups()
    
    # 3. Lakukan penggabungan dan konversi (Transform)
    df_fakta = transform_fact_sales(df_transaksi, dim_lookups)
    
    # 4. Masukkan ke tujuan akhir
    load_fact_sales(df_fakta)


if __name__ == "__main__":
    etl_fact_sales_flow()