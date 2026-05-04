# File: pipeline.py
from prefect import flow, task

# Import semua flow dari skrip dimensi
from dim_waktu_etl import flow_etl_dim_waktu
from dim_toko_etl import flow_etl_dim_toko
from dim_produk_etl import etl_dim_produk_flow
from dim_pelanggan_etl import etl_dim_pelanggan_flow
from dim_kurir_etl import etl_dim_kurir_flow

# Import semua flow dari skrip fakta
from fact_target_sales_etl import etl_fact_target_sales_flow
from fact_sales_etl import etl_fact_sales_flow
from fact_delivery_etl import etl_fact_delivery_flow

@flow(name="Master Pipeline ETL Enggang Ritel", description="Orkestrasi utama untuk semua proses ETL (Dimensi lalu Fakta)")
def main_etl_pipeline():
    print("Memulai eksekusi Master Pipeline ETL...")
    
    # 1. Jalankan proses ETL untuk Tabel Dimensi (Data Master)
    print("--- MEMULAI ETL DIMENSI ---")
    flow_etl_dim_waktu()
    flow_etl_dim_toko()
    etl_dim_produk_flow()
    etl_dim_pelanggan_flow()
    etl_dim_kurir_flow()
    
    # 2. Jalankan proses ETL untuk Tabel Fakta (Data Transaksi)
    # Pastikan tabel dimensi sudah berhasil sebelum menjalankan tabel fakta
    print("--- MEMULAI ETL FAKTA ---")
    etl_fact_target_sales_flow()
    etl_fact_sales_flow()
    etl_fact_delivery_flow()
    
    print("Master Pipeline ETL selesai dieksekusi dengan sukses!")

if __name__ == "__main__":
    # Tes eksekusi lokal
    main_etl_pipeline()
