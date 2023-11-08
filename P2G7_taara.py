
'''
=================================================

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. 
Adapun dataset yang dipakai adalah dataset mengenai penjualan produk Adidas di berbagai region.

=================================================
'''

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch

# Pengaturan koneksi ke database PostgreSQL
db_config = {
    "dbname": "mydatabase",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

def write_csv_to_postgres(csv_file, db_config, table_name):
    '''
    Fungsi untuk mengambil data dari file CSV dan memasukkannya ke dalam database PostgreSQL

    Parameter: 
    csv_file : membaca file CSV
    db_config : konfigurasi koneksi database PostgreSQL, termasuk nama database, pengguna, kata sandi, host, dan port.
    table_name : nama tabel di database yang akan menerima data

    Contoh penggunaan:
    write_csv_to_postgres(csv_file, db_config, table_name)
    '''

    # Menghubungkan ke database
    conn = psycopg2.connect(**db_config)

    # Baca file CSV menjadi DataFrame
    df = pd.read_csv(csv_file)

    # Membuat koneksi menggunakan SQLAlchemy
    engine = create_engine(f'postgresql+psycopg2://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["dbname"]}')

    # Menulis DataFrame ke database PostgreSQL
    df.to_sql(table_name, engine, if_exists='replace', index=False)

    # Tutup koneksi
    conn.close()


def fetch_data_from_postgres(db_config, table_name):
    '''
    Fungsi untuk mengambil data dari database PostgreSQL dan memuatnya ke dalam suatu DataFrame.

    Parameter:
    db_config : konfigurasi koneksi database PostgreSQL, termasuk nama database, pengguna, kata sandi, host, dan port.
    table_name : nama tabel di database yang akan menerima data

    Contoh penggunaan:
    data_from_postgres = fetch_data_from_postgres(db_config, table_name)
    '''

    # Mengatur koneksi ke database PostgreSQL
    conn = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")
    
    # Mengambil data dengan query ke dalam DataFrame
    query = 'SELECT * FROM public.table_gc7'
    df = pd.read_sql_query(query, conn)

    return df


def data_cleansing(input_df):
    '''
    Fungsi untuk melakukan data cleansing

    Parameter :
    input_df : objek DataFrame yang akan dibersihkan.

    return input_df : mengembalikan hasil data yang sudah dicleansing

    Contoh penggunaan:
    cleansed_data = data_cleansing(data_from_csv)
    '''
    # Menghapus simbol pada beberapa kolom
    input_df['Price per Unit'] = input_df['Price per Unit'].str.replace('$', '')
    input_df['Price per Unit'] = input_df['Price per Unit'].str.replace('.00 ', '')
    input_df['Units Sold'] = input_df['Units Sold'].str.replace(',', '')
    input_df['Total Sales'] = input_df['Total Sales'].str.replace('$', '')
    input_df['Total Sales'] = input_df['Total Sales'].str.replace(',', '')
    input_df['Operating Profit'] = input_df['Operating Profit'].str.replace('$', '')
    input_df['Operating Profit'] = input_df['Operating Profit'].str.replace(',', '')
    input_df['Operating Margin'] = input_df['Operating Margin'].str.replace('%', '')

    # Mengganti tipe input_df kolom
    input_df['Invoice Date'] = pd.to_datetime(input_df['Invoice Date'])
    input_df['Price per Unit'] = input_df['Price per Unit'].astype(int)
    input_df['Units Sold'] = input_df['Units Sold'].astype(int)
    input_df['Total Sales'] = input_df['Total Sales'].astype(int)
    input_df['Operating Profit'] = input_df['Operating Profit'].astype(int)
    input_df['Operating Margin'] = input_df['Operating Margin'].astype(int)

    # Mengganti nama kolom
    input_df.rename(columns={'Retailer': 'retailer'}, inplace=True)
    input_df.rename(columns={'Retailer ID': 'retailer_id'}, inplace=True)
    input_df.rename(columns={'Invoice Date': 'invoice_date'}, inplace=True)
    input_df.rename(columns={'Region': 'region'}, inplace=True)
    input_df.rename(columns={'State': 'state'}, inplace=True)
    input_df.rename(columns={'City': 'city'}, inplace=True)
    input_df.rename(columns={'Product': 'product'}, inplace=True)
    input_df.rename(columns={'Price per Unit': 'price_per_unit'}, inplace=True)
    input_df.rename(columns={'Units Sold': 'units_sold'}, inplace=True)
    input_df.rename(columns={'Total Sales': 'total_sales'}, inplace=True)
    input_df.rename(columns={'Operating Profit': 'operating_profit'}, inplace=True)
    input_df.rename(columns={'Operating Margin': 'operating_margin'}, inplace=True)
    input_df.rename(columns={'Sales Method': 'sales_method'}, inplace=True)

    return input_df


def migrate_to_elasticsearch(data_frame, es_index):
    '''
    Fungsi untuk migrasi data ke Elasticsearch

    Parameters:
    data_frame : objek DataFrame yang berisi data cleansing yang akan dimigrasi ke Elasticsearch.
    es_index : nama indeks Elasticsearch tempat data akan dimigrasi.

    Contoh penggunaan:
    migrate_to_elasticsearch(cleansed_data, elasticsearch_index)
    '''
    
    es = Elasticsearch('http://localhost:9200')
    print(es.ping())

    for i, row in data_frame.iterrows():
        doc = row.to_json()
        es.index(index=es_index, id=i + 1, body=doc)

    print('---------- Finished Migrating Data ----------')


# Nama tabel PostgreSQL yang akan digunakan
table_name = 'table_gc7'

# Nama indeks Elasticsearch
elasticsearch_index = 'sales'

# Nama file CSV
csv_file = 'P2G7_taara_data_raw.csv'

# Panggil fungsi untuk mengeksekusi function write_csv_to_postgres
write_csv_to_postgres(csv_file, db_config, table_name)

# Mengambil data dari PostgreSQL
data_from_postgres = fetch_data_from_postgres(db_config, table_name)

# Data cleansing
cleansed_data = data_cleansing(data_from_postgres)

# Simpan data yang sudah dicleansing ke dalam csv baru
cleansed_data.to_csv('P2G7_taara_data_clean.csv', index=False)
print('Data Telah Berhasil Disimpan')

# Migrasi data ke Elasticsearch
migrate_to_elasticsearch(cleansed_data, elasticsearch_index)
