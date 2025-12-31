import streamlit as st
import pandas as pd
import numpy as np
import io
import time
from datetime import datetime

# --- CONFIG & UI ---
st.set_page_config(page_title="Universal Order Processor", layout="wide")
st.title("🛒 Universal Marketplace Order Processor")
st.markdown("Logic: **Shopee (Instant & No Resi)** | **Tokopedia (Perlu Dikirim)**")

# --- FUNGSI CLEANING SKU ---
def clean_sku(sku):
    if pd.isna(sku): return ""
    sku = str(sku).strip()
    sku = ''.join(char for char in sku if ord(char) >= 32)
    if '-' in sku:
        return sku.split('-', 1)[-1].strip()
    return sku

# ==========================================
# CORE PROCESSING
# ==========================================
def process_universal_data(uploaded_files, kamus_data):
    start_time = time.time()
    
    df_bundle = kamus_data['bundle']
    df_sku = kamus_data['sku']
    df_kurir = kamus_data['kurir']

    df_bundle['SKU Bundle'] = df_bundle['SKU Bundle'].apply(clean_sku)
    
    # Ambil Kolom B (Kode) & C (Nama) dari SKU Master
    df_sku_lookup = df_sku.iloc[:, [1, 2]].copy()
    df_sku_lookup.columns = ['SKU Component', 'Product Name Master']
    df_sku_lookup['SKU Component'] = df_sku_lookup['SKU Component'].apply(clean_sku)

    # Ambil List Kurir Instant Shopee
    instant_list = []
    if 'Instant/Same Day' in df_kurir.columns:
        kurir_col = df_kurir.columns[0]
        instant_list = df_kurir[df_kurir['Instant/Same Day'].astype(str).str.upper().isin(['YES', 'YA', 'TRUE', '1'])][kurir_col].tolist()

    all_expanded_rows = []

    for mp_type, file_obj in uploaded_files:
        try:
            # Baca file mentah tanpa anggapan header dulu
            if file_obj.name.endswith('.csv'):
                df_raw = pd.read_csv(file_obj, dtype=str, header=None)
            else:
                df_raw = pd.read_excel(file_obj, dtype=str, header=None)

            # --- SEARCH REAL HEADER ---
            # Cari baris mana yang mengandung 'Order Status' atau 'Status Pesanan'
            header_row_idx = 0
            for i, row in df_raw.head(10).iterrows():
                row_str = " ".join([str(x) for x in row.values]).lower()
                if 'order status' in row_str or 'status pesanan' in row_str:
                    header_row_idx = i
                    break
            
            # Set header yang benar dan buang baris di atasnya
            df_actual = df_raw.iloc[header_row_idx:].copy()
            df_actual.columns = df_actual.iloc[0]
            df_actual = df_actual.iloc[1:].reset_index(drop=True)
            df_actual.columns = df_actual.columns.str.strip()

            # --- LOGIC SHOPEE ---
            if mp_type == 'Shopee':
                df_filtered = df_actual[
                    (df_actual['Status Pesanan'].str.strip() == 'Perlu Dikirim') &
                    (df_actual['Pesanan yang Dikelola Shopee'].str.strip().str.upper() == 'NO') &
                    (df_actual['Opsi Pengiriman'].isin(instant_list)) &
                    (df_actual['No. Resi'].isna() | (df_actual['No. Resi'].astype(str).str.strip() == ''))
                ].copy()
                sku_col, qty_col, order_id_col = 'Nomor Referensi SKU', 'Jumlah', 'No. Pesanan'

            # --- LOGIC TOKOPEDIA ---
            elif mp_type == 'Tokopedia':
                # Pastikan kolom 'Order Status' ada
                if 'Order Status' not in df_actual.columns:
                    st.error(f"Kolom 'Order Status' tidak ditemukan di {file_obj.name}. Kolom yang ada: {list(df_actual.columns)}")
                    continue
                
                # Filter: Perlu dikirim (Case Insensitive)
                df_filtered = df_actual[df_actual['Order Status'].astype(str).str.strip().str.lower() == 'perlu dikirim'].copy()
                sku_col, qty_col, order_id_col = 'Seller SKU', 'Quantity', 'Order ID'

            if df_filtered.empty: continue

            # 3. Bundle Expansion
            for _, row in df_filtered.iterrows():
                sku_key = clean_sku(row[sku_col])
                # Fix: Handle quantity string to float
                raw_qty = str(row[qty_col]).replace(',', '.')
                qty_order = float(raw_qty) if raw_qty != 'nan' else 0
                
                if sku_key in df_bundle['SKU Bundle'].values:
                    comps = df_bundle[df_bundle['SKU Bundle'] == sku_key]
                    for _, c in comps.iterrows():
                        comp_qty = float(str(c['Component Quantity']).replace(',', '.'))
                        all_expanded_rows.append({
                            'Marketplace': mp_type,
                            'Order ID': row[order_id_col],
                            'SKU Original': row[sku_col],
                            'Is Bundle?': 'Yes',
                            'SKU Component': clean_sku(c['SKU Component']),
                            'Qty': qty_order * comp_qty
                        })
                else:
                    all_expanded_rows.append({
                        'Marketplace': mp_type,
                        'Order ID': row[order_id_col],
                        'SKU Original': row[sku_col],
                        'Is Bundle?': 'No',
                        'SKU Component': sku_key,
                        'Qty': qty_order
                    })

        except Exception as e:
            st.error(f"Gagal memproses {file_obj.name}: {e}")

    if not all_expanded_rows: return None

    df_final = pd.DataFrame(all_expanded_rows)
    df_final = pd.merge(df_final, df_sku_lookup, on='SKU Component', how='left')
    df_final['Product Name'] = df_final['Product Name Master'].fillna(df_final['SKU Component'])
    
    df_summary = df_final.groupby(['Marketplace', 'SKU Component', 'Product Name']).agg({'Qty': 'sum'}).reset_index()
    return {'detail': df_final, 'summary': df_summary}

# --- SIDEBAR UI ---
st.sidebar.header("📁 Upload File")
kamus_file = st.sidebar.file_uploader("1. Upload Kamus.xlsx", type=['xlsx'])
shopee_file = st.sidebar.file_uploader("2. Order Shopee", type=['csv', 'xlsx'])
tokped_file = st.sidebar.file_uploader("3. Order Tokopedia", type=['csv', 'xlsx'])

if st.sidebar.button("🚀 PROSES DATA", type="primary", use_container_width=True):
    if not kamus_file or (not shopee_file and not tokped_file):
        st.error("Upload Kamus dan minimal 1 file Order!")
    else:
        try:
            excel = pd.ExcelFile(kamus_file, engine='openpyxl')
            kamus_dict = {
                'kurir': pd.read_excel(excel, sheet_name='Kurir-Shopee'),
                'bundle': pd.read_excel(excel, sheet_name='Bundle Master'),
                'sku': pd.read_excel(excel, sheet_name='SKU Master')
            }
            
            files_to_process = []
            if shopee_file: files_to_process.append(('Shopee', shopee_file))
            if tokped_file: files_to_process.append(('Tokopedia', tokped_file))
            
            res = process_universal_data(files_to_process, kamus_dict)
            
            if res:
                st.session_state.res = res
                st.success("Berhasil diproses!")
            else:
                st.warning("Tidak ada data yang cocok dengan filter.")
        except Exception as e:
            st.error(f"Terjadi kesalahan saat membaca Kamus: {e}")

# --- HASIL ---
if 'res' in st.session_state:
    res = st.session_state.res
    tab1, tab2, tab3 = st.tabs(["📋 Detail Order", "📦 Ringkasan SKU", "📥 Download"])
    with tab1: st.dataframe(res['detail'], use_container_width=True)
    with tab2: st.dataframe(res['summary'], use_container_width=True)
    with tab3:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            res['detail'].to_excel(writer, sheet_name='Detail', index=False)
            res['summary'].to_excel(writer, sheet_name='Summary', index=False)
        st.download_button("📥 Download Excel", data=output.getvalue(), file_name="Picking_List.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
