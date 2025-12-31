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

# --- FUNGSI CLEANING SKU (LOGIC KITA) ---
def clean_sku(sku):
    if pd.isna(sku): return ""
    sku = str(sku).strip()
    sku = ''.join(char for char in sku if ord(char) >= 32) # Hapus karakter aneh
    if '-' in sku:
        return sku.split('-', 1)[-1].strip() # Ambil bagian kanan hyphen
    return sku

# ==========================================
# CORE PROCESSING
# ==========================================
def process_universal_data(uploaded_files, kamus_data):
    start_time = time.time()
    
    # 1. Persiapan Kamus
    df_kurir = kamus_data['kurir']
    df_bundle = kamus_data['bundle']
    df_sku = kamus_data['sku']

    # Pre-clean Keys di Kamus
    df_bundle['SKU Bundle'] = df_bundle['SKU Bundle'].apply(clean_sku)
    
    # Ambil Kolom B (Kode) & C (Nama) dari SKU Master
    # Pandas index 1 = B, index 2 = C
    df_sku_lookup = df_sku.iloc[:, [1, 2]].copy()
    df_sku_lookup.columns = ['SKU Component', 'Product Name Master']
    df_sku_lookup['SKU Component'] = df_sku_lookup['SKU Component'].apply(clean_sku)

    # Ambil List Kurir Instant Shopee
    instant_list = []
    if 'Instant/Same Day' in df_kurir.columns:
        kurir_col = df_kurir.columns[0]
        instant_list = df_kurir[df_kurir['Instant/Same Day'].astype(str).str.upper().isin(['YES', 'YA', 'TRUE', '1'])][kurir_col].tolist()

    all_expanded_rows = []

    # 2. Proses File Order
    for mp_type, file_obj in uploaded_files:
        try:
            if file_obj.name.endswith('.csv'):
                df_raw = pd.read_csv(file_obj, dtype=str)
                if len(df_raw.columns) < 2: # Cek jika separator titik koma
                    file_obj.seek(0)
                    df_raw = pd.read_csv(file_obj, sep=';', dtype=str)
            else:
                df_raw = pd.read_excel(file_obj, dtype=str)

            # Buang baris deskripsi "Platform unique..." jika ada
            if len(df_raw) > 0 and str(df_raw.iloc[0, 0]).startswith('Platform unique'):
                df_raw = df_raw.iloc[1:].reset_index(drop=True)

            # --- LOGIC SHOPEE ---
            if mp_type == 'Shopee':
                df_raw.columns = df_raw.columns.str.strip()
                # Filter: Perlu Dikirim, Not Managed, Kurir Instant, No Resi
                df_filtered = df_raw[
                    (df_raw['Status Pesanan'].str.strip() == 'Perlu Dikirim') &
                    (df_raw['Pesanan yang Dikelola Shopee'].str.strip().str.upper() == 'NO') &
                    (df_raw['Opsi Pengiriman'].isin(instant_list)) &
                    (df_raw['No. Resi'].isna() | (df_raw['No. Resi'].astype(str).str.strip() == ''))
                ].copy()
                sku_col, qty_col, order_id_col = 'Nomor Referensi SKU', 'Jumlah', 'No. Pesanan'

            # --- LOGIC TOKOPEDIA ---
            elif mp_type == 'Tokopedia':
                df_raw.columns = df_raw.columns.str.strip()
                # Filter: Perlu dikirim saja
                df_filtered = df_raw[df_raw['Order Status'].astype(str).str.strip().str.lower() == 'perlu dikirim'].copy()
                sku_col, qty_col, order_id_col = 'Seller SKU', 'Quantity', 'Order ID'

            if df_filtered.empty: continue

            # 3. Bundle Expansion
            for _, row in df_filtered.iterrows():
                sku_key = clean_sku(row[sku_col])
                qty_order = float(row[qty_col])
                
                if sku_key in df_bundle['SKU Bundle'].values:
                    # Expand Bundle
                    comps = df_bundle[df_bundle['SKU Bundle'] == sku_key]
                    for _, c in comps.iterrows():
                        all_expanded_rows.append({
                            'Marketplace': mp_type,
                            'Order ID': row[order_id_col],
                            'SKU Original': row[sku_col],
                            'Is Bundle?': 'Yes',
                            'SKU Component': clean_sku(c['SKU Component']),
                            'Qty': qty_order * float(c['Component Quantity'])
                        })
                else:
                    # Satuan
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

    # 4. Finalisasi Data
    df_final = pd.DataFrame(all_expanded_rows)
    
    # Lookup Product Name (Delayed)
    df_final = pd.merge(df_final, df_sku_lookup, on='SKU Component', how='left')
    df_final['Product Name'] = df_final['Product Name Master'].fillna(df_final['SKU Component'])

    # Summary
    df_summary = df_final.groupby(['Marketplace', 'SKU Component', 'Product Name']).agg({'Qty': 'sum'}).reset_index()
    
    return {'detail': df_final, 'summary': df_summary}

# --- SIDEBAR UI ---
st.sidebar.header("📁 Upload File")
kamus_file = st.sidebar.file_uploader("1. File Kamus Master", type=['xlsx'])
shopee_file = st.sidebar.file_uploader("2. Order Shopee", type=['csv', 'xlsx'])
tokped_file = st.sidebar.file_uploader("3. Order Tokopedia", type=['csv', 'xlsx'])

if st.sidebar.button("🚀 PROSES DATA", type="primary", use_container_width=True):
    if not kamus_file or (not shopee_file and not tokped_file):
        st.error("Upload Kamus dan minimal 1 file Order!")
    else:
        # Load Kamus
        excel = pd.ExcelFile(kamus_file)
        kamus_dict = {
            'kurir': pd.read_excel(excel, sheet_name='Kurir-Shopee'),
            'bundle': pd.read_excel(excel, sheet_name='Bundle Master'),
            'sku': pd.read_excel(excel, sheet_name='SKU Master')
        }
        
        # Build Uploaded List
        files_to_process = []
        if shopee_file: files_to_process.append(('Shopee', shopee_file))
        if tokped_file: files_to_process.append(('Tokopedia', tokped_file))
        
        res = process_universal_data(files_to_process, kamus_dict)
        
        if res:
            st.session_state.res = res
            st.success("Berhasil diproses!")
        else:
            st.warning("Tidak ada data yang cocok dengan filter.")

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
