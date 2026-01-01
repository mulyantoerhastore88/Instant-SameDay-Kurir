import streamlit as st
import pandas as pd
import numpy as np
import io
import time
from datetime import datetime

# --- CONFIG ---
st.set_page_config(page_title="Universal Order Processor", layout="wide")
st.title("🛒 Universal Marketplace Order Processor")
st.markdown("Logic: **Shopee (Filter Lengkap)** | **Tokopedia (Status: Perlu Dikirim)**")

# --- FUNGSI CLEANING ---
def clean_sku(sku):
    if pd.isna(sku): return ""
    sku = str(sku).strip()
    # Hapus karakter aneh (non-printable)
    sku = ''.join(char for char in sku if ord(char) >= 32)
    if '-' in sku:
        return sku.split('-', 1)[-1].strip()
    return sku

# --- FUNGSI BACA FILE TOKPED (ANTI-ERROR) ---
def read_tokped_file(file_obj):
    try:
        # Coba baca sebagai CSV (koma)
        df = pd.read_csv(file_obj, dtype=str)
        
        # Cek separator, kalau kolom cuma 1 berarti salah separator
        if len(df.columns) < 2:
            file_obj.seek(0)
            df = pd.read_csv(file_obj, sep=';', dtype=str)
            
        # BERSIHKAN BARIS SAMPAH (Deskripsi di baris pertama data)
        # Ciri: Kolom Order ID isinya "Platform unique order ID."
        if len(df) > 0:
            first_val = str(df.iloc[0, 0])
            if "Platform unique" in first_val or "Current order" in str(df.iloc[0, 1]):
                df = df.iloc[1:].reset_index(drop=True)
                
        # Bersihkan nama kolom
        df.columns = df.columns.str.strip()
        return df
    except Exception as e:
        st.error(f"Gagal baca file Tokped: {e}")
        return None

# ==========================================
# MAIN LOGIC
# ==========================================
def process_data(uploaded_files, kamus_data):
    start_time = time.time()
    
    # 1. LOAD KAMUS (Sesuai Struktur Baru)
    df_kurir = kamus_data['kurir']
    df_bundle = kamus_data['bundle']
    df_sku = kamus_data['sku']

    # Mapping Bundle: Kit_Sku -> [(Comp_Sku, Qty), ...]
    bundle_map = {}
    # Pastikan nama kolom sesuai file kamus baru
    # Bundle Master: Kit_Sku, Component_Sku, Component_Qty
    for _, row in df_bundle.iterrows():
        kit = clean_sku(row.get('Kit_Sku', ''))
        comp = clean_sku(row.get('Component_Sku', ''))
        qty = float(row.get('Component_Qty', 1))
        if kit:
            if kit not in bundle_map: bundle_map[kit] = []
            bundle_map[kit].append((comp, qty))

    # Mapping Nama Produk: Product_Sku -> Product_Name
    sku_name_map = {}
    for _, row in df_sku.iterrows():
        p_sku = clean_sku(row.get('Product_Sku', ''))
        p_name = row.get('Product_Name', '')
        if p_sku: sku_name_map[p_sku] = p_name

    # List Kurir Instant Shopee
    instant_list = []
    if 'Instant/Same Day' in df_kurir.columns:
        instant_list = df_kurir[
            df_kurir['Instant/Same Day'].astype(str).str.upper().isin(['YES', 'YA', 'TRUE', '1'])
        ]['Opsi Pengiriman'].tolist()

    all_rows = []

    # 2. PROSES FILES
    for mp_type, file_obj in uploaded_files:
        df_raw = None
        
        # --- BACA FILE ---
        if mp_type == 'Tokopedia':
            df_raw = read_tokped_file(file_obj)
        else:
            # Shopee (biasanya Excel/CSV standar)
            try:
                if file_obj.name.endswith('.csv'):
                    df_raw = pd.read_csv(file_obj, dtype=str)
                else:
                    df_raw = pd.read_excel(file_obj, dtype=str)
            except:
                st.error(f"Gagal baca file {mp_type}")
                continue

        if df_raw is None or df_raw.empty: continue
        
        # Bersihkan nama kolom
        df_raw.columns = df_raw.columns.str.strip()

        # --- FILTERING LOGIC ---
        df_filtered = pd.DataFrame()
        
        if mp_type == 'Tokopedia':
            # Pastikan kolom kunci ada
            if 'Order Status' not in df_raw.columns:
                st.error(f"Kolom 'Order Status' tidak ditemukan di file Tokopedia. Kolom terbaca: {list(df_raw.columns)}")
                continue
            
            # FILTER TOKPED: HANYA STATUS "Perlu dikirim"
            df_filtered = df_raw[
                df_raw['Order Status'].astype(str).str.strip().str.lower() == 'perlu dikirim'
            ].copy()
            
            # Mapping kolom Tokped
            col_sku = 'Seller SKU'
            col_qty = 'Quantity'
            col_order = 'Order ID'
            col_resi = 'Tracking ID' # Cuma buat info
            col_kurir = 'Delivery Option' # Cuma buat info

        elif mp_type == 'Shopee':
            # FILTER SHOPEE: 4 Syarat Wajib
            # 1. Status = Perlu Dikirim
            # 2. Managed = No
            # 3. Kurir = Instant/Same Day (cek di kamus)
            # 4. Resi = Kosong
            
            # Handle kolom Tracking ID kosong
            track_col = 'No. Resi'
            if track_col not in df_raw.columns: df_raw[track_col] = np.nan
            
            df_filtered = df_raw[
                (df_raw['Status Pesanan'].astype(str).str.upper() == 'PERLU DIKIRIM') &
                (df_raw['Pesanan yang Dikelola Shopee'].astype(str).str.upper() == 'NO') &
                (df_raw['Opsi Pengiriman'].isin(instant_list)) &
                ((df_raw[track_col].isna()) | (df_raw[track_col].astype(str).str.strip() == '') | (df_raw[track_col].astype(str) == 'nan'))
            ].copy()
            
            # Mapping kolom Shopee
            col_sku = 'Nomor Referensi SKU'
            col_qty = 'Jumlah'
            col_order = 'No. Pesanan'
            col_resi = 'No. Resi'
            col_kurir = 'Opsi Pengiriman'

        if df_filtered.empty:
            continue

        # 3. EXPANSION (BUNDLE -> COMPONENT)
        for _, row in df_filtered.iterrows():
            raw_sku = str(row.get(col_sku, ''))
            sku_clean = clean_sku(raw_sku)
            
            # Handle qty (string to float)
            try:
                qty_order = float(str(row.get(col_qty, 0)).replace(',', '.'))
            except:
                qty_order = 0
            
            # Info tambahan
            resi_val = row.get(col_resi, '')
            kurir_val = row.get(col_kurir, '')

            # LOGIC PECAH BUNDLE
            if sku_clean in bundle_map:
                # Ini Bundle, pecah jadi komponen
                components = bundle_map[sku_clean]
                for comp_sku, comp_qty_unit in components:
                    total_qty = qty_order * comp_qty_unit
                    p_name = sku_name_map.get(comp_sku, comp_sku) # Lookup nama
                    
                    all_rows.append({
                        'Marketplace': mp_type,
                        'Order ID': row[col_order],
                        'Status': 'Perlu Dikirim',
                        'Kurir': kurir_val,
                        'Resi': resi_val,
                        'SKU Original': raw_sku,
                        'Is Bundle': 'Yes',
                        'SKU Component': comp_sku,
                        'Nama Produk (Master)': p_name,
                        'Qty Total': total_qty
                    })
            else:
                # Bukan Bundle (Single Item)
                p_name = sku_name_map.get(sku_clean, sku_clean) # Lookup nama
                
                all_rows.append({
                    'Marketplace': mp_type,
                    'Order ID': row[col_order],
                    'Status': 'Perlu Dikirim',
                    'Kurir': kurir_val,
                    'Resi': resi_val,
                    'SKU Original': raw_sku,
                    'Is Bundle': 'No',
                    'SKU Component': sku_clean,
                    'Nama Produk (Master)': p_name,
                    'Qty Total': qty_order
                })

    # 4. FINAL OUTPUT
    if not all_rows: return None
    
    df_detail = pd.DataFrame(all_rows)
    
    # Summary per SKU Component
    df_summary = df_detail.groupby(['Marketplace', 'SKU Component', 'Nama Produk (Master)']).agg({
        'Qty Total': 'sum'
    }).reset_index().sort_values('Qty Total', ascending=False)
    
    return {'detail': df_detail, 'summary': df_summary}

# --- UI STREAMLIT ---
st.sidebar.header("1. Upload Kamus (Wajib)")
kamus_file = st.sidebar.file_uploader("Kamus Dashboard.xlsx", type=['xlsx'])

st.sidebar.header("2. Upload Order")
shp_file = st.sidebar.file_uploader("Data-Order (Shopee)", type=['csv', 'xlsx'])
tok_file = st.sidebar.file_uploader("Untuk Dikirim (Tokopedia)", type=['csv', 'xlsx'])

if st.sidebar.button("🚀 PROSES DATA"):
    if not kamus_file:
        st.error("Upload file Kamus dulu bro!")
    elif not shp_file and not tok_file:
        st.error("Upload minimal satu file order (Shopee atau Tokopedia)!")
    else:
        with st.spinner("Wait bro, lagi parsing data..."):
            try:
                # Load Kamus
                excel_kamus = pd.ExcelFile(kamus_file)
                kamus_data = {
                    'kurir': pd.read_excel(excel_kamus, sheet_name='Kurir-Shopee'),
                    'bundle': pd.read_excel(excel_kamus, sheet_name='Bundle Master'),
                    'sku': pd.read_excel(excel_kamus, sheet_name='SKU Master')
                }
                
                files = []
                if shp_file: files.append(('Shopee', shp_file))
                if tok_file: files.append(('Tokopedia', tok_file))
                
                res = process_data(files, kamus_data)
                
                if res:
                    st.success(f"Selesai! Total Item: {res['summary']['Qty Total'].sum()}")
                    st.session_state.res = res
                else:
                    st.warning("Data terbaca, tapi TIDAK ADA yang lolos filter (Mungkin statusnya bukan 'Perlu Dikirim'?)")
                    
            except Exception as e:
                st.error(f"Error sistem: {e}")

# --- DISPLAY HASIL ---
if 'res' in st.session_state:
    res = st.session_state.res
    
    tab1, tab2 = st.tabs(["📋 Detail Order", "📦 Total per SKU"])
    
    with tab1:
        st.dataframe(res['detail'], use_container_width=True)
        
    with tab2:
        st.dataframe(res['summary'], use_container_width=True)
        
    # Download Button
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        res['detail'].to_excel(writer, sheet_name='Picking List', index=False)
        res['summary'].to_excel(writer, sheet_name='Summary Stock', index=False)
        
    st.download_button(
        label="📥 Download Excel Hasil",
        data=output.getvalue(),
        file_name=f"Picking_List_{datetime.now().strftime('%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
