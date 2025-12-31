import streamlit as st
import pandas as pd
import numpy as np
import io
import time
from datetime import datetime

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Universal Order Processor",
    page_icon="🛒",
    layout="wide"
)

# --- CUSTOM CSS ---
st.markdown("""
    <style>
    .stApp { background-color: #f8f9fa; }
    div[data-testid="stMetricValue"] { font-size: 24px; }
    </style>
""", unsafe_allow_html=True)

# --- TITLE ---
st.title("🛒 Universal Marketplace Order Processor")
st.markdown("Proses order **Shopee** & **Tokopedia/TikTok** dengan logic Instant/Same Day & Bundle Expansion.")
st.markdown("---")

# --- INITIALIZE STATE ---
if 'processed' not in st.session_state:
    st.session_state.processed = False
if 'results' not in st.session_state:
    st.session_state.results = {}

# ==========================================
# 1. CORE FUNCTIONS
# ==========================================

def clean_sku(sku):
    """Konversi ke string, strip, dan ambil bagian kanan hyphen."""
    if pd.isna(sku): return ""
    sku = str(sku).strip()
    sku = ''.join(char for char in sku if ord(char) >= 32)
    if '-' in sku:
        return sku.split('-', 1)[-1].strip()
    return sku

def clean_df_strings(df):
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
    return df

def smart_rename_columns(df, target_map):
    """
    Rename kolom dengan pencocokan fleksibel (ignore case, ignore space).
    target_map: {'target_col': ['possible_name1', 'possible_name2']}
    """
    # Buat dictionary map dari clean_name -> original_name
    clean_cols = {c.lower().replace(' ', '').replace('_', '').replace('.', ''): c for c in df.columns}
    
    rename_dict = {}
    found_cols = []
    
    for target, candidates in target_map.items():
        match_found = False
        for cand in candidates:
            cand_clean = cand.lower().replace(' ', '').replace('_', '').replace('.', '')
            if cand_clean in clean_cols:
                original_name = clean_cols[cand_clean]
                rename_dict[original_name] = target
                found_cols.append(target)
                match_found = True
                break
    
    if rename_dict:
        df = df.rename(columns=rename_dict)
        
    return df, found_cols

def standardize_shopee_data(df):
    # Mapping Target -> List Kemungkinan Nama Kolom
    target_map = {
        'order_id': ['No. Pesanan', 'Order ID', 'No Pesanan'],
        'status': ['Status Pesanan', 'Status'],
        'managed_by_platform': ['Pesanan yang Dikelola Shopee', 'Dikelola Shopee'],
        'shipping_option': ['Opsi Pengiriman', 'Jasa Kirim'],
        'tracking_id': ['No. Resi', 'Tracking Number'],
        'sku_reference': ['Nomor Referensi SKU', 'SKU Induk', 'SKU Reference'],
        'product_name_raw': ['Nama Produk', 'Product Name'],
        'quantity': ['Jumlah', 'Quantity'],
        'deadline': ['Pesanan Harus Dikirimkan Sebelum', 'Ship By Date']
    }
    
    df, _ = smart_rename_columns(df, target_map)
    df['marketplace'] = 'Shopee'
    
    # Safety columns
    for col in ['tracking_id', 'managed_by_platform', 'shipping_option']:
        if col not in df.columns: df[col] = ''
    return df

def standardize_tokped_data(df):
    # Mapping Target -> List Kemungkinan Nama Kolom
    target_map = {
        'order_id': ['Order ID', 'Nomor Invoice', 'Invoice'],
        'status': ['Order Status', 'Status Pesanan', 'Status'],
        'sku_reference': ['Seller SKU', 'Nomor SKU', 'SKU'],
        'product_name_raw': ['Product Name', 'Nama Produk'],
        'quantity': ['Quantity', 'Jumlah Produk', 'Jumlah'],
        'shipping_option': ['Delivery Option', 'Pengiriman', 'Kurir'],
        'tracking_id': ['Tracking ID', 'No. Resi', 'No Resi', 'Kode Booking']
    }
    
    df, found = smart_rename_columns(df, target_map)
    df['marketplace'] = 'Tokopedia'
    
    # Safety columns
    if 'managed_by_platform' not in df.columns: df['managed_by_platform'] = 'No'
    if 'tracking_id' not in df.columns: df['tracking_id'] = ''
    if 'shipping_option' not in df.columns: df['shipping_option'] = '' 
    
    # Cek apakah status ketemu
    if 'status' not in df.columns:
        # Debugging info
        return df, False, list(df.columns)
        
    return df, True, []

def standardize_tiktok_data(df):
    target_map = {
        'order_id': ['Order ID'],
        'status': ['Order Status', 'Status'],
        'sku_reference': ['Seller SKU', 'SKU'],
        'product_name_raw': ['Product Name', 'Nama Produk'],
        'quantity': ['Quantity', 'Jumlah'],
        'tracking_id': ['Tracking ID', 'No Resi'],
        'shipping_option': ['Shipping Provider Name', 'Delivery Option', 'Kurir']
    }
    
    df, _ = smart_rename_columns(df, target_map)
    df['marketplace'] = 'TikTok'
    df['managed_by_platform'] = 'No'
    if 'shipping_option' not in df.columns: df['shipping_option'] = ''
    return df

# ==========================================
# 2. MAIN PROCESSING LOGIC
# ==========================================

def process_data(uploaded_files, kamus_data):
    start_time = time.time()
    
    # Prepare Kamus
    df_kurir = clean_df_strings(kamus_data['kurir'])
    df_bundle = clean_df_strings(kamus_data['bundle'])
    df_sku = clean_df_strings(kamus_data['sku'])
    
    if 'SKU Bundle' in df_bundle.columns:
        df_bundle['SKU Bundle'] = df_bundle['SKU Bundle'].apply(clean_sku)
    
    # SKU Master Lookup
    try:
        if len(df_sku.columns) >= 3:
            df_sku_subset = df_sku.iloc[:, [1, 2]].copy()
            df_sku_subset.columns = ['SKU Component', 'Product Name Master']
        else:
            df_sku_subset = df_sku.iloc[:, [0, 1]].copy()
            df_sku_subset.columns = ['SKU Component', 'Product Name Master']
        df_sku_subset['SKU Component'] = df_sku_subset['SKU Component'].apply(clean_sku)
    except Exception as e:
        return {'error': f"❌ Error SKU Master: {e}"}

    # Instant Options
    instant_options = []
    if 'Instant/Same Day' in df_kurir.columns:
        kurir_col = df_kurir.columns[0]
        instant_options = df_kurir[
            df_kurir['Instant/Same Day'].str.upper().isin(['YES', 'YA', 'TRUE', '1'])
        ][kurir_col].astype(str).str.strip().unique().tolist()

    all_expanded_rows = []
    
    for mp_type, file_obj in uploaded_files:
        try:
            # --- SUPER ROBUST FILE READER ---
            # Mencoba membaca file dengan berbagai engine dan separator
            file_content = file_obj.getvalue()
            df_raw = None
            
            # Coba baca sebagai Excel dulu
            try:
                df_raw = pd.read_excel(io.BytesIO(file_content), dtype=str)
            except:
                pass
            
            # Jika gagal, coba baca sebagai CSV (koma)
            if df_raw is None:
                try:
                    df_raw = pd.read_csv(io.BytesIO(file_content), sep=',', dtype=str)
                    if len(df_raw.columns) < 2: raise ValueError("Not comma")
                except:
                    # Jika gagal, coba baca sebagai CSV (titik koma)
                    try:
                        df_raw = pd.read_csv(io.BytesIO(file_content), sep=';', dtype=str)
                    except:
                        st.error(f"Gagal membaca format file {file_obj.name}. Pastikan file Excel/CSV valid.")
                        continue

            # --- HEADER CLEANING ---
            # Deteksi jika baris pertama adalah deskripsi (Platform unique...)
            if len(df_raw) > 0:
                # Cek 3 baris pertama untuk mencari Header "Order Status" atau "Order ID"
                header_idx = -1
                for i in range(min(5, len(df_raw))):
                    row_str = str(df_raw.iloc[i].values).lower()
                    if 'order status' in row_str or 'status pesanan' in row_str or 'order id' in row_str:
                        header_idx = i
                        break
                
                if header_idx > 0:
                    # Reset header ke baris yang benar
                    df_raw.columns = df_raw.iloc[header_idx]
                    df_raw = df_raw.iloc[header_idx+1:].reset_index(drop=True)
                elif header_idx == 0:
                    # Header sudah benar di baris 0
                    pass
                else:
                    # Header tidak ketemu di 5 baris pertama, coba pakai baris 0 (default)
                    pass

        except Exception as e:
            st.error(f"Error fatal saat membaca file {file_obj.name}: {e}")
            continue
            
        # Standardize & Filter
        if mp_type == 'Shopee':
            df_std = standardize_shopee_data(df_raw)
            if 'status' not in df_std.columns:
                return {'error': f"Kolom 'Status Pesanan' tidak terdeteksi di file Shopee. Kolom terbaca: {list(df_std.columns)}"}
                
            df_filtered = df_std[
                (df_std['status'].astype(str).str.upper() == 'PERLU DIKIRIM') &
                (df_std['managed_by_platform'].astype(str).str.upper().isin(['NO', 'TIDAK'])) &
                (df_std['shipping_option'].isin(instant_options)) &
                ((df_std['tracking_id'].isna()) | (df_std['tracking_id'].astype(str) == '') | (df_std['tracking_id'].astype(str) == 'nan'))
            ].copy()
            
        elif mp_type == 'Tokopedia':
            df_std, success, cols = standardize_tokped_data(df_raw)
            if not success:
                return {'error': f"Kolom 'Order Status' tidak terdeteksi di file Tokopedia. Kolom terbaca: {cols}. Pastikan header file benar."}
                
            # Filter Tokped: Case Insensitive 'Perlu dikirim'
            df_filtered = df_std[
                (df_std['status'].astype(str).str.strip().str.lower() == 'perlu dikirim')
            ].copy()
            
        elif mp_type == 'TikTok':
            df_std = standardize_tiktok_data(df_raw)
            if 'status' not in df_std.columns:
                return {'error': f"Kolom 'Order Status' tidak terdeteksi di file TikTok."}
                
            df_filtered = df_std[
                (df_std['status'].astype(str).str.upper().isin(['AWAITING SHIPMENT', 'AWAITING COLLECTION'])) &
                ((df_std['tracking_id'].isna()) | (df_std['tracking_id'].astype(str) == '') | (df_std['tracking_id'].astype(str) == 'nan'))
            ].copy()
            
        if df_filtered.empty:
            continue
            
        # Bundle Expansion
        df_filtered['sku_clean'] = df_filtered['sku_reference'].apply(clean_sku)
        df_filtered['quantity'] = pd.to_numeric(df_filtered['quantity'], errors='coerce').fillna(0)
        
        bundle_skus = set(df_bundle['SKU Bundle'].unique())
        
        for _, row in df_filtered.iterrows():
            sku_key = row['sku_clean']
            qty_order = row['quantity']
            ship_opt = str(row.get('shipping_option', ''))
            
            if sku_key in bundle_skus:
                components = df_bundle[df_bundle['SKU Bundle'] == sku_key]
                for _, comp in components.iterrows():
                    comp_qty_col = [c for c in df_bundle.columns if 'quantity' in c.lower() or 'jumlah' in c.lower()]
                    comp_qty = float(comp[comp_qty_col[0]]) if comp_qty_col else 1
                    
                    comp_sku_col = [c for c in df_bundle.columns if 'component' in c.lower() or 'komponen' in c.lower()]
                    comp_sku = clean_sku(comp[comp_sku_col[0]]) if comp_sku_col else ""

                    all_expanded_rows.append({
                        'Marketplace': row['marketplace'],
                        'Order ID': row['order_id'],
                        'Status': row['status'],
                        'Shipping Option': ship_opt,
                        'SKU Original': row['sku_reference'],
                        'SKU Original (Cleaned)': sku_key,
                        'Is Bundle?': 'Yes',
                        'SKU Component': comp_sku,
                        'Total Qty': qty_order * comp_qty
                    })
            else:
                all_expanded_rows.append({
                    'Marketplace': row['marketplace'],
                    'Order ID': row['order_id'],
                    'Status': row['status'],
                    'Shipping Option': ship_opt,
                    'SKU Original': row['sku_reference'],
                    'SKU Original (Cleaned)': sku_key,
                    'Is Bundle?': 'No',
                    'SKU Component': sku_key,
                    'Total Qty': qty_order
                })

    if not all_expanded_rows:
        return {'error': '❌ Tidak ada data yang memenuhi kriteria filter (Status Perlu Dikirim / No Resi Kosong).'}

    df_result = pd.DataFrame(all_expanded_rows)
    
    # Lookup Product Names
    df_result = pd.merge(df_result, df_sku_subset, on='SKU Component', how='left')
    df_result = df_result.rename(columns={'Product Name Master': 'Component Name'})
    df_result['Component Name'] = df_result['Component Name'].fillna(df_result['SKU Component'])
    
    df_result = pd.merge(df_result, df_sku_subset, left_on='SKU Original (Cleaned)', right_on='SKU Component', how='left', suffixes=('', '_orig'))
    df_result = df_result.rename(columns={'Product Name Master': 'Product Name Original'})
    if 'SKU Component_orig' in df_result.columns: df_result.drop(columns=['SKU Component_orig'], inplace=True)

    # Outputs
    cols_order = ['Marketplace', 'Order ID', 'Status', 'Shipping Option', 'SKU Original', 'Product Name Original', 'Is Bundle?', 'SKU Component', 'Component Name', 'Total Qty']
    final_cols = [c for c in cols_order if c in df_result.columns]
    df_detail = df_result[final_cols]
    
    df_summary = df_result.groupby(['Marketplace', 'SKU Component', 'Component Name']).agg({'Total Qty': 'sum'}).reset_index().sort_values('Total Qty', ascending=False)
    
    return {'detail': df_detail, 'summary': df_summary, 'time': time.time() - start_time, 'total_orders': df_result['Order ID'].nunique(), 'total_items': len(df_result)}

# ==========================================
# 3. SIDEBAR & UI
# ==========================================

st.sidebar.header("📁 Upload Data")
kamus_file = st.sidebar.file_uploader("1. Upload Kamus.xlsx", type=['xlsx'])

st.sidebar.subheader("2. File Order")
mp_files = []
if st.sidebar.checkbox("Shopee", value=True):
    f = st.sidebar.file_uploader("Order Shopee", type=['csv', 'xlsx'], key='shp')
    if f: mp_files.append(('Shopee', f))

if st.sidebar.checkbox("Tokopedia", value=True):
    f = st.sidebar.file_uploader("Order Tokopedia", type=['csv', 'xlsx'], key='tok')
    if f: mp_files.append(('Tokopedia', f))

if st.sidebar.checkbox("TikTok", value=False):
    f = st.sidebar.file_uploader("Order TikTok", type=['csv', 'xlsx'], key='tt')
    if f: mp_files.append(('TikTok', f))

st.sidebar.divider()

if st.sidebar.button("🚀 PROSES DATA", type="primary", use_container_width=True):
    if not kamus_file or not mp_files:
        st.error("⚠️ Lengkapi file Kamus dan minimal 1 file Order!")
    else:
        with st.spinner("Sedang memproses..."):
            try:
                # Baca Excel Kamus dengan Engine Openpyxl
                excel_kamus = pd.ExcelFile(kamus_file, engine='openpyxl')
                kamus_dict = {
                    'kurir': pd.read_excel(excel_kamus, sheet_name='Kurir-Shopee'),
                    'bundle': pd.read_excel(excel_kamus, sheet_name='Bundle Master'),
                    'sku': pd.read_excel(excel_kamus, sheet_name='SKU Master')
                }
                res = process_data(mp_files, kamus_dict)
                
                if 'error' in res:
                    st.error(res['error'])
                else:
                    st.session_state.processed = True
                    st.session_state.results = res
                    st.rerun()
            except Exception as e:
                st.error(f"Critical Error: {e}")

# ==========================================
# 4. DISPLAY RESULTS
# ==========================================

if st.session_state.processed and 'results' in st.session_state:
    res = st.session_state.results
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Order", res['total_orders'])
    c2.metric("Total Item", res['total_items'])
    c3.metric("Waktu Proses", f"{res['time']:.2f} detik")
    
    st.divider()
    tab1, tab2, tab3 = st.tabs(["📋 Detail Order", "📦 Grand Total SKU", "📥 Download Excel"])
    
    with tab1: st.dataframe(res['detail'], use_container_width=True)
    with tab2: st.dataframe(res['summary'], use_container_width=True)
    with tab3:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            res['detail'].to_excel(writer, sheet_name='Detail Order', index=False)
            res['summary'].to_excel(writer, sheet_name='Grand Total SKU', index=False)
            writer.sheets['Detail Order'].set_column('A:J', 20)
        st.download_button("📥 DOWNLOAD EXCEL", data=output.getvalue(), file_name=f"Picking_List_{datetime.now().strftime('%H%M')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary")
