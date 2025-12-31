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
# 1. CORE FUNCTIONS (LOGIC DARI COLAB)
# ==========================================

def clean_sku(sku):
    """Konversi ke string, strip, dan ambil bagian kanan hyphen."""
    if pd.isna(sku): return ""
    sku = str(sku).strip()
    # Hapus karakter aneh
    sku = ''.join(char for char in sku if ord(char) >= 32)
    if '-' in sku:
        return sku.split('-', 1)[-1].strip()
    return sku

def clean_df_strings(df):
    """Membersihkan whitespace pada seluruh dataframe"""
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
    return df

def standardize_shopee_data(df):
    """Mapping kolom Shopee"""
    # Bersihkan nama kolom (strip spasi)
    df.columns = df.columns.str.strip()
    
    col_map = {
        'No. Pesanan': 'order_id',
        'Status Pesanan': 'status',
        'Pesanan yang Dikelola Shopee': 'managed_by_platform',
        'Opsi Pengiriman': 'shipping_option',
        'No. Resi': 'tracking_id',
        'Nomor Referensi SKU': 'sku_reference',
        'SKU Induk': 'parent_sku',
        'Nama Produk': 'product_name_raw',
        'Jumlah': 'quantity',
        'Pesanan Harus Dikirimkan Sebelum (Menghindari keterlambatan)': 'deadline'
    }
    # Rename hanya kolom yang ada
    df = df.rename(columns=col_map)
    df['marketplace'] = 'Shopee'
    
    # Pastikan kolom vital ada
    for col in ['tracking_id', 'managed_by_platform']:
        if col not in df.columns: df[col] = ''
        
    return df

def standardize_tokped_data(df):
    """Mapping kolom Tokopedia"""
    df.columns = df.columns.str.strip()
    
    # Tokopedia sering berubah format, kita handle beberapa kemungkinan
    col_map = {
        'Nomor Invoice': 'order_id',
        'Order ID': 'order_id',
        'Status Pesanan': 'status',
        'Order Status': 'status',
        'Nomor SKU': 'sku_reference',
        'SKU': 'sku_reference', 
        'Seller SKU': 'sku_reference',
        'Nama Produk': 'product_name_raw',
        'Jumlah Produk': 'quantity',
        'Quantity': 'quantity',
        'No. Resi / Kode Booking': 'tracking_id',
        'No. Resi': 'tracking_id',
        'Kurir': 'shipping_option',
        'Pengiriman': 'shipping_option'
    }
    df = df.rename(columns=col_map)
    df['marketplace'] = 'Tokopedia'
    df['managed_by_platform'] = 'No' # Default No
    
    # Normalize Tracking ID (Tokped sering isi '-' atau spasi jika kosong)
    if 'tracking_id' in df.columns:
        df['tracking_id'] = df['tracking_id'].replace({'-': '', 'nan': '', 'None': ''})
    else:
        df['tracking_id'] = ''
        
    return df

def standardize_tiktok_data(df):
    """Mapping kolom TikTok"""
    df.columns = df.columns.str.strip()
    col_map = {
        'Order ID': 'order_id',
        'Order Status': 'status',
        'Seller SKU': 'sku_reference',
        'Product Name': 'product_name_raw',
        'Quantity': 'quantity',
        'Tracking ID': 'tracking_id',
        'Shipping Provider Name': 'shipping_option'
    }
    df = df.rename(columns=col_map)
    df['marketplace'] = 'TikTok'
    df['managed_by_platform'] = 'No'
    return df

# ==========================================
# 2. MAIN PROCESSING LOGIC
# ==========================================

def process_data(uploaded_files, kamus_data):
    start_time = time.time()
    
    # --- 1. PREPARE KAMUS ---
    df_kurir = clean_df_strings(kamus_data['kurir'])
    df_bundle = clean_df_strings(kamus_data['bundle'])
    df_sku = clean_df_strings(kamus_data['sku'])
    
    # Pre-clean Key Kamus (PENTING!)
    if 'SKU Bundle' in df_bundle.columns:
        df_bundle['SKU Bundle'] = df_bundle['SKU Bundle'].apply(clean_sku)
        
    # Standardize SKU Master Columns (Asumsi kolom 1=Kode, Kolom 2=Nama)
    df_sku = df_sku.rename(columns={df_sku.columns[0]: 'SKU Component', df_sku.columns[1]: 'Product Name Master'})
    df_sku['SKU Component'] = df_sku['SKU Component'].apply(clean_sku)
    
    # Get Instant Couriers List
    instant_options = []
    if 'Instant/Same Day' in df_kurir.columns:
        # Ambil kolom pertama sebagai nama kurir
        kurir_col = df_kurir.columns[0]
        instant_options = df_kurir[
            df_kurir['Instant/Same Day'].str.upper().isin(['YES', 'YA', 'TRUE', '1'])
        ][kurir_col].unique().tolist()

    all_expanded_rows = []
    
    # --- 2. LOOP SETIAP FILE UPLOAD ---
    for mp_type, file_obj in uploaded_files:
        # Baca File
        try:
            if file_obj.name.endswith('.csv'):
                df_raw = pd.read_csv(file_obj, dtype=str)
            else:
                df_raw = pd.read_excel(file_obj, dtype=str)
        except Exception as e:
            st.error(f"Gagal membaca file {file_obj.name}: {e}")
            continue
            
        # Standardize based on Marketplace
        if mp_type == 'Shopee':
            df_std = standardize_shopee_data(df_raw)
            # FILTER KHUSUS SHOPEE
            df_filtered = df_std[
                (df_std['status'].str.upper() == 'PERLU DIKIRIM') &
                (df_std['managed_by_platform'].str.upper().isin(['NO', 'TIDAK'])) &
                (df_std['shipping_option'].isin(instant_options)) &
                ((df_std['tracking_id'].isna()) | (df_std['tracking_id'] == '') | (df_std['tracking_id'] == 'nan'))
            ].copy()
            
        elif mp_type == 'Tokopedia':
            df_std = standardize_tokped_data(df_raw)
            # FILTER TOKPED (Fokus ke Resi Kosong & Status Baru/Siap Dikirim)
            # Sesuaikan status tokped jika perlu (e.g. 'Pesanan Baru', 'Siap Dikirim')
            valid_status = ['Pesanan Baru', 'Siap Dikirim', 'New Order', 'Ready to Ship'] 
            # Jika status tidak standar, kita filter by Resi Kosong saja sebagai safety
            df_filtered = df_std[
                ((df_std['tracking_id'].isna()) | (df_std['tracking_id'] == '') | (df_std['tracking_id'] == 'nan'))
            ].copy()
            
        elif mp_type == 'TikTok':
            df_std = standardize_tiktok_data(df_raw)
            df_filtered = df_std[
                (df_std['status'].str.upper() == 'AWAITING SHIPMENT') &
                ((df_std['tracking_id'].isna()) | (df_std['tracking_id'] == '') | (df_std['tracking_id'] == 'nan'))
            ].copy()
            
        if df_filtered.empty:
            continue
            
        # --- 3. BUNDLE EXPANSION LOGIC ---
        # Clean SKU dari Order
        df_filtered['sku_clean'] = df_filtered['sku_reference'].apply(clean_sku)
        
        # Convert Quantity to int/float
        df_filtered['quantity'] = pd.to_numeric(df_filtered['quantity'], errors='coerce').fillna(0)
        
        # Get List of Bundle SKUs for fast checking
        bundle_skus = set(df_bundle['SKU Bundle'].unique())
        
        for _, row in df_filtered.iterrows():
            sku_key = row['sku_clean']
            qty_order = row['quantity']
            
            if sku_key in bundle_skus:
                # EXPAND BUNDLE
                components = df_bundle[df_bundle['SKU Bundle'] == sku_key]
                for _, comp in components.iterrows():
                    # Ambil qty komponen (handle column name variations)
                    comp_qty_col = [c for c in df_bundle.columns if 'quantity' in c.lower() or 'jumlah' in c.lower()]
                    comp_qty = float(comp[comp_qty_col[0]]) if comp_qty_col else 1
                    
                    comp_sku_col = [c for c in df_bundle.columns if 'component' in c.lower() or 'komponen' in c.lower()]
                    comp_sku = clean_sku(comp[comp_sku_col[0]]) if comp_sku_col else ""

                    all_expanded_rows.append({
                        'Marketplace': row['marketplace'],
                        'Order ID': row['order_id'],
                        'Status': row['status'],
                        'Shipping Option': row['shipping_option'],
                        'SKU Original': row['sku_reference'],
                        'SKU Original (Cleaned)': sku_key,
                        'Is Bundle?': 'Yes',
                        'SKU Component': comp_sku,
                        'Total Qty': qty_order * comp_qty
                    })
            else:
                # SINGLE ITEM
                all_expanded_rows.append({
                    'Marketplace': row['marketplace'],
                    'Order ID': row['order_id'],
                    'Status': row['status'],
                    'Shipping Option': row['shipping_option'],
                    'SKU Original': row['sku_reference'],
                    'SKU Original (Cleaned)': sku_key,
                    'Is Bundle?': 'No',
                    'SKU Component': sku_key,
                    'Total Qty': qty_order
                })

    if not all_expanded_rows:
        return {'error': '❌ Tidak ada data yang memenuhi kriteria filter (Resi Kosong & Instant/Priority).'}

    df_result = pd.DataFrame(all_expanded_rows)
    
    # --- 4. LOOKUP PRODUCT NAMES (MERGE) ---
    # Merge for Component Name
    df_result = pd.merge(
        df_result, 
        df_sku[['SKU Component', 'Product Name Master']], 
        on='SKU Component', 
        how='left'
    )
    df_result = df_result.rename(columns={'Product Name Master': 'Component Name'})
    df_result['Component Name'] = df_result['Component Name'].fillna(df_result['SKU Component']) # Fallback
    
    # Merge for Original Product Name
    df_result = pd.merge(
        df_result,
        df_sku[['SKU Component', 'Product Name Master']],
        left_on='SKU Original (Cleaned)',
        right_on='SKU Component',
        how='left',
        suffixes=('', '_orig')
    )
    df_result = df_result.rename(columns={'Product Name Master': 'Product Name Original'})
    df_result.drop(columns=['SKU Component_orig'], inplace=True)

    # --- 5. CREATE OUTPUTS ---
    
    # Output 1: Picking List Detail
    # Reorder columns
    cols_order = [
        'Marketplace', 'Order ID', 'Status', 'Shipping Option', 
        'SKU Original', 'Product Name Original', 'Is Bundle?', 
        'SKU Component', 'Component Name', 'Total Qty'
    ]
    # Keep only columns that exist
    final_cols = [c for c in cols_order if c in df_result.columns]
    df_detail = df_result[final_cols]
    
    # Output 2: Summary per SKU
    df_summary = df_result.groupby(['Marketplace', 'SKU Component', 'Component Name']).agg({
        'Total Qty': 'sum'
    }).reset_index().sort_values('Total Qty', ascending=False)
    
    return {
        'detail': df_detail,
        'summary': df_summary,
        'time': time.time() - start_time,
        'total_orders': df_result['Order ID'].nunique(),
        'total_items': len(df_result)
    }

# ==========================================
# 3. SIDEBAR & UI
# ==========================================

st.sidebar.header("📁 Upload Data")

# 1. Upload Kamus
st.sidebar.subheader("1. File Kamus Master")
kamus_file = st.sidebar.file_uploader("Upload Kamus.xlsx", type=['xlsx'])

# 2. Marketplace Selector
st.sidebar.subheader("2. File Order Marketplace")
mp_files = []

if st.sidebar.checkbox("Shopee", value=True):
    f = st.sidebar.file_uploader("Order Shopee", type=['csv', 'xlsx'], key='shp')
    if f: mp_files.append(('Shopee', f))

if st.sidebar.checkbox("Tokopedia", value=False):
    f = st.sidebar.file_uploader("Order Tokopedia", type=['csv', 'xlsx'], key='tok')
    if f: mp_files.append(('Tokopedia', f))

if st.sidebar.checkbox("TikTok", value=False):
    f = st.sidebar.file_uploader("Order TikTok", type=['csv', 'xlsx'], key='tt')
    if f: mp_files.append(('TikTok', f))

st.sidebar.divider()

if st.sidebar.button("🚀 PROSES DATA", type="primary", use_container_width=True):
    if not kamus_file:
        st.error("⚠️ File Kamus Master wajib diupload!")
    elif not mp_files:
        st.error("⚠️ Minimal upload satu file order!")
    else:
        with st.spinner("Sedang memproses..."):
            try:
                # Load Kamus Once
                excel_kamus = pd.ExcelFile(kamus_file)
                kamus_dict = {
                    'kurir': pd.read_excel(kamus_file, sheet_name='Kurir-Shopee'),
                    'bundle': pd.read_excel(kamus_file, sheet_name='Bundle Master'),
                    'sku': pd.read_excel(kamus_file, sheet_name='SKU Master')
                }
                
                # Process
                res = process_data(mp_files, kamus_dict)
                
                if 'error' in res:
                    st.error(res['error'])
                else:
                    st.session_state.processed = True
                    st.session_state.results = res
                    st.rerun() # Refresh page to show results
                    
            except Exception as e:
                st.error(f"Terjadi kesalahan: {e}")
                st.exception(e)

# ==========================================
# 4. DISPLAY RESULTS
# ==========================================

if st.session_state.processed and 'results' in st.session_state:
    res = st.session_state.results
    
    # Summary Metrics
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Order (Resi Blank)", res['total_orders'])
    c2.metric("Total Baris Item", res['total_items'])
    c3.metric("Waktu Proses", f"{res['time']:.2f} detik")
    
    st.divider()
    
    tab1, tab2, tab3 = st.tabs(["📋 Detail Order (Picking List)", "📦 Grand Total SKU (Stock Check)", "📥 Download Excel"])
    
    with tab1:
        st.write("Daftar pesanan yang perlu disiapkan (belum ada Resi):")
        st.dataframe(res['detail'], use_container_width=True)
        
    with tab2:
        st.write("Rekap total qty per SKU Component:")
        st.dataframe(res['summary'], use_container_width=True)
        
    with tab3:
        st.subheader("Download Hasil Proses")
        
        # Create Excel in Memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            res['detail'].to_excel(writer, sheet_name='Detail Order', index=False)
            res['summary'].to_excel(writer, sheet_name='Grand Total SKU', index=False)
            
            # Auto-adjust columns width (optional cosmetic)
            worksheet1 = writer.sheets['Detail Order']
            worksheet1.set_column('A:J', 20)
            
        st.download_button(
            label="📥 DOWNLOAD EXCEL FILE",
            data=output.getvalue(),
            file_name=f"Picking_List_Universal_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary"
        )
