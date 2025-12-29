import streamlit as st
import pandas as pd
import numpy as np
import io
import time
from datetime import datetime
import plotly.express as px

# Page config
st.set_page_config(
    page_title="Universal Order Processor",
    page_icon="🛒",
    layout="wide"
)

# Title
st.title("🛒 Universal Marketplace Order Processor")
st.markdown("Proses order dari Shopee, Tokopedia, TikTok dalam 1 dashboard!")

# Initialize session state
if 'processed' not in st.session_state:
    st.session_state.processed = False
if 'results' not in st.session_state:
    st.session_state.results = {}
if 'marketplace_type' not in st.session_state:
    st.session_state.marketplace_type = ""

# --- FUNGSI CLEANING ---
def clean_sku(sku):
    """Konversi ke string, strip, dan ambil bagian kanan hyphen."""
    if pd.isna(sku):
        return ""
    
    sku = str(sku).strip()
    
    # Remove leading/trailing whitespace
    sku = sku.strip()
    
    # Remove non-breaking spaces and other invisible chars
    sku = ''.join(char for char in sku if ord(char) >= 32)
    
    # Jika ada hyphen, ambil bagian terakhir
    if '-' in sku:
        parts = sku.split('-')
        # Coba ambil bagian yang paling mungkin SKU
        for part in reversed(parts):
            part = part.strip()
            if part:  # Skip empty parts
                return part
    
    return sku

# --- FUNGSI DETECT MARKETPLACE ---
def detect_marketplace(df, filename=""):
    """Deteksi tipe marketplace dari dataframe atau filename"""
    
    # Cek dari nama file
    filename_lower = filename.lower()
    if 'shopee' in filename_lower:
        return 'shopee'
    elif any(x in filename_lower for x in ['tokped', 'tokopedia', 'tiktok']):
        return 'tokped_tiktok'
    
    # Cek dari kolom
    columns_lower = [col.lower() for col in df.columns]
    
    # Deteksi Shopee
    shopee_keywords = ['no. pesanan', 'status pesanan', 'pesanan yang dikelola shopee', 'opsi pengiriman']
    if any(any(kw in col for kw in shopee_keywords) for col in columns_lower):
        return 'shopee'
    
    # Deteksi Tokopedia/TikTok
    tokped_keywords = ['order id', 'seller sku', 'sku id', 'fulfillment type']
    if any(any(kw in col for kw in tokped_keywords) for col in columns_lower):
        return 'tokped_tiktok'
    
    # Default ke unknown
    return 'unknown'

# --- FUNGSI STANDARDIZE DATA ---
def standardize_shopee_data(df):
    """Standardize Shopee data format"""
    df_std = df.copy()
    
    # Mapping kolom Shopee ke standar
    column_mapping = {
        'No. Pesanan': 'order_id',
        'Status Pesanan': 'status',
        'Pesanan yang Dikelola Shopee': 'managed_by_platform',
        'Opsi Pengiriman': 'shipping_option',
        'No. Resi': 'tracking_id',
        'Nomor Referensi SKU': 'sku_reference',
        'SKU Induk': 'parent_sku',
        'Nama Produk': 'product_name',
        'Jumlah': 'quantity',
        'Pesanan Harus Dikirimkan Sebelum (Menghindari keterlambatan)': 'deadline'
    }
    
    # Rename kolom yang ada
    for old_col, new_col in column_mapping.items():
        if old_col in df_std.columns:
            df_std.rename(columns={old_col: new_col}, inplace=True)
    
    # Tambahkan kolom marketplace
    df_std['marketplace'] = 'shopee'
    
    # Pastikan kolom required ada
    required_cols = ['order_id', 'status', 'managed_by_platform', 'shipping_option', 'tracking_id', 'sku_reference', 'quantity']
    for col in required_cols:
        if col not in df_std.columns:
            df_std[col] = ""
    
    return df_std

def standardize_tokped_data(df):
    """Standardize Tokopedia/TikTok data format"""
    df_std = df.copy()
    
    # Mapping kolom Tokped/TikTok ke standar
    column_mapping = {
        'Order ID': 'order_id',
        'Order Status': 'status',
        'Seller SKU': 'sku_reference',
        'SKU ID': 'sku_id',
        'Product Name': 'product_name',
        'Quantity': 'quantity',
        'Tracking ID': 'tracking_id',
        'Fulfillment Type': 'fulfillment_type',
        'Delivery Option': 'shipping_option',
        'Created Time': 'created_time'
    }
    
    # Rename kolom yang ada
    for old_col, new_col in column_mapping.items():
        if old_col in df_std.columns:
            df_std.rename(columns={old_col: new_col}, inplace=True)
    
    # Tambahkan kolom untuk konsistensi dengan Shopee
    df_std['managed_by_platform'] = 'No'  # Tokped biasanya bukan managed
    df_std['marketplace'] = 'tokped_tiktok'
    
    # Pastikan kolom required ada
    required_cols = ['order_id', 'status', 'sku_reference', 'quantity', 'tracking_id']
    for col in required_cols:
        if col not in df_std.columns:
            df_std[col] = ""
    
    return df_std

# --- FUNGSI LOOKUP BATCH ---
@st.cache_data
def create_sku_mapping(df_sku):
    """Buat mapping dictionary dari SKU Master untuk lookup cepat"""
    sku_mapping = {}
    
    if df_sku.empty:
        return sku_mapping
    
    # Bersihkan semua kolom SKU yang mungkin
    sku_columns = []
    for col in df_sku.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['sku', 'material', 'kode', 'code']):
            sku_columns.append(col)
    
    # Juga cari kolom Product Name
    product_name_col = None
    for col in df_sku.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['description', 'name', 'nama', 'produk']):
            product_name_col = col
            break
    
    if not product_name_col:
        # Default ke kolom terakhir jika tidak ditemukan
        product_name_col = df_sku.columns[-1]
    
    # Buat mapping dari semua kemungkinan kolom SKU
    for sku_col in sku_columns:
        for idx, row in df_sku.iterrows():
            sku_value = str(row[sku_col]) if pd.notna(row[sku_col]) else ""
            product_name = str(row[product_name_col]) if pd.notna(row[product_name_col]) else ""
            
            if sku_value:
                # Clean SKU
                cleaned_sku = clean_sku(sku_value)
                if cleaned_sku and cleaned_sku not in sku_mapping:
                    sku_mapping[cleaned_sku] = product_name
    
    return sku_mapping

def lookup_product_name_batch(sku_mapping, sku_codes):
    """Batch lookup menggunakan mapping dictionary"""
    results = []
    for sku in sku_codes:
        cleaned = clean_sku(str(sku))
        results.append(sku_mapping.get(cleaned, ""))
    return results

# --- FUNGSI MEMBACA FILE KAMUS ---
def read_kamus_file(kamus_file):
    """Membaca file kamus Excel yang memiliki 3 sheet"""
    try:
        if kamus_file.name.endswith('.xlsx') or kamus_file.name.endswith('.xls'):
            # Baca semua sheet
            excel_file = pd.ExcelFile(kamus_file, engine='openpyxl')
            sheet_names = excel_file.sheet_names
            
            # Cari sheet Kurir-Shopee
            kurir_keywords = ['kurir', 'shopee', 'pengiriman', 'courier']
            kurir_sheets = [s for s in sheet_names if any(kw in s.lower() for kw in kurir_keywords)]
            
            if kurir_sheets:
                df_kurir = pd.read_excel(kamus_file, sheet_name=kurir_sheets[0], engine='openpyxl')
            else:
                df_kurir = pd.read_excel(kamus_file, sheet_name=0, engine='openpyxl')
            
            # Cari sheet Bundle Master
            bundle_keywords = ['bundle', 'bundling', 'paket']
            bundle_sheets = [s for s in sheet_names if any(kw in s.lower() for kw in bundle_keywords)]
            
            if bundle_sheets:
                df_bundle = pd.read_excel(kamus_file, sheet_name=bundle_sheets[0], engine='openpyxl')
            else:
                if len(sheet_names) > 1:
                    df_bundle = pd.read_excel(kamus_file, sheet_name=1, engine='openpyxl')
                else:
                    df_bundle = pd.read_excel(kamus_file, sheet_name=0, engine='openpyxl')
            
            # Cari sheet SKU Master
            sku_keywords = ['sku', 'master', 'produk', 'product', 'material']
            sku_sheets = [s for s in sheet_names if any(kw in s.lower() for kw in sku_keywords)]
            
            if sku_sheets:
                df_sku = pd.read_excel(kamus_file, sheet_name=sku_sheets[0], engine='openpyxl')
            else:
                if len(sheet_names) > 2:
                    df_sku = pd.read_excel(kamus_file, sheet_name=2, engine='openpyxl')
                elif len(sheet_names) > 1:
                    df_sku = pd.read_excel(kamus_file, sheet_name=1, engine='openpyxl')
                else:
                    df_sku = pd.read_excel(kamus_file, sheet_name=0, engine='openpyxl')
            
            return {
                'kurir': df_kurir,
                'bundle': df_bundle,
                'sku': df_sku
            }
            
        else:
            st.error("File kamus harus dalam format Excel (.xlsx atau .xls)")
            return None
            
    except Exception as e:
        st.error(f"Error membaca file kamus: {str(e)}")
        return None

# --- FUNGSI PROCESSING UTAMA ---
def process_universal_data(df_orders_list, kamus_data, marketplace_types):
    """Process data dari multiple marketplace"""
    
    start_time = time.time()
    all_expanded_rows = []
    
    # Prepare SKU mapping
    sku_mapping = create_sku_mapping(kamus_data['sku'])
    
    # Clean bundle data
    df_bundle = kamus_data['bundle'].copy()
    if 'SKU Bundle' in df_bundle.columns:
        df_bundle['SKU Bundle'] = df_bundle['SKU Bundle'].apply(clean_sku)
    
    # Prepare bundle mapping
    bundle_mapping = {}
    component_col = 'SKU Component' if 'SKU Component' in df_bundle.columns else df_bundle.columns[1] if len(df_bundle.columns) > 1 else None
    
    if component_col:
        for bundle_sku, group in df_bundle.groupby('SKU Bundle'):
            bundle_mapping[bundle_sku] = []
            for _, row in group.iterrows():
                component_sku = clean_sku(row[component_col])
                qty = row.get('Component Quantity', 1) if 'Component Quantity' in row else 1
                bundle_mapping[bundle_sku].append((component_sku, qty))
    
    # Prepare kurir filter untuk Shopee
    df_kurir = kamus_data['kurir'].copy()
    instant_same_day_options = []
    
    if 'Instant/Same Day' in df_kurir.columns:
        df_kurir['Opsi Pengiriman'] = df_kurir.iloc[:, 0].astype(str).str.strip()
        df_kurir['Instant/Same Day'] = df_kurir['Instant/Same Day'].astype(str).str.strip()
        
        instant_same_day_options = df_kurir[
            df_kurir['Instant/Same Day'].str.upper().str.strip().isin(['YES', 'YA', '1', 'TRUE', 'Y', 'IYA'])
        ]['Opsi Pengiriman'].unique()
    
    # Process each marketplace data
    for idx, (df_orders, marketplace_type) in enumerate(zip(df_orders_list, marketplace_types)):
        
        # Standardize data
        if marketplace_type == 'shopee':
            df_std = standardize_shopee_data(df_orders)
            
            # Filter untuk Shopee
            df_filtered = df_std[
                (df_std['status'].str.upper() == 'PERLU DIKIRIM') &
                (df_std['managed_by_platform'].str.upper().isin(['NO', 'TIDAK', 'FALSE'])) &
                (df_std['shipping_option'].isin(instant_same_day_options)) &
                ((df_std['tracking_id'] == '') | df_std['tracking_id'].isna())
            ].copy()
            
        elif marketplace_type == 'tokped_tiktok':
            df_std = standardize_tokped_data(df_orders)
            
            # Filter untuk Tokped/TikTok (default semua instant, no tracking)
            df_filtered = df_std[
                ((df_std['tracking_id'] == '') | df_std['tracking_id'].isna())
            ].copy()
        
        else:
            continue
        
        if df_filtered.empty:
            continue
        
        # Clean SKU
        df_filtered['sku_clean'] = df_filtered['sku_reference'].apply(clean_sku)
        
        # Bundle expansion
        for _, row in df_filtered.iterrows():
            sku_clean = row['sku_clean']
            order_id = row['order_id']
            quantity = row['quantity']
            marketplace = row.get('marketplace', marketplace_type)
            
            if sku_clean in bundle_mapping:
                # Bundle ditemukan
                for component_sku, comp_qty in bundle_mapping[sku_clean]:
                    all_expanded_rows.append({
                        'Marketplace': marketplace,
                        'Order ID': order_id,
                        'Status': row.get('status', ''),
                        'Shipping Option': row.get('shipping_option', ''),
                        'Original SKU': row.get('sku_reference', ''),
                        'Is Bundle': 'Yes',
                        'SKU Component': component_sku,
                        'Quantity Final': quantity * comp_qty,
                    })
            else:
                # Item satuan
                all_expanded_rows.append({
                    'Marketplace': marketplace,
                    'Order ID': order_id,
                    'Status': row.get('status', ''),
                    'Shipping Option': row.get('shipping_option', ''),
                    'Original SKU': row.get('sku_reference', ''),
                    'Is Bundle': 'No',
                    'SKU Component': sku_clean,
                    'Quantity Final': quantity * 1,
                })
    
    if not all_expanded_rows:
        return {"error": "❌ Tidak ada data yang memenuhi kriteria filter."}
    
    # Convert to DataFrame
    df_expanded = pd.DataFrame(all_expanded_rows)
    
    # Add product names
    if not df_expanded.empty:
        sku_components = df_expanded['SKU Component'].tolist()
        product_names = lookup_product_name_batch(sku_mapping, sku_components)
        df_expanded['Product Name'] = product_names
    
    # Prepare outputs
    end_time = time.time()
    processing_time = end_time - start_time
    
    # Output 1: Detail Order
    df_output1 = df_expanded.copy()
    
    # Output 2: SKU Summary (group by marketplace + sku)
    if not df_expanded.empty:
        df_output2 = df_expanded.groupby(['Marketplace', 'SKU Component', 'Product Name']).agg({
            'Quantity Final': 'sum'
        }).reset_index()
        df_output2 = df_output2.rename(columns={
            'Quantity Final': 'Total Quantity'
        }).sort_values('Total Quantity', ascending=False)
    else:
        df_output2 = pd.DataFrame(columns=['Marketplace', 'SKU Component', 'Product Name', 'Total Quantity'])
    
    # Output 3: Marketplace Summary
    marketplace_summary = []
    for marketplace_type in set(marketplace_types):
        orders_count = df_expanded[df_expanded['Marketplace'] == marketplace_type]['Order ID'].nunique()
        items_count = df_expanded[df_expanded['Marketplace'] == marketplace_type].shape[0]
        marketplace_summary.append({
            'Marketplace': marketplace_type,
            'Total Orders': orders_count,
            'Total Items': items_count
        })
    
    df_output3 = pd.DataFrame(marketplace_summary)
    
    # Count missing SKUs
    missing_skus = []
    if not df_expanded.empty:
        missing_mask = df_expanded['Product Name'].isna() | (df_expanded['Product Name'] == '')
        missing_skus = df_expanded.loc[missing_mask, 'SKU Component'].unique().tolist()
    
    return {
        'output1': df_output1,
        'output2': df_output2,
        'output3': df_output3,
        'processing_time': processing_time,
        'missing_skus': missing_skus,
        'total_orders': df_expanded['Order ID'].nunique(),
        'total_items': len(df_expanded),
        'marketplaces': list(set(marketplace_types))
    }

# --- MAIN UI ---
st.sidebar.header("📁 Upload Files")

# Marketplace selection
st.sidebar.subheader("1. Pilih Marketplace")
marketplace_options = st.sidebar.multiselect(
    "Pilih marketplace yang akan diproses:",
    ["Shopee", "Tokopedia/TikTok"],
    default=["Shopee", "Tokopedia/TikTok"]
)

uploaded_files = {}
marketplace_types = []

# File upload sections
st.sidebar.subheader("2. Upload File Order")

if "Shopee" in marketplace_options:
    shopee_file = st.sidebar.file_uploader(
        "📦 File Order Shopee",
        type=['csv', 'xlsx', 'xls'],
        key="shopee"
    )
    if shopee_file:
        uploaded_files['shopee'] = shopee_file
        marketplace_types.append('shopee')

if "Tokopedia/TikTok" in marketplace_options:
    tokped_file = st.sidebar.file_uploader(
        "🛍️ File Order Tokopedia/TikTok",
        type=['csv', 'xlsx', 'xls'],
        key="tokped"
    )
    if tokped_file:
        uploaded_files['tokped_tiktok'] = tokped_file
        marketplace_types.append('tokped_tiktok')

st.sidebar.subheader("3. Upload File Kamus")
kamus_file = st.sidebar.file_uploader(
    "📚 File Kamus Master (Excel)",
    type=['xlsx', 'xls'],
    key="kamus"
)

st.sidebar.divider()

# Process button
if uploaded_files and kamus_file and marketplace_types:
    if st.sidebar.button("🚀 PROCESS ALL DATA", type="primary", width='stretch'):
        with st.spinner("Memproses data dari semua marketplace..."):
            try:
                # Read all order files
                df_orders_list = []
                processed_marketplace_types = []
                
                for mtype, file in uploaded_files.items():
                    if file.name.endswith('.csv'):
                        df = pd.read_csv(file)
                    else:
                        df = pd.read_excel(file, engine='openpyxl')
                    
                    df_orders_list.append(df)
                    processed_marketplace_types.append(mtype)
                
                # Read kamus file
                kamus_data = read_kamus_file(kamus_file)
                
                if kamus_data:
                    # Process data
                    results = process_universal_data(df_orders_list, kamus_data, processed_marketplace_types)
                    
                    if results and "error" not in results:
                        st.session_state.results = results
                        st.session_state.processed = True
                        st.session_state.marketplace_type = "universal"
                        st.success("✅ Semua data berhasil diproses!")
                        st.rerun()
                    elif results and "error" in results:
                        st.error(results["error"])
                    else:
                        st.error("❌ Processing failed")
            
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                import traceback
                with st.expander("Error Details"):
                    st.code(traceback.format_exc())
else:
    st.sidebar.button("🚀 PROCESS ALL DATA", type="primary", width='stretch', disabled=True)

st.sidebar.divider()
st.sidebar.caption("🌐 Universal Processor v1.0")

# Display results
if st.session_state.processed and 'results' in st.session_state:
    results = st.session_state.results
    
    # Header
    st.success(f"✅ Processing completed in {results.get('processing_time', 0):.1f}s")
    
    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Orders", results.get('total_orders', 0))
    with col2:
        st.metric("Total Items", results.get('total_items', 0))
    with col3:
        st.metric("Marketplaces", len(results.get('marketplaces', [])))
    with col4:
        if not results['output1'].empty:
            matched = results['output1']['Product Name'].notna().sum()
            total = len(results['output1'])
            match_rate = (matched / total) * 100 if total > 0 else 0
            st.metric("Product Name Match", f"{match_rate:.1f}%")
        else:
            st.metric("Product Name Match", "0%")
    
    # Warning untuk missing SKUs
    if 'missing_skus' in results and len(results['missing_skus']) > 0:
        st.warning(f"⚠️ {len(results['missing_skus'])} SKU tidak memiliki Product Name")
    
    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📋 Detail Order", 
        "📦 SKU Summary", 
        "📊 Marketplace Summary", 
        "💾 Download"
    ])
    
    with tab1:
        st.subheader("Detail Order (All Marketplaces)")
        
        if results['output1'].empty:
            st.info("Tidak ada data detail order")
        else:
            # Filter options
            col1, col2, col3 = st.columns(3)
            with col1:
                marketplace_filter = st.multiselect(
                    "Filter Marketplace",
                    options=results['output1']['Marketplace'].unique(),
                    default=results['output1']['Marketplace'].unique()
                )
            with col2:
                bundle_filter = st.selectbox(
                    "Bundle Type",
                    ["All", "Bundle Only", "Single Only"]
                )
            with col3:
                name_filter = st.selectbox(
                    "Product Name",
                    ["All", "With Name", "Without Name"]
                )
            
            # Apply filters
            df_display = results['output1'].copy()
            
            if marketplace_filter:
                df_display = df_display[df_display['Marketplace'].isin(marketplace_filter)]
            
            if bundle_filter == "Bundle Only":
                df_display = df_display[df_display['Is Bundle'] == 'Yes']
            elif bundle_filter == "Single Only":
                df_display = df_display[df_display['Is Bundle'] == 'No']
            
            if name_filter == "With Name":
                df_display = df_display[df_display['Product Name'].notna() & (df_display['Product Name'] != '')]
            elif name_filter == "Without Name":
                df_display = df_display[df_display['Product Name'].isna() | (df_display['Product Name'] == '')]
            
            st.dataframe(
                df_display,
                width='stretch',
                height=400
            )
    
    with tab2:
        st.subheader("SKU Summary (Per Marketplace)")
        
        if results['output2'].empty:
            st.info("Tidak ada data SKU summary")
        else:
            # Group by marketplace untuk tabs
            marketplaces = results['output2']['Marketplace'].unique()
            
            # Buat tabs untuk setiap marketplace
            sku_tabs = st.tabs([f"📦 {mp}" for mp in marketplaces])
            
            for idx, mp in enumerate(marketplaces):
                with sku_tabs[idx]:
                    df_mp = results['output2'][results['output2']['Marketplace'] == mp]
                    
                    col_a, col_b = st.columns(2)
                    with col_a:
                        st.metric(f"Total SKU di {mp}", len(df_mp))
                    with col_b:
                        with_names = df_mp[df_mp['Product Name'].notna() & (df_mp['Product Name'] != '')]
                        st.metric(f"Dengan Product Name", len(with_names))
                    
                    # Tabs untuk dengan/tanpa product name
                    sub_tab1, sub_tab2 = st.tabs(["✅ Dengan Nama", "⚠️ Tanpa Nama"])
                    
                    with sub_tab1:
                        df_with = df_mp[df_mp['Product Name'].notna() & (df_mp['Product Name'] != '')]
                        if not df_with.empty:
                            st.dataframe(df_with, width='stretch')
                        else:
                            st.info(f"Tidak ada SKU dengan Product Name di {mp}")
                    
                    with sub_tab2:
                        df_without = df_mp[df_mp['Product Name'].isna() | (df_mp['Product Name'] == '')]
                        if not df_without.empty:
                            st.dataframe(df_without[['SKU Component', 'Total Quantity']], width='stretch')
                        else:
                            st.success(f"🎉 Semua SKU memiliki Product Name di {mp}")
    
    with tab3:
        st.subheader("Marketplace Summary")
        
        if results['output3'].empty:
            st.info("Tidak ada data summary")
        else:
            col_i, col_ii = st.columns([2, 1])
            
            with col_i:
                st.dataframe(
                    results['output3'],
                    width='stretch',
                    hide_index=True
                )
            
            with col_ii:
                # Pie chart
                fig = px.pie(
                    results['output3'],
                    values='Total Orders',
                    names='Marketplace',
                    title="Order Distribution by Marketplace",
                    hole=0.3
                )
                st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.subheader("📥 Download Results")
        
        if results['output1'].empty:
            st.info("Tidak ada data untuk didownload")
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Pilihan download
            col_format, col_include = st.columns(2)
            with col_format:
                download_format = st.radio("Format", ["CSV", "Excel"], horizontal=True)
            with col_include:
                split_by_marketplace = st.checkbox("Split by Marketplace", value=True)
            
            st.divider()
            
            if download_format == "CSV":
                if split_by_marketplace:
                    # Download per marketplace
                    for mp in results['output1']['Marketplace'].unique():
                        df_mp = results['output1'][results['output1']['Marketplace'] == mp]
                        csv_data = df_mp.to_csv(index=False, encoding='utf-8-sig')
                        
                        st.download_button(
                            label=f"📥 Download {mp} Orders",
                            data=csv_data,
                            file_name=f"{mp.lower()}_orders_{timestamp}.csv",
                            mime="text/csv",
                            width='stretch'
                        )
                else:
                    # Download semua
                    csv_all = results['output1'].to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="📥 Download All Orders",
                        data=csv_all,
                        file_name=f"all_marketplaces_orders_{timestamp}.csv",
                        mime="text/csv",
                        width='stretch'
                    )
            else:
                # Excel format
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    if split_by_marketplace:
                        for mp in results['output1']['Marketplace'].unique():
                            df_mp = results['output1'][results['output1']['Marketplace'] == mp]
                            df_mp.to_excel(writer, sheet_name=f'{mp[:20]} Orders', index=False)
                    else:
                        results['output1'].to_excel(writer, sheet_name='All Orders', index=False)
                    
                    results['output2'].to_excel(writer, sheet_name='SKU Summary', index=False)
                    results['output3'].to_excel(writer, sheet_name='Marketplace Summary', index=False)
                
                st.download_button(
                    label="📊 Download All (Excel)",
                    data=output.getvalue(),
                    file_name=f"universal_order_report_{timestamp}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    width='stretch'
                )

else:
    # Landing page
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.info("""
        ## 🌐 Universal Order Processor
        
        **Support untuk:** Shopee, Tokopedia, TikTok
        
        **Fitur:**
        - ✅ Multi-marketplace processing
        - ✅ Auto detect file format
        - ✅ Bundle expansion
        - ✅ Product name lookup
        - ✅ Per marketplace summary
        - ✅ Flexible filters
        
        **Cara pakai:**
        1. Pilih marketplace di sidebar
        2. Upload file order untuk masing-masing marketplace
        3. Upload file kamus master (Excel)
        4. Klik PROCESS ALL DATA
        """)
    
    with col2:
        st.image("https://cdn-icons-png.flaticon.com/512/3208/3208720.png", width=150)

# Footer
st.divider()
st.caption("🌐 **Universal Processor** - Satu dashboard untuk semua marketplace")
