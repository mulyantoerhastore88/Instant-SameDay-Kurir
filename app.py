import streamlit as st
import pandas as pd
import numpy as np
import io
import time
from datetime import datetime
import plotly.express as px

# Page config
st.set_page_config(
    page_title="Instant & SameDay Processor",
    page_icon="🚚",
    layout="wide"
)

# Title
st.title("🚚 Instant & SameDay Order Processor")
st.markdown("Upload file order dan kamus master, proses langsung di sini!")

# Initialize session state
if 'processed' not in st.session_state:
    st.session_state.processed = False
if 'results' not in st.session_state:
    st.session_state.results = {}

# --- FUNGSI CLEANING YANG LEBIH ROBUST ---
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

def clean_df_strings(df):
    """Membersihkan semua kolom string dari spasi/karakter tak terlihat."""
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).apply(lambda x: x.strip() if pd.notna(x) else x)
    return df

# --- FUNGSI LOOKUP BATCH UNTUK PERFORMANCE ---
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
            
            # Debug: tampilkan semua sheet
            st.sidebar.write(f"📊 Sheets ditemukan: {sheet_names}")
            
            # Cari nama sheet yang sesuai
            sheets = {}
            
            # Cari sheet Kurir-Shopee (lebih fleksibel)
            kurir_keywords = ['kurir', 'shopee', 'pengiriman', 'courier']
            kurir_sheets = [s for s in sheet_names if any(kw in s.lower() for kw in kurir_keywords)]
            
            if kurir_sheets:
                sheets['kurir'] = pd.read_excel(kamus_file, sheet_name=kurir_sheets[0], engine='openpyxl')
                st.sidebar.success(f"✅ Sheet Kurir: {kurir_sheets[0]}")
            else:
                # Coba sheet pertama
                sheets['kurir'] = pd.read_excel(kamus_file, sheet_name=0, engine='openpyxl')
                st.sidebar.warning(f"⚠️ Sheet Kurir tidak ditemukan, menggunakan sheet pertama: {sheet_names[0]}")
            
            # Cari sheet Bundle Master
            bundle_keywords = ['bundle', 'bundling', 'paket']
            bundle_sheets = [s for s in sheet_names if any(kw in s.lower() for kw in bundle_keywords)]
            
            if bundle_sheets:
                sheets['bundle'] = pd.read_excel(kamus_file, sheet_name=bundle_sheets[0], engine='openpyxl')
                st.sidebar.success(f"✅ Sheet Bundle: {bundle_sheets[0]}")
            else:
                # Coba sheet kedua atau pertama
                if len(sheet_names) > 1:
                    sheets['bundle'] = pd.read_excel(kamus_file, sheet_name=1, engine='openpyxl')
                    st.sidebar.warning(f"⚠️ Sheet Bundle tidak ditemukan, menggunakan sheet kedua: {sheet_names[1]}")
                else:
                    sheets['bundle'] = pd.read_excel(kamus_file, sheet_name=0, engine='openpyxl')
                    st.sidebar.warning(f"⚠️ Hanya 1 sheet, menggunakan: {sheet_names[0]}")
            
            # Cari sheet SKU Master
            sku_keywords = ['sku', 'master', 'produk', 'product', 'material']
            sku_sheets = [s for s in sheet_names if any(kw in s.lower() for kw in sku_keywords)]
            
            if sku_sheets:
                sheets['sku'] = pd.read_excel(kamus_file, sheet_name=sku_sheets[0], engine='openpyxl')
                st.sidebar.success(f"✅ Sheet SKU: {sku_sheets[0]}")
            else:
                # Coba sheet ketiga atau lainnya
                if len(sheet_names) > 2:
                    sheets['sku'] = pd.read_excel(kamus_file, sheet_name=2, engine='openpyxl')
                    st.sidebar.warning(f"⚠️ Sheet SKU tidak ditemukan, menggunakan sheet ketiga: {sheet_names[2]}")
                elif len(sheet_names) > 1:
                    sheets['sku'] = pd.read_excel(kamus_file, sheet_name=1, engine='openpyxl')
                else:
                    sheets['sku'] = pd.read_excel(kamus_file, sheet_name=0, engine='openpyxl')
            
            # Tampilkan preview kolom
            with st.sidebar.expander("🔍 Kolom di setiap sheet"):
                for sheet_name, df in sheets.items():
                    st.write(f"**{sheet_name}**: {', '.join(df.columns.astype(str))[:50]}...")
            
            return sheets
            
        else:
            st.error("File kamus harus dalam format Excel (.xlsx atau .xls)")
            return None
            
    except Exception as e:
        st.error(f"Error membaca file kamus: {str(e)}")
        return None

# --- FUNGSI PROCESSING UTAMA YANG DIOPTIMASI ---
def process_data(df_orders, kamus_data):
    """Logika processing utama dengan optimasi"""
    
    # Start timer
    start_time = time.time()
    
    # Extract data dari kamus
    df_kurir = kamus_data['kurir'].copy()
    df_bundle = kamus_data['bundle'].copy()
    df_sku = kamus_data['sku'].copy()
    
    # --- PREPARE SKU MAPPING (FAST LOOKUP) ---
    sku_mapping = create_sku_mapping(df_sku)
    st.sidebar.info(f"📚 SKU Mapping: {len(sku_mapping)} entries")
    
    # --- CLEANING DATA MASTER ---
    df_bundle = clean_df_strings(df_bundle)
    
    # Clean SKU Bundle column
    if 'SKU Bundle' in df_bundle.columns:
        df_bundle['SKU Bundle'] = df_bundle['SKU Bundle'].apply(clean_sku)
    else:
        # Cari kolom yang mungkin berisi SKU Bundle
        for col in df_bundle.columns:
            if 'bundle' in col.lower():
                df_bundle.rename(columns={col: 'SKU Bundle'}, inplace=True)
                df_bundle['SKU Bundle'] = df_bundle['SKU Bundle'].apply(clean_sku)
                break
    
    # --- CLEAN ORDERS DATA ---
    df_orders.columns = df_orders.columns.astype(str).str.strip()
    
    # Debug: tampilkan kolom orders
    st.sidebar.write(f"📦 Kolom Order: {len(df_orders.columns)} kolom")
    
    # Pastikan kolom yang diperlukan ada
    required_cols = ['Status Pesanan', 'Pesanan yang Dikelola Shopee', 'Opsi Pengiriman', 'No. Resi', 'Jumlah']
    for col in required_cols:
        if col not in df_orders.columns:
            # Cari kolom dengan nama mirip
            for actual_col in df_orders.columns:
                if col.lower() in actual_col.lower():
                    df_orders.rename(columns={actual_col: col}, inplace=True)
                    break
            if col not in df_orders.columns:
                if col == 'Jumlah':
                    df_orders[col] = 1
                else:
                    df_orders[col] = ""
    
    # Cleaning
    df_orders['Status Pesanan'] = df_orders['Status Pesanan'].astype(str).str.strip()
    df_orders['Pesanan yang Dikelola Shopee'] = df_orders['Pesanan yang Dikelola Shopee'].astype(str).str.strip()
    df_orders['Opsi Pengiriman'] = df_orders['Opsi Pengiriman'].astype(str).str.strip()
    df_orders['No. Resi'] = df_orders['No. Resi'].astype(str).str.strip()
    df_orders['Is Resi Blank'] = (df_orders['No. Resi'] == '') | (df_orders['No. Resi'].isna()) | (df_orders['No. Resi'].str.lower() == 'nan')
    
    # --- PREPARE KURIR FILTER ---
    # Cari kolom Instant/Same Day
    instant_col = None
    for col in df_kurir.columns:
        if any(keyword in col.lower() for keyword in ['instant', 'same', 'day']):
            instant_col = col
            df_kurir.rename(columns={col: 'Instant/Same Day'}, inplace=True)
            break
    
    if 'Instant/Same Day' not in df_kurir.columns:
        # Default ke kolom kedua jika ada
        if len(df_kurir.columns) > 1:
            df_kurir.rename(columns={df_kurir.columns[1]: 'Instant/Same Day'}, inplace=True)
        else:
            st.error("Kolom 'Instant/Same Day' tidak ditemukan di Kurir Master")
            return None
    
    # Clean kurir data
    df_kurir['Opsi Pengiriman'] = df_kurir.iloc[:, 0].astype(str).str.strip()
    df_kurir['Instant/Same Day'] = df_kurir['Instant/Same Day'].astype(str).str.strip()
    
    # Get instant/same day options
    instant_same_day_options = df_kurir[
        df_kurir['Instant/Same Day'].str.upper().str.strip().isin(['YES', 'YA', '1', 'TRUE', 'Y', 'IYA'])
    ]['Opsi Pengiriman'].unique()
    
    # --- FILTER ORDERS ---
    df_filtered = df_orders[
        (df_orders['Status Pesanan'].str.upper() == 'PERLU DIKIRIM') &
        (df_orders['Pesanan yang Dikelola Shopee'].str.upper().isin(['NO', 'TIDAK', 'FALSE'])) &
        (df_orders['Opsi Pengiriman'].isin(instant_same_day_options)) &
        (df_orders['Is Resi Blank'] == True)
    ].copy()
    
    if df_filtered.empty:
        return {"error": "❌ Tidak ada data yang memenuhi kriteria filter."}
    
    # --- SKU AWAL CLEANING ---
    # Cari kolom SKU
    sku_columns = []
    for col in df_filtered.columns:
        if any(keyword in col.lower() for keyword in ['sku', 'referensi', 'produk', 'nama']):
            sku_columns.append(col)
    
    if sku_columns:
        df_filtered['SKU Awal'] = df_filtered[sku_columns[0]]
        for col in sku_columns[1:]:
            df_filtered['SKU Awal'] = df_filtered['SKU Awal'].fillna(df_filtered[col])
    else:
        df_filtered['SKU Awal'] = ""
    
    df_filtered['SKU Awal'] = df_filtered['SKU Awal'].apply(clean_sku)
    
    # --- BUNDLE EXPANSION (OPTIMIZED) ---
    sku_bundle_list = df_bundle['SKU Bundle'].unique() if 'SKU Bundle' in df_bundle.columns else []
    
    # Buat mapping bundle untuk lookup cepat
    bundle_mapping = {}
    if 'SKU Component' in df_bundle.columns:
        component_col = 'SKU Component'
    else:
        # Cari kolom component
        component_col = None
        for col in df_bundle.columns:
            if any(keyword in col.lower() for keyword in ['component', 'item', 'produk']):
                component_col = col
                break
        if not component_col and len(df_bundle.columns) > 1:
            component_col = df_bundle.columns[1]
    
    if component_col:
        for bundle_sku, group in df_bundle.groupby('SKU Bundle'):
            bundle_mapping[bundle_sku] = []
            for _, row in group.iterrows():
                component_sku = clean_sku(row[component_col])
                qty = row.get('Component Quantity', 1) if 'Component Quantity' in row else 1
                bundle_mapping[bundle_sku].append((component_sku, qty))
    
    # Process expansion dengan progress bar
    progress_bar = st.sidebar.progress(0)
    expanded_rows = []
    total_rows = len(df_filtered)
    
    for idx, (_, row) in enumerate(df_filtered.iterrows()):
        sku_awal_cleaned = row['SKU Awal']
        original_sku_raw = row.get('Nomor Referensi SKU', row.get('SKU Awal', ''))
        quantity = row.get('Jumlah', 1)
        
        # Update progress
        if idx % 10 == 0:
            progress_bar.progress(min(idx / total_rows, 0.9))
        
        if sku_awal_cleaned and sku_awal_cleaned in bundle_mapping:
            # Bundle ditemukan
            for component_sku, comp_qty in bundle_mapping[sku_awal_cleaned]:
                expanded_rows.append({
                    'No. Pesanan': row.get('No. Pesanan', ''),
                    'Status Pesanan': row.get('Status Pesanan', ''),
                    'Opsi Pengiriman': row.get('Opsi Pengiriman', ''),
                    'Nomor Referensi SKU Original': original_sku_raw,
                    'Is Bundle?': 'Yes',
                    'SKU Component': component_sku,
                    'Jumlah Final': quantity * comp_qty,
                })
        else:
            # Item satuan
            expanded_rows.append({
                'No. Pesanan': row.get('No. Pesanan', ''),
                'Status Pesanan': row.get('Status Pesanan', ''),
                'Opsi Pengiriman': row.get('Opsi Pengiriman', ''),
                'Nomor Referensi SKU Original': original_sku_raw,
                'Is Bundle?': 'No',
                'SKU Component': sku_awal_cleaned,
                'Jumlah Final': quantity * 1,
            })
    
    progress_bar.progress(1.0)
    time.sleep(0.5)  # Biar progress bar kelihatan
    progress_bar.empty()
    
    df_bundle_expanded = pd.DataFrame(expanded_rows)
    
    # --- ADD PRODUCT NAMES (BATCH PROCESSING) ---
    if not df_bundle_expanded.empty:
        # Batch lookup untuk semua SKU sekaligus
        sku_components = df_bundle_expanded['SKU Component'].tolist()
        product_names = lookup_product_name_batch(sku_mapping, sku_components)
        df_bundle_expanded['Product Name'] = product_names
    
    # --- PREPARE OUTPUTS ---
    # Output 1: Detail Order
    df_output1 = df_bundle_expanded.copy()
    df_output1 = df_output1[[
        'No. Pesanan', 'Status Pesanan', 'Opsi Pengiriman',
        'Nomor Referensi SKU Original', 'Is Bundle?',
        'SKU Component', 'Product Name', 'Jumlah Final'
    ]]
    
    df_output1 = df_output1.rename(columns={
        'SKU Component': 'Nomor Referensi SKU (Component/Cleaned)',
        'Jumlah Final': 'Jumlah dikalikan Component Quantity (Grand Total)'
    })
    
    # Output 2: Grand Total per SKU
    if not df_bundle_expanded.empty:
        df_output2 = df_bundle_expanded.groupby(['SKU Component', 'Product Name']).agg(
            {'Jumlah Final': 'sum'}
        ).reset_index()
        
        df_output2 = df_output2.rename(columns={
            'SKU Component': 'Nomor Referensi SKU (Cleaned)',
            'Jumlah Final': 'Jumlah (Grand total by SKU)'
        }).sort_values('Jumlah (Grand total by SKU)', ascending=False)
    else:
        df_output2 = pd.DataFrame(columns=['Nomor Referensi SKU (Cleaned)', 'Product Name', 'Jumlah (Grand total by SKU)'])
    
    # Output 3: Order Summarize
    df_output3 = df_filtered.groupby('Opsi Pengiriman').agg(
        {'No. Pesanan': 'nunique'}
    ).reset_index().rename(columns={'No. Pesanan': 'Total Order'})
    
    # Debug info
    end_time = time.time()
    processing_time = end_time - start_time
    
    # Tampilkan debug info di sidebar
    with st.sidebar.expander("⚡ Performance Info"):
        st.write(f"⏱️ Processing time: {processing_time:.2f} seconds")
        st.write(f"📊 Original orders: {len(df_orders)}")
        st.write(f"🎯 Filtered orders: {len(df_filtered)}")
        st.write(f"📦 Expanded items: {len(df_bundle_expanded)}")
        st.write(f"📚 SKU Mapping entries: {len(sku_mapping)}")
        
        # Hitung match rate
        if not df_bundle_expanded.empty:
            matched = df_bundle_expanded['Product Name'].notna().sum()
            total = len(df_bundle_expanded)
            match_rate = (matched / total) * 100
            st.write(f"✅ Product Name match: {matched}/{total} ({match_rate:.1f}%)")
    
    # Debug untuk SKU yang tidak ketemu
    missing_skus = df_output1[df_output1['Product Name'] == ""]['Nomor Referensi SKU (Component/Cleaned)'].unique()
    if len(missing_skus) > 0:
        with st.sidebar.expander("🔍 Debug Missing SKUs"):
            st.write(f"Missing SKUs: {len(missing_skus)}")
            
            # Sample debugging
            sample_missing = missing_skus[0] if len(missing_skus) > 0 else ""
            if sample_missing:
                st.write(f"Sample missing SKU: '{sample_missing}'")
                
                # Cari di SKU Master
                found = False
                for key in sku_mapping:
                    if sample_missing in key or key in sample_missing:
                        st.write(f"Matching key in mapping: '{key}' -> '{sku_mapping[key]}'")
                        found = True
                
                if not found:
                    st.write("Not found in SKU mapping")
                    
                    # Tampilkan SKU Master sample
                    st.write("Sample from SKU Master:")
                    st.write(df_sku.head(5)[['Material', 'Material description']] 
                           if 'Material' in df_sku.columns else df_sku.head(5))
    
    return {
        'output1': df_output1,
        'output2': df_output2,
        'output3': df_output3,
        'filtered_count': len(df_filtered),
        'expanded_count': len(df_bundle_expanded),
        'original_count': len(df_orders),
        'sku_master': df_sku,
        'sku_mapping': sku_mapping,
        'processing_time': processing_time,
        'missing_skus': missing_skus
    }

# --- MAIN UI ---
# SIDEBAR
with st.sidebar:
    st.header("📁 Upload Files")
    
    # Upload section
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("1. File Order")
        order_file = st.file_uploader(
            "Upload CSV/Excel",
            type=['csv', 'xlsx', 'xls'],
            key="order",
            label_visibility="collapsed"
        )
    
    with col2:
        st.subheader("2. File Kamus")
        kamus_file = st.file_uploader(
            "Upload Excel",
            type=['xlsx', 'xls'],
            key="kamus",
            label_visibility="collapsed"
        )
    
    if order_file:
        st.success(f"✅ Order: {order_file.name}")
    if kamus_file:
        st.success(f"✅ Kamus: {kamus_file.name}")
    
    st.divider()
    
    # Process button dengan status
    if order_file and kamus_file:
        if st.button("🚀 PROCESS DATA", type="primary", use_container_width=True):
            st.session_state.order_file = order_file
            st.session_state.kamus_file = kamus_file
            st.rerun()
    else:
        st.button("🚀 PROCESS DATA", type="primary", use_container_width=True, disabled=True)
    
    st.divider()
    st.caption("⚡ Optimized v4.0 | Fast Processing")

# MAIN CONTENT - Processing
if hasattr(st.session_state, 'order_file') and hasattr(st.session_state, 'kamus_file'):
    order_file = st.session_state.order_file
    kamus_file = st.session_state.kamus_file
    
    with st.spinner(f"Processing {order_file.name}..."):
        try:
            # Read files
            if order_file.name.endswith('.csv'):
                df_orders = pd.read_csv(order_file)
            else:
                df_orders = pd.read_excel(order_file, engine='openpyxl')
            
            kamus_data = read_kamus_file(kamus_file)
            
            if kamus_data:
                # Process dengan progress indicators
                results = process_data(df_orders, kamus_data)
                
                if results and "error" not in results:
                    st.session_state.results = results
                    st.session_state.processed = True
                    
                    # Clear file references
                    del st.session_state.order_file
                    del st.session_state.kamus_file
                    
                    st.rerun()
                elif results and "error" in results:
                    st.error(results["error"])
                else:
                    st.error("❌ Processing failed")
        
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
            import traceback
            st.code(traceback.format_exc())

# Display results jika sudah processed
if st.session_state.processed:
    results = st.session_state.results
    
    # Header dengan metrics
    st.success(f"✅ Processing completed in {results['processing_time']:.1f}s")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Original Orders", results['original_count'])
    with col2:
        st.metric("Filtered Orders", results['filtered_count'])
    with col3:
        st.metric("Expanded Items", results['expanded_count'])
    with col4:
        matched = results['output1']['Product Name'].notna().sum()
        total = len(results['output1'])
        match_rate = (matched / total) * 100
        st.metric("Product Name Match", f"{match_rate:.1f}%")
    
    # Warning jika ada SKU yang tidak ketemu
    if len(results['missing_skus']) > 0:
        st.warning(f"⚠️ {len(results['missing_skus'])} SKU tidak memiliki Product Name")
        
        with st.expander("🔍 Debug SKU Mismatch", expanded=False):
            st.write("**SKU yang tidak ditemukan:**")
            st.write(results['missing_skus'][:50])  # Limit 50
            
            st.write("**Contoh SKU Master:**")
            st.dataframe(results['sku_master'].head(10), use_container_width=True)
            
            # Advanced debugging
            if st.button("Run Deep Debug"):
                st.write("**Deep Debug Analysis:**")
                
                # Bandingkan format
                sample_missing = results['missing_skus'][0]
                st.write(f"Sample missing: '{sample_missing}'")
                st.write(f"Length: {len(sample_missing)} chars")
                st.write(f"ASCII codes: {[ord(c) for c in sample_missing[:10]]}")
                
                # Cari di SKU Master dengan berbagai cara
                st.write("**Search in SKU Master:**")
                
                df_sku = results['sku_master']
                found_anywhere = False
                
                for col in df_sku.columns:
                    # Coba exact match
                    exact_matches = df_sku[df_sku[col].astype(str).str.strip() == sample_missing]
                    if not exact_matches.empty:
                        st.write(f"✅ Exact match in column '{col}':")
                        st.write(exact_matches.head())
                        found_anywhere = True
                    
                    # Coba partial match
                    partial_matches = df_sku[df_sku[col].astype(str).str.contains(sample_missing, na=False)]
                    if not partial_matches.empty:
                        st.write(f"🔍 Partial match in column '{col}':")
                        st.write(partial_matches.head())
                        found_anywhere = True
                
                if not found_anywhere:
                    st.write("❌ Not found in any column")
                    
                    # Tampilkan SKU yang mirip
                    st.write("**Similar SKUs in master:**")
                    for col in df_sku.columns:
                        if df_sku[col].dtype == 'object':
                            similar = df_sku[df_sku[col].astype(str).str.contains(sample_missing[:5], na=False)]
                            if not similar.empty:
                                st.write(f"In column '{col}':")
                                st.write(similar.head())
    
    # Tabs untuk output
    tab1, tab2, tab3, tab4 = st.tabs([
        "📋 Detail Order", 
        "📦 SKU Summary", 
        "📊 Statistics", 
        "💾 Download"
    ])
    
    with tab1:
        # ... (sama seperti sebelumnya, tapi tambahkan filter)
        st.subheader("Detail Order")
        
        # Filter options
        col_filter1, col_filter2 = st.columns(2)
        with col_filter1:
            show_bundles = st.selectbox(
                "Show",
                ["All", "Bundles Only", "Singles Only"],
                key="filter_bundle"
            )
        
        with col_filter2:
            show_product_name = st.selectbox(
                "Product Name",
                ["All", "With Name", "Without Name"],
                key="filter_name"
            )
        
        # Apply filters
        df_display = results['output1'].copy()
        
        if show_bundles == "Bundles Only":
            df_display = df_display[df_display['Is Bundle?'] == 'Yes']
        elif show_bundles == "Singles Only":
            df_display = df_display[df_display['Is Bundle?'] == 'No']
        
        if show_product_name == "With Name":
            df_display = df_display[df_display['Product Name'].notna() & (df_display['Product Name'] != '')]
        elif show_product_name == "Without Name":
            df_display = df_display[df_display['Product Name'].isna() | (df_display['Product Name'] == '')]
        
        st.dataframe(
            df_display,
            use_container_width=True,
            height=400
        )
    
    with tab2:
        # ... (sama seperti sebelumnya)
        pass
    
    with tab3:
        # ... (sama seperti sebelumnya)
        pass
    
    with tab4:
        # ... (sama seperti sebelumnya)
        pass

else:
    # Landing page
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.info("""
        ## 📋 Instruksi Cepat:
        
        1. **Upload 2 file** di sidebar:
           - **File Order** (CSV/Excel dari Shopee)
           - **File Kamus Master** (Excel dengan 3 sheet)
        
        2. Klik **PROCESS DATA**
        
        3. Lihat hasil & download
        
        ⚡ **Optimized:** Processing lebih cepat dengan batch operations
        🔍 **Debug:** Fitur debugging untuk SKU yang tidak match
        """)
    
    with col2:
        st.image("https://cdn-icons-png.flaticon.com/512/3208/3208720.png", width=150)

# Footer
st.divider()
st.caption("💡 **Tip:** Jika SKU tidak match, cek format teks, spasi, atau karakter khusus di file Kamus Master")
