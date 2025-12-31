import streamlit as st
import pandas as pd
import numpy as np
import io
import time
from datetime import datetime

# Page config
st.set_page_config(
    page_title="Order Processor Simple",
    page_icon="📦",
    layout="wide"
)

# Title
st.title("📦 Order Processor - Simple Version")
st.markdown("Upload file Shopee & Tokopedia sekaligus, langsung proses & download!")

# Initialize session state
if 'processed' not in st.session_state:
    st.session_state.processed = False
if 'results' not in st.session_state:
    st.session_state.results = {}

# --- FUNGSI SIMPLE CLEANING ---
def clean_sku(sku):
    """Bersihkan SKU: ambil bagian setelah hyphen terakhir."""
    if pd.isna(sku):
        return ""
    sku = str(sku).strip()
    if '-' in sku:
        parts = sku.split('-')
        # Ambil bagian terakhir yang bukan kosong
        for part in reversed(parts):
            if part.strip():
                return part.strip()
    return sku

# --- FUNGSI LOOKUP PRODUCT NAME ---
def create_sku_mapping(df_sku):
    """Buat mapping dari SKU Master (kolom B ke C)"""
    sku_mapping = {}
    
    if df_sku.empty:
        return sku_mapping
    
    # Debug: tampilkan kolom yang ada
    st.sidebar.write(f"📋 Kolom SKU Master: {list(df_sku.columns)}")
    
    # Ambil kolom B (index 1) dan C (index 2)
    if len(df_sku.columns) >= 3:
        sku_col = df_sku.columns[1]  # Kolom B
        name_col = df_sku.columns[2]  # Kolom C
    elif len(df_sku.columns) >= 2:
        sku_col = df_sku.columns[0]
        name_col = df_sku.columns[1]
    else:
        return sku_mapping
    
    # Buat mapping
    for _, row in df_sku.iterrows():
        sku_code = str(row[sku_col]) if pd.notna(row[sku_col]) else ""
        product_name = str(row[name_col]) if pd.notna(row[name_col]) else ""
        
        if sku_code and product_name:
            cleaned_sku = clean_sku(sku_code)
            if cleaned_sku and cleaned_sku not in sku_mapping:
                sku_mapping[cleaned_sku] = product_name
    
    st.sidebar.success(f"✅ Mapping: {len(sku_mapping)} SKU → Product Name")
    
    # Tampilkan sample
    if len(sku_mapping) > 0:
        sample = list(sku_mapping.items())[:2]
        for sku, name in sample:
            st.sidebar.write(f"  {sku} → {name[:20]}...")
    
    return sku_mapping

# --- FUNGSI BACA FILE KAMUS ---
def read_kamus_file(kamus_file):
    """Baca file kamus Excel dengan 3 sheet"""
    try:
        if kamus_file.name.endswith('.xlsx') or kamus_file.name.endswith('.xls'):
            # Baca semua sheet
            excel_file = pd.ExcelFile(kamus_file, engine='openpyxl')
            sheet_names = excel_file.sheet_names
            
            # Baca 3 sheet pertama (Kurir, Bundle, SKU Master)
            df_kurir = pd.read_excel(kamus_file, sheet_name=0, engine='openpyxl')
            df_bundle = pd.read_excel(kamus_file, sheet_name=1, engine='openpyxl')
            df_sku = pd.read_excel(kamus_file, sheet_name=2, engine='openpyxl')
            
            st.sidebar.success(f"✅ Kamus: {len(sheet_names)} sheet terbaca")
            
            return {
                'kurir': df_kurir,
                'bundle': df_bundle,
                'sku': df_sku
            }
    except Exception as e:
        st.error(f"Error baca file kamus: {str(e)}")
        return None

# --- FUNGSI PROCESS SHOPEE ---
def process_shopee(df_shopee, kamus_data, sku_mapping):
    """Proses data Shopee"""
    expanded_rows = []
    
    # Ambil data dari kamus
    df_kurir = kamus_data['kurir']
    df_bundle = kamus_data['bundle']
    
    # Clean bundle data
    df_bundle['SKU Bundle'] = df_bundle['SKU Bundle'].apply(clean_sku)
    
    # Buat bundle mapping
    bundle_mapping = {}
    component_col = 'SKU Component' if 'SKU Component' in df_bundle.columns else df_bundle.columns[1] if len(df_bundle.columns) > 1 else None
    
    if component_col:
        for bundle_sku, group in df_bundle.groupby('SKU Bundle'):
            bundle_mapping[bundle_sku] = []
            for _, row in group.iterrows():
                component_sku = clean_sku(row[component_col])
                qty = row.get('Component Quantity', 1)
                bundle_mapping[bundle_sku].append((component_sku, qty))
    
    # Standardize column names untuk Shopee
    df_shopee.columns = [str(col).strip() for col in df_shopee.columns]
    
    # Rename columns jika perlu
    col_mapping = {
        'No. Pesanan': 'order_id',
        'Status Pesanan': 'status',
        'Pesanan yang Dikelola Shopee': 'managed_shopee',
        'Opsi Pengiriman': 'shipping',
        'No. Resi': 'resi',
        'Nomor Referensi SKU': 'sku_reference',
        'SKU Induk': 'sku_induk',
        'Nama Produk': 'product_name',
        'Jumlah': 'quantity'
    }
    
    for old_col, new_col in col_mapping.items():
        if old_col in df_shopee.columns:
            df_shopee.rename(columns={old_col: new_col}, inplace=True)
    
    # Filter untuk Shopee: Perlu Dikirim + No Resi
    df_filtered = df_shopee[
        (df_shopee['status'].astype(str).str.upper() == 'PERLU DIKIRIM') &
        (df_shopee['managed_shopee'].astype(str).str.upper() == 'NO') &
        (df_shopee['resi'].isna() | (df_shopee['resi'] == ''))
    ].copy()
    
    if df_filtered.empty:
        return expanded_rows
    
    # Get instant/same day kurir
    df_kurir['Opsi Pengiriman'] = df_kurir.iloc[:, 0].astype(str).str.strip()
    df_kurir['Instant/Same Day'] = df_kurir.iloc[:, 1].astype(str).str.strip()
    
    instant_kurir = df_kurir[
        df_kurir['Instant/Same Day'].str.upper().isin(['YES', 'YA', '1', 'TRUE'])
    ]['Opsi Pengiriman'].unique()
    
    df_filtered = df_filtered[df_filtered['shipping'].isin(instant_kurir)]
    
    # Process each row
    for _, row in df_filtered.iterrows():
        # Cari SKU dari berbagai kolom
        sku_candidates = [
            row.get('sku_reference', ''),
            row.get('sku_induk', ''),
            row.get('product_name', '')
        ]
        
        sku_awal = ''
        for candidate in sku_candidates:
            if pd.notna(candidate) and str(candidate).strip():
                sku_awal = clean_sku(str(candidate))
                if sku_awal:
                    break
        
        if not sku_awal:
            continue
            
        qty = row.get('quantity', 1)
        
        if sku_awal in bundle_mapping:
            # Bundle
            for component_sku, comp_qty in bundle_mapping[sku_awal]:
                expanded_rows.append({
                    'Marketplace': 'Shopee',
                    'Order ID': row.get('order_id', ''),
                    'Original SKU': sku_awal,
                    'Is Bundle': 'Yes',
                    'SKU Component': component_sku,
                    'Product Name': sku_mapping.get(component_sku, ''),
                    'Quantity': qty * comp_qty
                })
        else:
            # Single
            expanded_rows.append({
                'Marketplace': 'Shopee',
                'Order ID': row.get('order_id', ''),
                'Original SKU': sku_awal,
                'Is Bundle': 'No',
                'SKU Component': sku_awal,
                'Product Name': sku_mapping.get(sku_awal, ''),
                'Quantity': qty
            })
    
    return expanded_rows

# --- FUNGSI PROCESS TOKPED ---
def process_tokped(df_tokped, kamus_data, sku_mapping):
    """Proses data Tokopedia/TikTok"""
    expanded_rows = []
    
    # Ambil bundle data dari kamus
    df_bundle = kamus_data['bundle']
    
    # Clean bundle data
    df_bundle['SKU Bundle'] = df_bundle['SKU Bundle'].apply(clean_sku)
    
    # Buat bundle mapping
    bundle_mapping = {}
    component_col = 'SKU Component' if 'SKU Component' in df_bundle.columns else df_bundle.columns[1] if len(df_bundle.columns) > 1 else None
    
    if component_col:
        for bundle_sku, group in df_bundle.groupby('SKU Bundle'):
            bundle_mapping[bundle_sku] = []
            for _, row in group.iterrows():
                component_sku = clean_sku(row[component_col])
                qty = row.get('Component Quantity', 1)
                bundle_mapping[bundle_sku].append((component_sku, qty))
    
    # Standardize column names untuk Tokped
    df_tokped.columns = [str(col).strip() for col in df_tokped.columns]
    
    # Rename columns jika perlu
    col_mapping = {
        'Order ID': 'order_id',
        'Seller SKU': 'sku_reference',
        'Quantity': 'quantity',
        'Tracking ID': 'tracking_id'
    }
    
    for old_col, new_col in col_mapping.items():
        if old_col in df_tokped.columns:
            df_tokped.rename(columns={old_col: new_col}, inplace=True)
    
    # Filter untuk Tokped: No Tracking ID
    if 'tracking_id' in df_tokped.columns:
        df_filtered = df_tokped[
            df_tokped['tracking_id'].isna() | (df_tokped['tracking_id'] == '')
        ].copy()
    else:
        df_filtered = df_tokped.copy()
    
    # Process each row
    for _, row in df_filtered.iterrows():
        sku_awal = clean_sku(row.get('sku_reference', ''))
        if not sku_awal:
            continue
            
        qty = row.get('quantity', 1)
        
        if sku_awal in bundle_mapping:
            # Bundle
            for component_sku, comp_qty in bundle_mapping[sku_awal]:
                expanded_rows.append({
                    'Marketplace': 'Tokopedia/TikTok',
                    'Order ID': row.get('order_id', ''),
                    'Original SKU': sku_awal,
                    'Is Bundle': 'Yes',
                    'SKU Component': component_sku,
                    'Product Name': sku_mapping.get(component_sku, ''),
                    'Quantity': qty * comp_qty
                })
        else:
            # Single
            expanded_rows.append({
                'Marketplace': 'Tokopedia/TikTok',
                'Order ID': row.get('order_id', ''),
                'Original SKU': sku_awal,
                'Is Bundle': 'No',
                'SKU Component': sku_awal,
                'Product Name': sku_mapping.get(sku_awal, ''),
                'Quantity': qty
            })
    
    return expanded_rows

# --- FUNGSI PROCESS ALL ---
def process_all_data(shopee_file, tokped_file, kamus_data):
    """Proses semua file sekaligus"""
    start_time = time.time()
    all_expanded_rows = []
    
    # Buat SKU mapping dari kamus
    sku_mapping = create_sku_mapping(kamus_data['sku'])
    
    # Proses Shopee jika ada
    if shopee_file is not None:
        try:
            # Baca file Shopee
            if shopee_file.name.endswith('.csv'):
                df_shopee = pd.read_csv(shopee_file)
            else:
                df_shopee = pd.read_excel(shopee_file, engine='openpyxl')
            
            st.sidebar.success(f"✅ Shopee: {df_shopee.shape[0]} rows")
            
            # Proses Shopee
            shopee_rows = process_shopee(df_shopee, kamus_data, sku_mapping)
            all_expanded_rows.extend(shopee_rows)
            
            st.sidebar.info(f"Shopee processed: {len(shopee_rows)} items")
            
        except Exception as e:
            st.error(f"Error processing Shopee: {str(e)}")
    
    # Proses Tokped jika ada
    if tokped_file is not None:
        try:
            # Baca file Tokped
            if tokped_file.name.endswith('.csv'):
                df_tokped = pd.read_csv(tokped_file)
            else:
                df_tokped = pd.read_excel(tokped_file, engine='openpyxl')
            
            st.sidebar.success(f"✅ Tokopedia/TikTok: {df_tokped.shape[0]} rows")
            
            # Proses Tokped
            tokped_rows = process_tokped(df_tokped, kamus_data, sku_mapping)
            all_expanded_rows.extend(tokped_rows)
            
            st.sidebar.info(f"Tokped processed: {len(tokped_rows)} items")
            
        except Exception as e:
            st.error(f"Error processing Tokped: {str(e)}")
    
    # Jika tidak ada data
    if not all_expanded_rows:
        return {"error": "❌ Tidak ada data yang memenuhi filter."}
    
    # Buat DataFrame hasil
    df_expanded = pd.DataFrame(all_expanded_rows)
    
    # --- BUAT OUTPUTS ---
    # 1. Summary per Marketplace
    if not df_expanded.empty:
        summary_data = []
        for mp in df_expanded['Marketplace'].unique():
            mp_data = df_expanded[df_expanded['Marketplace'] == mp]
            summary_data.append({
                'Marketplace': mp,
                'Total Orders': mp_data['Order ID'].nunique(),
                'Total Items': mp_data.shape[0],
                'Bundle Items': mp_data[mp_data['Is Bundle'] == 'Yes'].shape[0],
                'Single Items': mp_data[mp_data['Is Bundle'] == 'No'].shape[0]
            })
        df_summary = pd.DataFrame(summary_data)
    else:
        df_summary = pd.DataFrame(columns=['Marketplace', 'Total Orders', 'Total Items', 'Bundle Items', 'Single Items'])
    
    # 2. Total SKU Component untuk picking
    if not df_expanded.empty:
        df_picking = df_expanded.groupby(['SKU Component', 'Product Name']).agg({
            'Quantity': 'sum',
            'Marketplace': lambda x: ', '.join(sorted(set(x)))
        }).reset_index()
        
        df_picking = df_picking.rename(columns={
            'Quantity': 'Total Quantity',
            'Marketplace': 'Used In Marketplace'
        }).sort_values('Total Quantity', ascending=False)
    else:
        df_picking = pd.DataFrame(columns=['SKU Component', 'Product Name', 'Total Quantity', 'Used In Marketplace'])
    
    processing_time = time.time() - start_time
    
    return {
        'summary': df_summary,
        'detail': df_expanded,
        'picking': df_picking,
        'processing_time': processing_time,
        'total_items': len(df_expanded),
        'shopee_items': len([r for r in all_expanded_rows if r['Marketplace'] == 'Shopee']),
        'tokped_items': len([r for r in all_expanded_rows if r['Marketplace'] == 'Tokopedia/TikTok'])
    }

# --- SIDEBAR UPLOAD ---
with st.sidebar:
    st.header("📁 Upload Files")
    
    st.subheader("1. File Order Shopee")
    shopee_file = st.file_uploader(
        "Upload file Shopee",
        type=['csv', 'xlsx', 'xls'],
        help="File order dari Shopee (opsional)",
        key="shopee"
    )
    
    st.subheader("2. File Order Tokopedia/TikTok")
    tokped_file = st.file_uploader(
        "Upload file Tokopedia/TikTok",
        type=['csv', 'xlsx', 'xls'],
        help="File order dari Tokopedia atau TikTok (opsional)",
        key="tokped"
    )
    
    st.subheader("3. File Kamus")
    kamus_file = st.file_uploader(
        "Upload file kamus (Excel)",
        type=['xlsx', 'xls'],
        help="Excel dengan 3 sheet: Kurir, Bundle, SKU Master",
        key="kamus"
    )
    
    st.divider()
    
    # Check if at least one order file and kamus file are uploaded
    has_order_files = (shopee_file is not None) or (tokped_file is not None)
    
    if has_order_files and kamus_file:
        if st.button("🚀 PROCESS ALL DATA", type="primary", use_container_width=True):
            st.session_state.shopee_file = shopee_file
            st.session_state.tokped_file = tokped_file
            st.session_state.kamus_file = kamus_file
            st.rerun()
    else:
        st.button("🚀 PROCESS ALL DATA", type="primary", use_container_width=True, disabled=True)
        if not has_order_files:
            st.warning("⚠️ Upload minimal 1 file order (Shopee atau Tokped)")
        if not kamus_file:
            st.warning("⚠️ Upload file kamus")
    
    st.caption("Version: Simple & Fast")

# --- MAIN PROCESSING ---
if all(hasattr(st.session_state, attr) for attr in ['kamus_file']) and \
   (hasattr(st.session_state, 'shopee_file') or hasattr(st.session_state, 'tokped_file')):
    
    shopee_file = st.session_state.get('shopee_file')
    tokped_file = st.session_state.get('tokped_file')
    kamus_file = st.session_state.kamus_file
    
    with st.spinner("Processing semua data..."):
        try:
            # Read kamus file
            kamus_data = read_kamus_file(kamus_file)
            
            if kamus_data:
                # Process all data
                results = process_all_data(shopee_file, tokped_file, kamus_data)
                
                if "error" not in results:
                    st.session_state.results = results
                    st.session_state.processed = True
                    
                    # Clear file references
                    if hasattr(st.session_state, 'shopee_file'):
                        del st.session_state.shopee_file
                    if hasattr(st.session_state, 'tokped_file'):
                        del st.session_state.tokped_file
                    if hasattr(st.session_state, 'kamus_file'):
                        del st.session_state.kamus_file
                    
                    st.success("✅ Semua data berhasil diproses!")
                    st.rerun()
                else:
                    st.error(results["error"])
        
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")

# --- DISPLAY RESULTS ---
if st.session_state.processed and 'results' in st.session_state:
    results = st.session_state.results
    
    # Header metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Items", results['total_items'])
    with col2:
        st.metric("Shopee Items", results.get('shopee_items', 0))
    with col3:
        st.metric("Tokped Items", results.get('tokped_items', 0))
    with col4:
        st.metric("Processing Time", f"{results['processing_time']:.1f}s")
    
    # 3 Tabs
    tab1, tab2, tab3 = st.tabs([
        "📊 Summary Order", 
        "📦 SKU untuk Picking", 
        "💾 Download Excel"
    ])
    
    with tab1:
        col_a, col_b = st.columns([1, 2])
        
        with col_a:
            st.subheader("Summary")
            if not results['summary'].empty:
                st.dataframe(
                    results['summary'],
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("Tidak ada data summary")
        
        with col_b:
            st.subheader("Detail Order")
            if not results['detail'].empty:
                st.dataframe(
                    results['detail'],
                    use_container_width=True,
                    hide_index=True,
                    height=500
                )
            else:
                st.info("Tidak ada data detail")
    
    with tab2:
        st.subheader("Total SKU Component untuk Picking")
        
        if not results['picking'].empty:
            # Info penting
            total_unique_sku = len(results['picking'])
            total_qty = results['picking']['Total Quantity'].sum()
            
            col_x, col_y = st.columns(2)
            with col_x:
                st.metric("Unique SKU", total_unique_sku)
            with col_y:
                st.metric("Total Quantity", total_qty)
            
            # Tampilkan data picking
            st.dataframe(
                results['picking'],
                use_container_width=True,
                hide_index=True,
                height=500
            )
            
            # Highlight yang tanpa product name
            missing_names = results['picking'][results['picking']['Product Name'] == '']
            if not missing_names.empty:
                st.warning(f"⚠️ {len(missing_names)} SKU tanpa Product Name")
                with st.expander("Lihat SKU tanpa Product Name"):
                    st.dataframe(missing_names[['SKU Component', 'Total Quantity']], 
                               use_container_width=True)
        else:
            st.info("Tidak ada data untuk picking")
    
    with tab3:
        st.subheader("Download Hasil ke Excel")
        
        if not results['detail'].empty:
            # Generate timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Info file
            st.info(f"File akan berisi 3 sheet:")
            st.write("1. **Summary** - Ringkasan per marketplace")
            st.write("2. **Detail Order** - Detail semua order")
            st.write("3. **SKU untuk Picking** - Total per SKU untuk picker")
            
            # Buat Excel file
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                results['summary'].to_excel(writer, sheet_name='Summary', index=False)
                results['detail'].to_excel(writer, sheet_name='Detail Order', index=False)
                results['picking'].to_excel(writer, sheet_name='SKU untuk Picking', index=False)
            
            # Tombol download
            st.download_button(
                label="📥 DOWNLOAD EXCEL REPORT",
                data=output.getvalue(),
                file_name=f"order_report_{timestamp}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                type="primary"
            )
            
            st.success("✅ File Excel siap didownload!")
        else:
            st.info("Tidak ada data untuk didownload")

else:
    # Landing page
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.info("""
        ## 📋 Cara Pakai:
        
        1. **Upload file Shopee** (opsional - bisa skip kalau tidak ada)
        2. **Upload file Tokopedia/TikTok** (opsional - bisa skip kalau tidak ada)
        3. **Upload file kamus** (wajib - Excel dengan 3 sheet)
        4. Klik **PROCESS ALL DATA**
        5. Lihat hasil di 3 tab
        
        ### Minimal 1 file order (Shopee ATAU Tokped)
        
        ### Format Kamus (Excel):
        - **Sheet 1**: Kurir-Shopee  
          (kolom A: Opsi Pengiriman, kolom B: Instant/Same Day)
        
        - **Sheet 2**: Bundle Master  
          (kolom A: SKU Bundle, kolom B: SKU Component, kolom G: Component Quantity)
        
        - **Sheet 3**: SKU Master  
          (kolom B: SKU Code, kolom C: Material description)
        """)
    
    with col2:
        st.image("https://cdn-icons-png.flaticon.com/512/3208/3208720.png", width=150)

# Footer
st.divider()
st.caption("Simple Order Processor | Upload Shopee & Tokped sekaligus")
