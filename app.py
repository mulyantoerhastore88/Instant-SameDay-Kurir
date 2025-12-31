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
st.markdown("Upload file order & kamus, langsung proses & download!")

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
    
    # Ambil kolom B (index 1) dan C (index 2)
    # Pastikan dataframe memiliki minimal 3 kolom
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
    
    return sku_mapping

# --- FUNGSI BACA FILE KAMUS ---
def read_kamus_file(kamus_file):
    """Baca file kamus Excel dengan 3 sheet"""
    try:
        if kamus_file.name.endswith('.xlsx') or kamus_file.name.endswith('.xls'):
            # Baca sheet
            df_kurir = pd.read_excel(kamus_file, sheet_name=0, engine='openpyxl')  # Sheet 1
            df_bundle = pd.read_excel(kamus_file, sheet_name=1, engine='openpyxl')  # Sheet 2
            df_sku = pd.read_excel(kamus_file, sheet_name=2, engine='openpyxl')    # Sheet 3
            
            return {
                'kurir': df_kurir,
                'bundle': df_bundle,
                'sku': df_sku
            }
    except Exception as e:
        st.error(f"Error baca file kamus: {str(e)}")
        return None

# --- FUNGSI DETECT FILE TYPE ---
def detect_file_type(df, filename):
    """Deteksi apakah file Shopee atau Tokped"""
    filename_lower = filename.lower()
    
    # Cek dari nama file
    if 'shopee' in filename_lower:
        return 'shopee'
    elif any(x in filename_lower for x in ['tokped', 'tokopedia', 'tiktok']):
        return 'tokped'
    
    # Cek dari kolom
    cols_lower = [col.lower() for col in df.columns]
    
    # Deteksi Shopee
    shopee_cols = ['no. pesanan', 'status pesanan', 'pesanan yang dikelola shopee']
    if any(any(sc in col for sc in shopee_cols) for col in cols_lower):
        return 'shopee'
    
    # Deteksi Tokped
    tokped_cols = ['order id', 'seller sku', 'sku id']
    if any(any(tc in col for tc in tokped_cols) for col in cols_lower):
        return 'tokped'
    
    return 'unknown'

# --- FUNGSI PROCESSING UTAMA ---
def process_simple(df_orders, kamus_data, file_type):
    """Proses data sesuai tipe file"""
    
    start_time = time.time()
    
    # Ambil data dari kamus
    df_kurir = kamus_data['kurir']
    df_bundle = kamus_data['bundle']
    df_sku = kamus_data['sku']
    
    # Buat SKU mapping
    sku_mapping = create_sku_mapping(df_sku)
    
    # Clean bundle data
    df_bundle['SKU Bundle'] = df_bundle['SKU Bundle'].apply(clean_sku)
    
    # Buat bundle mapping
    bundle_mapping = {}
    if 'SKU Component' in df_bundle.columns:
        component_col = 'SKU Component'
    elif 'Component' in df_bundle.columns:
        component_col = 'Component'
    else:
        component_col = df_bundle.columns[1] if len(df_bundle.columns) > 1 else None
    
    if component_col:
        for bundle_sku, group in df_bundle.groupby('SKU Bundle'):
            bundle_mapping[bundle_sku] = []
            for _, row in group.iterrows():
                component_sku = clean_sku(row[component_col])
                qty = row.get('Component Quantity', 1)
                bundle_mapping[bundle_sku].append((component_sku, qty))
    
    # --- PROCESS BERDASARKAN TIPE FILE ---
    expanded_rows = []
    
    if file_type == 'shopee':
        # Standardize column names
        df_orders.columns = [str(col).strip() for col in df_orders.columns]
        
        # Rename columns if needed
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
            if old_col in df_orders.columns:
                df_orders.rename(columns={old_col: new_col}, inplace=True)
        
        # Filter untuk Shopee
        df_filtered = df_orders[
            (df_orders['status'].astype(str).str.upper() == 'PERLU DIKIRIM') &
            (df_orders['managed_shopee'].astype(str).str.upper() == 'NO') &
            (df_orders['resi'].isna() | (df_orders['resi'] == ''))
        ].copy()
        
        # Get instant/same day kurir
        df_kurir['Opsi Pengiriman'] = df_kurir.iloc[:, 0].astype(str).str.strip()
        df_kurir['Instant/Same Day'] = df_kurir.iloc[:, 1].astype(str).str.strip()
        
        instant_kurir = df_kurir[
            df_kurir['Instant/Same Day'].str.upper().isin(['YES', 'YA', '1', 'TRUE'])
        ]['Opsi Pengiriman'].unique()
        
        df_filtered = df_filtered[df_filtered['shipping'].isin(instant_kurir)]
        
        # Process each row
        for _, row in df_filtered.iterrows():
            sku_awal = clean_sku(row.get('sku_reference', row.get('sku_induk', row.get('product_name', ''))))
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
    
    elif file_type == 'tokped':
        # Standardize column names
        df_orders.columns = [str(col).strip() for col in df_orders.columns]
        
        # Rename columns if needed
        col_mapping = {
            'Order ID': 'order_id',
            'Seller SKU': 'sku_reference',
            'Quantity': 'quantity'
        }
        
        for old_col, new_col in col_mapping.items():
            if old_col in df_orders.columns:
                df_orders.rename(columns={old_col: new_col}, inplace=True)
        
        # Filter hanya yang belum ada resi
        if 'Tracking ID' in df_orders.columns:
            df_filtered = df_orders[
                df_orders['Tracking ID'].isna() | (df_orders['Tracking ID'] == '')
            ].copy()
        else:
            df_filtered = df_orders.copy()
        
        # Process each row
        for _, row in df_filtered.iterrows():
            sku_awal = clean_sku(row.get('sku_reference', ''))
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
    
    # Buat DataFrame hasil
    df_expanded = pd.DataFrame(expanded_rows)
    
    if df_expanded.empty:
        return {"error": "❌ Tidak ada data yang memenuhi filter."}
    
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
        'file_type': file_type
    }

# --- SIDEBAR UPLOAD ---
with st.sidebar:
    st.header("📁 Upload Files")
    
    st.subheader("1. File Order")
    order_file = st.file_uploader(
        "Upload file order",
        type=['csv', 'xlsx', 'xls'],
        help="Shopee atau Tokopedia/TikTok"
    )
    
    st.subheader("2. File Kamus")
    kamus_file = st.file_uploader(
        "Upload file kamus (Excel)",
        type=['xlsx', 'xls'],
        help="Excel dengan 3 sheet: Kurir, Bundle, SKU Master"
    )
    
    st.divider()
    
    if order_file and kamus_file:
        if st.button("🚀 PROCESS DATA", type="primary", use_container_width=True):
            st.session_state.order_file = order_file
            st.session_state.kamus_file = kamus_file
            st.rerun()
    else:
        st.button("🚀 PROCESS DATA", type="primary", use_container_width=True, disabled=True)
    
    st.caption("Version: Simple & Fast")

# --- MAIN PROCESSING ---
if hasattr(st.session_state, 'order_file') and hasattr(st.session_state, 'kamus_file'):
    order_file = st.session_state.order_file
    kamus_file = st.session_state.kamus_file
    
    with st.spinner("Processing data..."):
        try:
            # Read files
            if order_file.name.endswith('.csv'):
                df_orders = pd.read_csv(order_file)
            else:
                df_orders = pd.read_excel(order_file, engine='openpyxl')
            
            kamus_data = read_kamus_file(kamus_file)
            
            if kamus_data:
                # Detect file type
                file_type = detect_file_type(df_orders, order_file.name)
                
                if file_type == 'unknown':
                    st.error("❌ Tidak bisa mendeteksi tipe file. Pastikan format file sesuai Shopee atau Tokopedia.")
                else:
                    # Process data
                    results = process_simple(df_orders, kamus_data, file_type)
                    
                    if "error" not in results:
                        st.session_state.results = results
                        st.session_state.processed = True
                        del st.session_state.order_file
                        del st.session_state.kamus_file
                        st.success("✅ Data berhasil diproses!")
                        st.rerun()
                    else:
                        st.error(results["error"])
        
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")

# --- DISPLAY RESULTS ---
if st.session_state.processed and 'results' in st.session_state:
    results = st.session_state.results
    
    # Header metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Items", results['total_items'])
    with col2:
        st.metric("Processing Time", f"{results['processing_time']:.1f}s")
    with col3:
        st.metric("File Type", results['file_type'].capitalize())
    
    # 3 Tabs
    tab1, tab2, tab3 = st.tabs([
        "📊 Summary Order", 
        "📦 SKU untuk Picking", 
        "💾 Download Excel"
    ])
    
    with tab1:
        st.subheader("Summary per Marketplace")
        
        if not results['summary'].empty:
            # Tampilkan summary
            st.dataframe(
                results['summary'],
                use_container_width=True,
                hide_index=True
            )
            
            # Detail order
            st.subheader("Detail Order (Single & Bundle)")
            st.dataframe(
                results['detail'],
                use_container_width=True,
                hide_index=True,
                height=400
            )
        else:
            st.info("Tidak ada data summary")
    
    with tab2:
        st.subheader("Total SKU Component untuk Picking")
        
        if not results['picking'].empty:
            # Info penting
            total_unique_sku = len(results['picking'])
            total_qty = results['picking']['Total Quantity'].sum()
            
            col_a, col_b = st.columns(2)
            with col_a:
                st.metric("Unique SKU", total_unique_sku)
            with col_b:
                st.metric("Total Quantity", total_qty)
            
            # Tampilkan data picking
            st.dataframe(
                results['picking'],
                use_container_width=True,
                hide_index=True
            )
            
            # Highlight yang tanpa product name
            missing_names = results['picking'][results['picking']['Product Name'] == '']
            if not missing_names.empty:
                st.warning(f"⚠️ {len(missing_names)} SKU tanpa Product Name")
                with st.expander("Lihat SKU tanpa Product Name"):
                    st.dataframe(missing_names[['SKU Component', 'Total Quantity']], use_container_width=True)
        else:
            st.info("Tidak ada data untuk picking")
    
    with tab3:
        st.subheader("Download Hasil ke Excel")
        
        if not results['detail'].empty:
            # Generate timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Buat Excel file
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                results['summary'].to_excel(writer, sheet_name='Summary', index=False)
                results['detail'].to_excel(writer, sheet_name='Detail Order', index=False)
                results['picking'].to_excel(writer, sheet_name='SKU untuk Picking', index=False)
            
            # Tombol download
            st.download_button(
                label="📥 Download Excel Report",
                data=output.getvalue(),
                file_name=f"order_report_{timestamp}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
            
            st.success("✅ File Excel siap didownload!")
            st.info("File berisi 3 sheet: Summary, Detail Order, dan SKU untuk Picking")
        else:
            st.info("Tidak ada data untuk didownload")

else:
    # Landing page
    st.info("""
    ## 📋 Cara Pakai:
    
    1. **Upload file order** (Shopee atau Tokopedia/TikTok)
    2. **Upload file kamus** (Excel dengan 3 sheet)
    3. Klik **PROCESS DATA**
    4. Lihat hasil di 3 tab:
       - **Tab 1**: Summary & Detail Order
       - **Tab 2**: SKU untuk Picking
       - **Tab 3**: Download Excel
    
    ### Format Kamus (Excel):
    - Sheet 1: Kurir-Shopee (kolom: Opsi Pengiriman, Instant/Same Day)
    - Sheet 2: Bundle Master (kolom: SKU Bundle, SKU Component, Component Quantity)
    - Sheet 3: SKU Master (kolom: Material, SKU Code, Material description)
    """)

# Footer
st.divider()
st.caption("Simple Order Processor | Hanya 3 tab yang diperlukan")
