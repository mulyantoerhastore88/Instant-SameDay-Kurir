import streamlit as st
import pandas as pd
import numpy as np
import io
import time
from datetime import datetime

# Page config
st.set_page_config(
    page_title="Order Processor - Final",
    page_icon="📦",
    layout="wide"
)

# Title
st.title("📦 Order Processor - Final Version")
st.markdown("Upload file Shopee & Tokopedia, langsung proses & download!")

# Initialize session state
if 'processed' not in st.session_state:
    st.session_state.processed = False
if 'results' not in st.session_state:
    st.session_state.results = {}

# --- FUNGSI CLEANING SKU ---
def clean_sku_for_lookup(sku):
    """Bersihkan SKU untuk lookup: ambil bagian setelah hyphen pertama."""
    if pd.isna(sku):
        return ""
    
    sku_str = str(sku).strip()
    
    # Jika ada hyphen, ambil bagian setelah hyphen pertama
    if '-' in sku_str:
        # Split hanya pada hyphen pertama
        parts = sku_str.split('-', 1)
        if len(parts) > 1:
            cleaned = parts[1].strip()
            # Hapus hyphen tambahan jika ada
            if '-' in cleaned:
                cleaned = cleaned.split('-')[0].strip()
            return cleaned
    
    return sku_str

def get_original_sku_for_display(sku):
    """Ambil SKU original untuk display (khusus FG- dan CS- tetap full)."""
    if pd.isna(sku):
        return ""
    
    sku_str = str(sku).strip()
    return sku_str

# --- FUNGSI LOOKUP PRODUCT NAME ---
def create_sku_mapping(df_sku):
    """Buat mapping dari SKU Master (kolom B ke C)"""
    sku_mapping = {}
    
    if df_sku.empty:
        return sku_mapping
    
    # Ambil kolom B (index 1) dan C (index 2)
    if len(df_sku.columns) >= 3:
        sku_col = df_sku.columns[1]  # Kolom B
        name_col = df_sku.columns[2]  # Kolom C
    elif len(df_sku.columns) >= 2:
        sku_col = df_sku.columns[0]
        name_col = df_sku.columns[1]
    else:
        return sku_mapping
    
    # Buat mapping dengan cleaned SKU
    for _, row in df_sku.iterrows():
        sku_code = str(row[sku_col]) if pd.notna(row[sku_col]) else ""
        product_name = str(row[name_col]) if pd.notna(row[name_col]) else ""
        
        if sku_code and product_name:
            cleaned_sku = clean_sku_for_lookup(sku_code)
            if cleaned_sku and cleaned_sku not in sku_mapping:
                sku_mapping[cleaned_sku] = product_name
    
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
    """Proses data Shopee dengan 5 filter"""
    expanded_rows = []
    
    if df_shopee is None or df_shopee.empty:
        return expanded_rows
    
    # Ambil data dari kamus
    df_kurir = kamus_data['kurir']
    df_bundle = kamus_data['bundle']
    
    # Clean bundle data untuk lookup
    if 'SKU Bundle' in df_bundle.columns:
        df_bundle['SKU Bundle Cleaned'] = df_bundle['SKU Bundle'].apply(clean_sku_for_lookup)
        df_bundle['SKU Bundle Original'] = df_bundle['SKU Bundle'].apply(get_original_sku_for_display)
    
    # Buat bundle mapping
    bundle_mapping = {}
    component_col = None
    
    # Cari kolom component
    for col in df_bundle.columns:
        if 'component' in col.lower() and 'sku' in col.lower():
            component_col = col
            break
    
    if not component_col:
        for col in df_bundle.columns:
            if 'sku' in col.lower() and col != 'SKU Bundle':
                component_col = col
                break
    
    if not component_col and len(df_bundle.columns) > 1:
        component_col = df_bundle.columns[1]
    
    if component_col and 'SKU Bundle Cleaned' in df_bundle.columns:
        for bundle_sku_cleaned, group in df_bundle.groupby('SKU Bundle Cleaned'):
            bundle_original = group.iloc[0]['SKU Bundle Original'] if 'SKU Bundle Original' in group.columns else bundle_sku_cleaned
            bundle_mapping[bundle_sku_cleaned] = {
                'original': bundle_original,
                'components': []
            }
            for _, row in group.iterrows():
                component_sku = str(row[component_col]) if pd.notna(row[component_col]) else ""
                # Cari kolom quantity
                qty_cols = [col for col in df_bundle.columns if 'quantity' in col.lower()]
                qty = row.get(qty_cols[0] if qty_cols else 'Component Quantity', 1)
                bundle_mapping[bundle_sku_cleaned]['components'].append((component_sku, qty))
    
    # Standardize column names untuk Shopee
    df_shopee.columns = [str(col).strip() for col in df_shopee.columns]
    
    # Cari kolom yang diperlukan
    order_id_col = None
    status_col = None
    managed_col = None
    sku_col = None
    qty_col = None
    shipping_col = None
    resi_col = None
    
    for col in df_shopee.columns:
        col_lower = col.lower()
        if 'pesanan' in col_lower and ('no' in col_lower or 'id' in col_lower):
            order_id_col = col
        elif 'status' in col_lower:
            status_col = col
        elif 'kelola' in col_lower or 'managed' in col_lower:
            managed_col = col
        elif 'sku' in col_lower or 'referensi' in col_lower:
            sku_col = col
        elif 'jumlah' in col_lower or 'qty' in col_lower:
            qty_col = col
        elif 'pengiriman' in col_lower or 'shipping' in col_lower:
            shipping_col = col
        elif 'resi' in col_lower:
            resi_col = col
    
    # Jika tidak ketemu, gunakan default
    if not order_id_col and len(df_shopee.columns) > 0:
        order_id_col = df_shopee.columns[0]
    if not sku_col and len(df_shopee.columns) > 1:
        sku_col = df_shopee.columns[1]
    if not qty_col and len(df_shopee.columns) > 2:
        qty_col = df_shopee.columns[2]
    
    # Filter 1: Status = "Perlu Dikirim"
    df_filtered = df_shopee.copy()
    if status_col:
        df_filtered = df_filtered[
            df_filtered[status_col].astype(str).str.upper().str.contains('PERLU DIKIRIM')
        ]
    
    # Filter 2 & 5: Dikelola Shopee = "No"
    if managed_col:
        df_filtered = df_filtered[
            df_filtered[managed_col].astype(str).str.upper().str.contains('NO')
        ]
    
    # Filter 3: No Resi = kosong
    if resi_col:
        df_filtered = df_filtered[
            df_filtered[resi_col].isna() | 
            (df_filtered[resi_col].astype(str).str.strip() == '') |
            (df_filtered[resi_col].astype(str).str.lower() == 'nan')
        ]
    
    # Filter 4: Opsi Pengiriman = Instant/Same Day
    if shipping_col:
        # Get instant/same day kurir
        df_kurir['Opsi Pengiriman'] = df_kurir.iloc[:, 0].astype(str).str.strip()
        if len(df_kurir.columns) > 1:
            df_kurir['Instant/Same Day'] = df_kurir.iloc[:, 1].astype(str).str.strip()
        else:
            df_kurir['Instant/Same Day'] = 'No'
        
        instant_kurir = df_kurir[
            df_kurir['Instant/Same Day'].str.upper().str.contains('YES|YA|1|TRUE')
        ]['Opsi Pengiriman'].unique()
        
        df_filtered = df_filtered[df_filtered[shipping_col].isin(instant_kurir)]
    
    if df_filtered.empty:
        return expanded_rows
    
    # Process each row
    for _, row in df_filtered.iterrows():
        # Ambil SKU original
        sku_original = ""
        if sku_col and sku_col in row and pd.notna(row[sku_col]):
            sku_original = str(row[sku_col])
        
        sku_cleaned = clean_sku_for_lookup(sku_original)
        
        if not sku_cleaned:
            continue
            
        # Ambil quantity
        qty = 1.0
        if qty_col and qty_col in row and pd.notna(row[qty_col]):
            try:
                qty = float(row[qty_col])
            except:
                qty = 1.0
        
        # Ambil order ID
        order_id = ""
        if order_id_col and order_id_col in row and pd.notna(row[order_id_col]):
            order_id = str(row[order_id_col])
        
        # Cek apakah bundle
        if sku_cleaned in bundle_mapping:
            # Bundle ditemukan
            bundle_info = bundle_mapping[sku_cleaned]
            for component_sku, comp_qty in bundle_info['components']:
                component_cleaned = clean_sku_for_lookup(component_sku)
                expanded_rows.append({
                    'Marketplace': 'Shopee',
                    'Order ID': order_id,
                    'Original SKU': sku_original,
                    'Cleaned SKU': sku_cleaned,
                    'Product Name': sku_mapping.get(sku_cleaned, ''),
                    'Quantity': qty,
                    'Bundle Y/N': 'Y',
                    'Component SKU': component_sku,
                    'Quantity Final': qty * comp_qty
                })
        else:
            # Single item
            expanded_rows.append({
                'Marketplace': 'Shopee',
                'Order ID': order_id,
                'Original SKU': sku_original,
                'Cleaned SKU': sku_cleaned,
                'Product Name': sku_mapping.get(sku_cleaned, ''),
                'Quantity': qty,
                'Bundle Y/N': 'N',
                'Component SKU': sku_cleaned,
                'Quantity Final': qty
            })
    
    return expanded_rows

# --- FUNGSI PROCESS TOKPED ---
def process_tokped(df_tokped, kamus_data, sku_mapping):
    """Proses data Tokopedia/TikTok TANPA FILTER"""
    expanded_rows = []
    
    if df_tokped is None or df_tokped.empty:
        return expanded_rows
    
    # Ambil bundle data dari kamus
    df_bundle = kamus_data['bundle']
    
    # Clean bundle data untuk lookup
    if 'SKU Bundle' in df_bundle.columns:
        df_bundle['SKU Bundle Cleaned'] = df_bundle['SKU Bundle'].apply(clean_sku_for_lookup)
        df_bundle['SKU Bundle Original'] = df_bundle['SKU Bundle'].apply(get_original_sku_for_display)
    
    # Buat bundle mapping
    bundle_mapping = {}
    component_col = None
    
    # Cari kolom component
    for col in df_bundle.columns:
        if 'component' in col.lower() and 'sku' in col.lower():
            component_col = col
            break
    
    if not component_col:
        for col in df_bundle.columns:
            if 'sku' in col.lower() and col != 'SKU Bundle':
                component_col = col
                break
    
    if not component_col and len(df_bundle.columns) > 1:
        component_col = df_bundle.columns[1]
    
    if component_col and 'SKU Bundle Cleaned' in df_bundle.columns:
        for bundle_sku_cleaned, group in df_bundle.groupby('SKU Bundle Cleaned'):
            bundle_original = group.iloc[0]['SKU Bundle Original'] if 'SKU Bundle Original' in group.columns else bundle_sku_cleaned
            bundle_mapping[bundle_sku_cleaned] = {
                'original': bundle_original,
                'components': []
            }
            for _, row in group.iterrows():
                component_sku = str(row[component_col]) if pd.notna(row[component_col]) else ""
                # Cari kolom quantity
                qty_cols = [col for col in df_bundle.columns if 'quantity' in col.lower()]
                qty = row.get(qty_cols[0] if qty_cols else 'Component Quantity', 1)
                bundle_mapping[bundle_sku_cleaned]['components'].append((component_sku, qty))
    
    # Standardize column names untuk Tokped
    df_tokped.columns = [str(col).strip() for col in df_tokped.columns]
    
    # Cari kolom yang diperlukan
    order_id_col = None
    sku_col = None
    qty_col = None
    
    for col in df_tokped.columns:
        col_lower = col.lower()
        if 'order' in col_lower and 'id' in col_lower:
            order_id_col = col
        elif 'seller' in col_lower and 'sku' in col_lower:
            sku_col = col
        elif 'quantity' in col_lower and 'sku' not in col_lower:
            qty_col = col
    
    # Coba alternatif
    if not sku_col:
        for col in df_tokped.columns:
            if 'sku' in col.lower():
                sku_col = col
                break
    
    if not qty_col:
        for col in df_tokped.columns:
            if 'qty' in col.lower():
                qty_col = col
                break
    
    # Jika tidak ada kolom yang diperlukan, gunakan default
    if not order_id_col and len(df_tokped.columns) > 0:
        order_id_col = df_tokped.columns[0]
    if not sku_col and len(df_tokped.columns) > 1:
        sku_col = df_tokped.columns[1]
    if not qty_col and len(df_tokped.columns) > 2:
        qty_col = df_tokped.columns[2]
    
    # **TIDAK ADA FILTER UNTUK TOKPED**
    df_filtered = df_tokped
    
    # Process each row
    for idx, row in df_filtered.iterrows():
        # Ambil SKU original
        sku_original = ""
        if sku_col and sku_col in row and pd.notna(row[sku_col]):
            sku_original = str(row[sku_col])
        
        sku_cleaned = clean_sku_for_lookup(sku_original)
        
        if not sku_cleaned:
            continue
            
        # Ambil quantity
        qty = 1.0
        if qty_col and qty_col in row and pd.notna(row[qty_col]):
            try:
                qty = float(row[qty_col])
            except:
                qty = 1.0
        
        # Ambil order ID
        order_id = f"TOKPED_{idx+1}"  # Default jika tidak ada
        if order_id_col and order_id_col in row and pd.notna(row[order_id_col]):
            order_id = str(row[order_id_col])
        
        # Cek apakah bundle
        if sku_cleaned in bundle_mapping:
            # Bundle ditemukan
            bundle_info = bundle_mapping[sku_cleaned]
            for component_sku, comp_qty in bundle_info['components']:
                component_cleaned = clean_sku_for_lookup(component_sku)
                expanded_rows.append({
                    'Marketplace': 'Tokopedia/TikTok',
                    'Order ID': order_id,
                    'Original SKU': sku_original,
                    'Cleaned SKU': sku_cleaned,
                    'Product Name': sku_mapping.get(sku_cleaned, ''),
                    'Quantity': qty,
                    'Bundle Y/N': 'Y',
                    'Component SKU': component_sku,
                    'Quantity Final': qty * comp_qty
                })
        else:
            # Single item
            expanded_rows.append({
                'Marketplace': 'Tokopedia/TikTok',
                'Order ID': order_id,
                'Original SKU': sku_original,
                'Cleaned SKU': sku_cleaned,
                'Product Name': sku_mapping.get(sku_cleaned, ''),
                'Quantity': qty,
                'Bundle Y/N': 'N',
                'Component SKU': sku_cleaned,
                'Quantity Final': qty
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
                df_shopee = pd.read_excel(shopee_file, sheet_name=0, engine='openpyxl')
            
            # Proses Shopee
            shopee_rows = process_shopee(df_shopee, kamus_data, sku_mapping)
            all_expanded_rows.extend(shopee_rows)
            
        except Exception as e:
            st.error(f"Error processing Shopee: {str(e)}")
    
    # Proses Tokped jika ada
    if tokped_file is not None:
        try:
            # Baca file Tokped
            if tokped_file.name.endswith('.csv'):
                df_tokped = pd.read_csv(tokped_file)
            else:
                df_tokped = pd.read_excel(tokped_file, sheet_name=0, engine='openpyxl')
            
            # Proses Tokped
            tokped_rows = process_tokped(df_tokped, kamus_data, sku_mapping)
            all_expanded_rows.extend(tokped_rows)
            
        except Exception as e:
            st.error(f"Error processing Tokped: {str(e)}")
    
    # Jika tidak ada data
    if not all_expanded_rows:
        return {"error": "❌ Tidak ada data yang berhasil diproses."}
    
    # Buat DataFrame hasil detail order
    df_detail = pd.DataFrame(all_expanded_rows)
    
    # --- BUAT TAB 1: SUMMARY DETAIL ORDER ---
    # Sudah ada di df_detail dengan format yang diminta
    
    # --- BUAT TAB 2: SKU UNTUK PICKING ---
    if not df_detail.empty:
        picking_data = []
        
        for _, row in df_detail.iterrows():
            component_sku = row['Component SKU']
            sku_original = row['Original SKU']
            quantity_final = row['Quantity Final']
            marketplace = row['Marketplace']
            
            # Untuk FG- dan CS-: tampilkan full dengan strip di SKU Component
            # Untuk lainnya: gunakan cleaned version
            if str(sku_original).upper().startswith(('FG-', 'CS-')):
                sku_component = sku_original
                sku_original_display = sku_original
            else:
                sku_component = clean_sku_for_lookup(component_sku)
                sku_original_display = sku_original
            
            # Cari product name dari cleaned SKU
            cleaned_for_lookup = clean_sku_for_lookup(component_sku)
            product_name = sku_mapping.get(cleaned_for_lookup, '')
            
            picking_data.append({
                'SKU Component': sku_component,
                'SKU Original': sku_original_display,
                'Product Name': product_name,
                'Quantity': quantity_final,
                'Marketplace': marketplace
            })
        
        df_picking_raw = pd.DataFrame(picking_data)
        
        # Group untuk picking table
        df_picking = df_picking_raw.groupby(['SKU Component', 'SKU Original', 'Product Name']).agg({
            'Quantity': 'sum',
            'Marketplace': lambda x: ', '.join(sorted(set(x)))
        }).reset_index()
        
        df_picking = df_picking.rename(columns={
            'Quantity': 'Total Quantity',
            'Marketplace': 'Used In Marketplace'
        }).sort_values('Total Quantity', ascending=False)
    else:
        df_picking = pd.DataFrame(columns=['SKU Component', 'SKU Original', 'Product Name', 'Total Quantity', 'Used In Marketplace'])
    
    processing_time = time.time() - start_time
    
    # Hitung per marketplace
    shopee_count = len([r for r in all_expanded_rows if r['Marketplace'] == 'Shopee'])
    tokped_count = len([r for r in all_expanded_rows if r['Marketplace'] == 'Tokopedia/TikTok'])
    
    return {
        'detail': df_detail,
        'picking': df_picking,
        'processing_time': processing_time,
        'total_items': len(df_detail),
        'shopee_items': shopee_count,
        'tokped_items': tokped_count
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
        help="File order dari Tokopedia/TikTok (opsional)",
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
    
    st.caption("Final Version | Clean SKU Logic")

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
                    for key in ['shopee_file', 'tokped_file', 'kamus_file']:
                        if hasattr(st.session_state, key):
                            del st.session_state[key]
                    
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
    
    # 2 Tabs seperti yang diminta
    tab1, tab2 = st.tabs([
        "📋 Summary Detail Order", 
        "📦 SKU untuk Picking"
    ])
    
    with tab1:
        st.subheader("Detail Order (Expanded)")
        
        if not results['detail'].empty:
            # Tampilkan dataframe dengan format yang diminta
            display_cols = [
                'Marketplace', 'Order ID', 'Original SKU', 'Cleaned SKU',
                'Product Name', 'Quantity', 'Bundle Y/N', 'Component SKU',
                'Quantity Final'
            ]
            
            # Pastikan kolom ada
            available_cols = [col for col in display_cols if col in results['detail'].columns]
            
            st.dataframe(
                results['detail'][available_cols],
                use_container_width=True,
                hide_index=True,
                height=500
            )
            
            # Summary stats
            col_a, col_b, col_c = st.columns(3)
            with col_a:
                bundle_count = results['detail'][results['detail']['Bundle Y/N'] == 'Y'].shape[0]
                st.metric("Bundle Items", bundle_count)
            with col_b:
                single_count = results['detail'][results['detail']['Bundle Y/N'] == 'N'].shape[0]
                st.metric("Single Items", single_count)
            with col_c:
                orders_count = results['detail']['Order ID'].nunique()
                st.metric("Unique Orders", orders_count)
        else:
            st.info("Tidak ada data detail order")
    
    with tab2:
        st.subheader("SKU untuk Picking")
        
        if not results['picking'].empty:
            # Info penting
            total_unique_sku = len(results['picking'])
            total_qty = results['picking']['Total Quantity'].sum()
            
            col_x, col_y = st.columns(2)
            with col_x:
                st.metric("Unique SKU", total_unique_sku)
            with col_y:
                st.metric("Total Quantity", int(total_qty))
            
            # Highlight FG- dan CS-
            fg_cs_items = results['picking'][
                results['picking']['SKU Component'].astype(str).str.upper().str.startswith(('FG-', 'CS-'))
            ]
            
            if not fg_cs_items.empty:
                st.info(f"📝 **Catatan Picker:** {len(fg_cs_items)} item FG- (Free Goods) dan CS- (Clearance Sale) harus diambil dengan strip!")
            
            # Tampilkan data picking
            st.dataframe(
                results['picking'],
                use_container_width=True,
                hide_index=True,
                height=500
            )
            
            # Download button untuk picking list
            st.subheader("💾 Download Picking List")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            csv_picking = results['picking'].to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📥 Download Picking List (CSV)",
                data=csv_picking,
                file_name=f"picking_list_{timestamp}.csv",
                mime="text/csv",
                use_container_width=True
            )
        else:
            st.info("Tidak ada data untuk picking")
    
    # Additional download options
    st.divider()
    st.subheader("📊 Download Full Report")
    
    if not results['detail'].empty:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Buat Excel file dengan 2 sheet
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            results['detail'].to_excel(writer, sheet_name='Detail Order', index=False)
            results['picking'].to_excel(writer, sheet_name='SKU untuk Picking', index=False)
        
        st.download_button(
            label="📥 DOWNLOAD FULL EXCEL REPORT",
            data=output.getvalue(),
            file_name=f"order_report_{timestamp}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary"
        )

else:
    # Landing page
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.info("""
        ## 📋 Cara Pakai:
        
        1. **Upload file Shopee** (opsional - dengan 5 filter)
        2. **Upload file Tokopedia/TikTok** (opsional - TANPA FILTER)
        3. **Upload file kamus** (wajib - Excel 3 sheet)
        4. Klik **PROCESS ALL DATA**
        5. Lihat hasil di 2 tab
        
        ### **Filter Shopee:**
        1. Status = "Perlu Dikirim"
        2. Dikelola Shopee = "No"
        3. No Resi = kosong
        4. Opsi Pengiriman = Instant/Same Day
        5. Double check Pesanan yang Dikelola Shopee = No
        
        ### **SKU Cleaning Rules:**
        - VR-12345-AB → Cleaned: 12345
        - FG-67890 → Tetap full: FG-67890 (untuk picker)
        - CS-ABCDE → Tetap full: CS-ABCDE (untuk picker)
        """)
    
    with col2:
        st.image("https://cdn-icons-png.flaticon.com/512/3208/3208720.png", width=150)

# Footer
st.divider()
st.caption("Final Version | SKU Cleaning Logic | 2 Tab Output")
