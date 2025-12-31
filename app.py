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
    mapping_count = 0
    for _, row in df_sku.iterrows():
        sku_code = str(row[sku_col]) if pd.notna(row[sku_col]) else ""
        product_name = str(row[name_col]) if pd.notna(row[name_col]) else ""
        
        if sku_code and product_name:
            cleaned_sku = clean_sku(sku_code)
            if cleaned_sku and cleaned_sku not in sku_mapping:
                sku_mapping[cleaned_sku] = product_name
                mapping_count += 1
    
    st.sidebar.success(f"✅ Mapping: {mapping_count} SKU → Product Name")
    
    return sku_mapping

# --- FUNGSI BACA FILE KAMUS ---
def read_kamus_file(kamus_file):
    """Baca file kamus Excel dengan 3 sheet"""
    try:
        if kamus_file.name.endswith('.xlsx') or kamus_file.name.endswith('.xls'):
            # Baca semua sheet
            excel_file = pd.ExcelFile(kamus_file, engine='openpyxl')
            sheet_names = excel_file.sheet_names
            
            st.sidebar.info(f"📑 Sheets in Kamus: {sheet_names}")
            
            # Cari sheet yang sesuai
            df_kurir = None
            df_bundle = None
            df_sku = None
            
            # Cari sheet Kurir-Shopee
            for sheet in sheet_names:
                if any(keyword in sheet.lower() for keyword in ['kurir', 'shopee', 'pengiriman']):
                    df_kurir = pd.read_excel(kamus_file, sheet_name=sheet, engine='openpyxl')
                    break
            
            if df_kurir is None and len(sheet_names) > 0:
                df_kurir = pd.read_excel(kamus_file, sheet_name=0, engine='openpyxl')
            
            # Cari sheet Bundle Master
            for sheet in sheet_names:
                if 'bundle' in sheet.lower():
                    df_bundle = pd.read_excel(kamus_file, sheet_name=sheet, engine='openpyxl')
                    break
            
            if df_bundle is None and len(sheet_names) > 1:
                df_bundle = pd.read_excel(kamus_file, sheet_name=1, engine='openpyxl')
            elif df_bundle is None:
                df_bundle = pd.read_excel(kamus_file, sheet_name=0, engine='openpyxl')
            
            # Cari sheet SKU Master
            for sheet in sheet_names:
                if 'sku' in sheet.lower() and 'master' in sheet.lower():
                    df_sku = pd.read_excel(kamus_file, sheet_name=sheet, engine='openpyxl')
                    break
            
            if df_sku is None and len(sheet_names) > 2:
                df_sku = pd.read_excel(kamus_file, sheet_name=2, engine='openpyxl')
            elif df_sku is None and len(sheet_names) > 1:
                df_sku = pd.read_excel(kamus_file, sheet_name=1, engine='openpyxl')
            else:
                df_sku = pd.read_excel(kamus_file, sheet_name=0, engine='openpyxl')
            
            st.sidebar.success(f"✅ Kamus: 3 sheet terbaca")
            
            return {
                'kurir': df_kurir,
                'bundle': df_bundle,
                'sku': df_sku
            }
    except Exception as e:
        st.error(f"Error baca file kamus: {str(e)}")
        return None

# --- FUNGSI BACA FILE TOKPED DENGAN SHEET NAME KHUSUS ---
def read_tokped_file(tokped_file):
    """Baca file Tokped dengan sheet name khusus"""
    try:
        if tokped_file.name.endswith('.csv'):
            df = pd.read_csv(tokped_file)
            st.sidebar.info(f"📄 Tokped: CSV file, {df.shape[0]} rows")
            return df
        else:
            # Baca semua sheet names
            excel_file = pd.ExcelFile(tokped_file, engine='openpyxl')
            sheet_names = excel_file.sheet_names
            
            st.sidebar.info(f"📑 Sheets in Tokped: {sheet_names}")
            
            # Cari sheet "OrderSKUList"
            target_sheet = None
            for sheet in sheet_names:
                if 'orderskulist' in sheet.lower():
                    target_sheet = sheet
                    break
            
            # Jika tidak ketemu, coba sheet lain
            if target_sheet is None:
                for sheet in sheet_names:
                    if any(keyword in sheet.lower() for keyword in ['order', 'sku', 'list']):
                        target_sheet = sheet
                        break
            
            # Jika masih tidak ketemu, gunakan sheet pertama
            if target_sheet is None and len(sheet_names) > 0:
                target_sheet = sheet_names[0]
            
            # Baca sheet
            df = pd.read_excel(tokped_file, sheet_name=target_sheet, engine='openpyxl')
            st.sidebar.success(f"✅ Tokped: Sheet '{target_sheet}' terbaca, {df.shape[0]} rows")
            
            return df
            
    except Exception as e:
        st.error(f"Error membaca file Tokped: {str(e)}")
        return None

# --- FUNGSI BACA FILE SHOPEE ---
def read_shopee_file(shopee_file):
    """Baca file Shopee"""
    try:
        if shopee_file.name.endswith('.csv'):
            df = pd.read_csv(shopee_file)
            st.sidebar.info(f"📄 Shopee: CSV file, {df.shape[0]} rows")
            return df
        else:
            # Baca sheet pertama
            df = pd.read_excel(shopee_file, sheet_name=0, engine='openpyxl')
            st.sidebar.success(f"✅ Shopee: Sheet pertama terbaca, {df.shape[0]} rows")
            return df
    except Exception as e:
        st.error(f"Error membaca file Shopee: {str(e)}")
        return None

# --- FUNGSI PROCESS SHOPEE ---
def process_shopee(df_shopee, kamus_data, sku_mapping):
    """Proses data Shopee dengan filter"""
    expanded_rows = []
    
    if df_shopee is None or df_shopee.empty:
        return expanded_rows
    
    # Ambil data dari kamus
    df_kurir = kamus_data['kurir']
    df_bundle = kamus_data['bundle']
    
    # Clean bundle data
    if 'SKU Bundle' in df_bundle.columns:
        df_bundle['SKU Bundle'] = df_bundle['SKU Bundle'].apply(clean_sku)
    
    # Buat bundle mapping
    bundle_mapping = {}
    component_col = None
    
    # Cari kolom component
    for col in df_bundle.columns:
        if 'component' in col.lower() or 'sku' in col.lower():
            component_col = col
            break
    
    if not component_col and len(df_bundle.columns) > 1:
        component_col = df_bundle.columns[1]
    
    if component_col and 'SKU Bundle' in df_bundle.columns:
        for bundle_sku, group in df_bundle.groupby('SKU Bundle'):
            bundle_mapping[bundle_sku] = []
            for _, row in group.iterrows():
                component_sku = clean_sku(row[component_col])
                # Cari kolom quantity
                qty_cols = [col for col in df_bundle.columns if 'quantity' in col.lower()]
                qty = row.get(qty_cols[0] if qty_cols else 'Component Quantity', 1)
                bundle_mapping[bundle_sku].append((component_sku, qty))
    
    # Standardize column names untuk Shopee
    df_shopee.columns = [str(col).strip() for col in df_shopee.columns]
    
    # Debug: tampilkan kolom yang ada
    st.sidebar.write(f"📦 Kolom Shopee (10 pertama): {list(df_shopee.columns)[:10]}")
    
    # Cari kolom yang diperlukan
    order_id_col = None
    status_col = None
    sku_col = None
    qty_col = None
    
    for col in df_shopee.columns:
        col_lower = col.lower()
        if 'pesanan' in col_lower and ('no' in col_lower or 'id' in col_lower):
            order_id_col = col
        elif 'status' in col_lower:
            status_col = col
        elif 'sku' in col_lower or 'referensi' in col_lower:
            sku_col = col
        elif 'jumlah' in col_lower or 'qty' in col_lower or 'quantity' in col_lower:
            qty_col = col
    
    # Jika tidak ketemu, gunakan default
    if not order_id_col and len(df_shopee.columns) > 0:
        order_id_col = df_shopee.columns[0]
    
    if not sku_col and len(df_shopee.columns) > 1:
        # Coba cari kolom yang mungkin berisi SKU
        for col in df_shopee.columns:
            if any(keyword in col.lower() for keyword in ['produk', 'nama', 'item']):
                sku_col = col
                break
        if not sku_col:
            sku_col = df_shopee.columns[1]
    
    if not qty_col and len(df_shopee.columns) > 2:
        qty_col = df_shopee.columns[2]
    
    st.sidebar.write(f"🔍 Shopee cols: order={order_id_col}, sku={sku_col}, qty={qty_col}")
    
    # Filter untuk Shopee
    df_filtered = df_shopee.copy()
    
    # Filter status jika ada
    if status_col:
        df_filtered = df_filtered[
            df_filtered[status_col].astype(str).str.upper().str.contains('PERLU DIKIRIM')
        ]
    
    # Filter managed by shopee jika ada
    managed_cols = [col for col in df_filtered.columns if 'kelola' in col.lower() or 'managed' in col.lower()]
    if managed_cols:
        managed_col = managed_cols[0]
        df_filtered = df_filtered[
            df_filtered[managed_col].astype(str).str.upper().str.contains('NO')
        ]
    
    # Filter no resi jika ada
    resi_cols = [col for col in df_filtered.columns if 'resi' in col.lower() or 'tracking' in col.lower()]
    if resi_cols:
        resi_col = resi_cols[0]
        df_filtered = df_filtered[
            df_filtered[resi_col].isna() | (df_filtered[resi_col].astype(str).str.strip() == '')
        ]
    
    # Filter instant kurir jika ada
    shipping_cols = [col for col in df_filtered.columns if 'pengiriman' in col.lower() or 'shipping' in col.lower()]
    if shipping_cols:
        shipping_col = shipping_cols[0]
        # Get instant/same day kurir
        df_kurir['Opsi Pengiriman'] = df_kurir.iloc[:, 0].astype(str).str.strip()
        df_kurir['Instant/Same Day'] = df_kurir.iloc[:, 1].astype(str).str.strip()
        
        instant_kurir = df_kurir[
            df_kurir['Instant/Same Day'].str.upper().str.contains('YES')
        ]['Opsi Pengiriman'].unique()
        
        df_filtered = df_filtered[df_filtered[shipping_col].isin(instant_kurir)]
    
    if df_filtered.empty:
        st.sidebar.warning("⚠️ Tidak ada data Shopee setelah filter")
        return expanded_rows
    
    # Process each row
    for _, row in df_filtered.iterrows():
        # Ambil SKU
        sku_value = ""
        if sku_col and sku_col in row and pd.notna(row[sku_col]):
            sku_value = str(row[sku_col])
        
        sku_awal = clean_sku(sku_value)
        
        if not sku_awal:
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
        
        if sku_awal in bundle_mapping:
            # Bundle
            for component_sku, comp_qty in bundle_mapping[sku_awal]:
                expanded_rows.append({
                    'Marketplace': 'Shopee',
                    'Order ID': order_id,
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
                'Order ID': order_id,
                'Original SKU': sku_awal,
                'Is Bundle': 'No',
                'SKU Component': sku_awal,
                'Product Name': sku_mapping.get(sku_awal, ''),
                'Quantity': qty
            })
    
    st.sidebar.info(f"✅ Shopee: {len(expanded_rows)} items processed")
    return expanded_rows

# --- FUNGSI PROCESS TOKPED ---
def process_tokped(df_tokped, kamus_data, sku_mapping):
    """Proses data Tokopedia/TikTok TANPA FILTER"""
    expanded_rows = []
    
    if df_tokped is None or df_tokped.empty:
        return expanded_rows
    
    # Ambil bundle data dari kamus
    df_bundle = kamus_data['bundle']
    
    # Clean bundle data
    if 'SKU Bundle' in df_bundle.columns:
        df_bundle['SKU Bundle'] = df_bundle['SKU Bundle'].apply(clean_sku)
    
    # Buat bundle mapping
    bundle_mapping = {}
    component_col = None
    
    # Cari kolom component
    for col in df_bundle.columns:
        if 'component' in col.lower() or 'sku' in col.lower():
            component_col = col
            break
    
    if not component_col and len(df_bundle.columns) > 1:
        component_col = df_bundle.columns[1]
    
    if component_col and 'SKU Bundle' in df_bundle.columns:
        for bundle_sku, group in df_bundle.groupby('SKU Bundle'):
            bundle_mapping[bundle_sku] = []
            for _, row in group.iterrows():
                component_sku = clean_sku(row[component_col])
                # Cari kolom quantity
                qty_cols = [col for col in df_bundle.columns if 'quantity' in col.lower()]
                qty = row.get(qty_cols[0] if qty_cols else 'Component Quantity', 1)
                bundle_mapping[bundle_sku].append((component_sku, qty))
    
    # Standardize column names untuk Tokped
    df_tokped.columns = [str(col).strip() for col in df_tokped.columns]
    
    # Debug: tampilkan kolom yang ada
    st.sidebar.write(f"🛍️ Kolom Tokped (10 pertama): {list(df_tokped.columns)[:10]}")
    
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
        elif 'quantity' in col_lower and 'sku' not in col_lower:  # Hindari "Sku Quantity of return"
            qty_col = col
    
    # Coba alternatif
    if not sku_col:
        for col in df_tokped.columns:
            if 'sku' in col.lower():
                sku_col = col
                break
    
    if not qty_col:
        for col in df_tokped.columns:
            if 'qty' in col.lower() or 'quantity' in col.lower():
                qty_col = col
                break
    
    st.sidebar.write(f"🔍 Tokped cols: order={order_id_col}, sku={sku_col}, qty={qty_col}")
    
    # **PENTING: TIDAK ADA FILTER UNTUK TOKPED**
    # Langsung proses semua data karena sudah difilter dari MP
    df_filtered = df_tokped
    
    # Jika tidak ada kolom yang diperlukan, gunakan default
    if not order_id_col and len(df_filtered.columns) > 0:
        order_id_col = df_filtered.columns[0]
    
    if not sku_col and len(df_filtered.columns) > 1:
        sku_col = df_filtered.columns[1]
    
    if not qty_col and len(df_filtered.columns) > 2:
        qty_col = df_filtered.columns[2]
    
    # Process each row
    processed_count = 0
    for idx, row in df_filtered.iterrows():
        # Ambil SKU
        sku_value = ""
        if sku_col and sku_col in row and pd.notna(row[sku_col]):
            sku_value = str(row[sku_col])
        
        sku_awal = clean_sku(sku_value)
        
        if not sku_awal:
            # Coba kolom lain untuk SKU
            for col in df_filtered.columns:
                if col not in [order_id_col, qty_col] and pd.notna(row[col]):
                    cleaned = clean_sku(str(row[col]))
                    if cleaned:
                        sku_awal = cleaned
                        break
        
        if not sku_awal:
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
        
        if sku_awal in bundle_mapping:
            # Bundle
            for component_sku, comp_qty in bundle_mapping[sku_awal]:
                expanded_rows.append({
                    'Marketplace': 'Tokopedia/TikTok',
                    'Order ID': order_id,
                    'Original SKU': sku_awal,
                    'Is Bundle': 'Yes',
                    'SKU Component': component_sku,
                    'Product Name': sku_mapping.get(component_sku, ''),
                    'Quantity': qty * comp_qty
                })
                processed_count += 1
        else:
            # Single
            expanded_rows.append({
                'Marketplace': 'Tokopedia/TikTok',
                'Order ID': order_id,
                'Original SKU': sku_awal,
                'Is Bundle': 'No',
                'SKU Component': sku_awal,
                'Product Name': sku_mapping.get(sku_awal, ''),
                'Quantity': qty
            })
            processed_count += 1
    
    st.sidebar.success(f"✅ Tokped: {processed_count} items processed")
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
            df_shopee = read_shopee_file(shopee_file)
            
            if df_shopee is not None and not df_shopee.empty:
                # Proses Shopee
                shopee_rows = process_shopee(df_shopee, kamus_data, sku_mapping)
                all_expanded_rows.extend(shopee_rows)
                st.sidebar.info(f"Shopee: {len(shopee_rows)} items")
            else:
                st.sidebar.warning("Shopee: File kosong atau error")
                
        except Exception as e:
            st.error(f"Error processing Shopee: {str(e)}")
    
    # Proses Tokped jika ada
    if tokped_file is not None:
        try:
            # Baca file Tokped dengan sheet name khusus
            df_tokped = read_tokped_file(tokped_file)
            
            if df_tokped is not None and not df_tokped.empty:
                # Proses Tokped
                tokped_rows = process_tokped(df_tokped, kamus_data, sku_mapping)
                all_expanded_rows.extend(tokped_rows)
                st.sidebar.info(f"Tokped: {len(tokped_rows)} items")
            else:
                st.sidebar.warning("Tokped: File kosong atau error")
                
        except Exception as e:
            st.error(f"Error processing Tokped: {str(e)}")
            import traceback
            st.code(traceback.format_exc())
    
    # Jika tidak ada data
    if not all_expanded_rows:
        return {"error": "❌ Tidak ada data yang berhasil diproses. Periksa:\n1. File format dan sheet name\n2. Kolom yang diperlukan ada\n3. File tidak kosong"}
    
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
    
    # Hitung per marketplace
    shopee_count = len([r for r in all_expanded_rows if r['Marketplace'] == 'Shopee'])
    tokped_count = len([r for r in all_expanded_rows if r['Marketplace'] == 'Tokopedia/TikTok'])
    
    return {
        'summary': df_summary,
        'detail': df_expanded,
        'picking': df_picking,
        'processing_time': processing_time,
        'total_items': len(df_expanded),
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
        help="File order dari Tokopedia atau TikTok (sheet: OrderSKUList)",
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
    
    st.caption("Version: Support Sheet Name 'OrderSKUList'")

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
            import traceback
            st.code(traceback.format_exc())

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
                st.metric("Total Quantity", int(total_qty))
            
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
        
        1. **Upload file Shopee** (opsional - filter: Perlu Dikirim + No Resi + Instant)
        2. **Upload file Tokopedia/TikTok** (opsional - sheet name: OrderSKUList)
        3. **Upload file kamus** (wajib - Excel dengan 3 sheet)
        4. Klik **PROCESS ALL DATA**
        5. Lihat hasil di 3 tab
        
        ### **Catatan Penting:**
        - **Tokped**: Sheet name harus "OrderSKUList" (case insensitive)
        - **Shopee**: Tanpa sheet name khusus
        - **Kamus**: 3 sheet (Kurir, Bundle, SKU Master)
        
        ### Kolom yang Dicari:
        - **Shopee**: No. Pesanan, Status Pesanan, SKU, Jumlah
        - **Tokped**: Order ID, Seller SKU, Quantity
        """)
    
    with col2:
        st.image("https://cdn-icons-png.flaticon.com/512/3208/3208720.png", width=150)

# Footer
st.divider()
st.caption("✅ Support Sheet Name: 'OrderSKUList' untuk Tokopedia/TikTok")
