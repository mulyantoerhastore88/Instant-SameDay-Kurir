import streamlit as st
import pandas as pd
import numpy as np
import io
import time
import chardet
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# --- CONFIG ---
st.set_page_config(page_title="Universal Order Processor", layout="wide")
st.title("🛒 Universal Marketplace Order Processor")
st.markdown("""
**Logic Applied:**
1. **Shopee**: Status='Perlu Dikirim' | Resi=Blank | Managed='No' | Kurir=Instant(Kamus).
2. **Tokopedia**: Status='Perlu Dikirim'.
3. **SKU Logic**: Prefix **FG-** & **CS-** dipertahankan, sisanya ambil suffix.
""")

# --- DEBUG MODE ---
st.sidebar.header("🔧 Debug Mode")
DEBUG_MODE = st.sidebar.checkbox("Tampilkan info detil", value=False)

# --- FUNGSI CLEANING SKU (UPDATED) ---
def clean_sku(sku):
    """
    Logic:
    1. Jika awalan 'FG-' atau 'CS-', biarkan apa adanya (hanya trim spasi).
    2. Jika tidak, ambil bagian kanan setelah hyphen (-).
    """
    if pd.isna(sku): return ""
    sku = str(sku).strip()
    # Hapus karakter aneh (non-printable)
    sku = ''.join(char for char in sku if ord(char) >= 32)
    
    sku_upper = sku.upper()
    
    # KECUALIAN: FG- dan CS- jangan dipotong
    if sku_upper.startswith('FG-') or sku_upper.startswith('CS-'):
        return sku
        
    # Logic Default: Ambil kanan
    if '-' in sku:
        return sku.split('-', 1)[-1].strip()
        
    return sku

# --- FUNGSI DETEKSI ENCODING ---
def detect_encoding(file_obj):
    """Deteksi encoding file"""
    sample = file_obj.read(10000)
    file_obj.seek(0)
    result = chardet.detect(sample)
    encoding = result['encoding']
    if DEBUG_MODE:
        st.sidebar.info(f"Detected encoding: {encoding} (confidence: {result['confidence']:.2f})")
    return encoding

# --- FUNGSI SMART LOADER (AGRESIVE SEPARATOR CHECK) ---
def load_data_smart(file_obj):
    """
    Mencoba membaca file dengan prioritas Excel -> CSV.
    Otomatis cek separator (, ; \t) jika kolom cuma 1.
    """
    df = None
    filename = file_obj.name.lower()
    file_display_name = file_obj.name
    
    if DEBUG_MODE:
        st.sidebar.subheader(f"📂 Processing: {file_display_name}")
    
    try:
        # A. COBA BACA EXCEL
        if filename.endswith('.xlsx') or filename.endswith('.xls'):
            try:
                # Coba baca semua sheet pertama
                df = pd.read_excel(file_obj, dtype=str, header=None, engine='openpyxl')
                if DEBUG_MODE:
                    st.sidebar.success(f"✓ Excel loaded: {df.shape[0]} rows, {df.shape[1]} cols")
                    st.sidebar.text(f"First row: {df.iloc[0].tolist()[:5]}")
            except Exception as e:
                if DEBUG_MODE:
                    st.sidebar.warning(f"Excel failed: {str(e)[:100]}")
                df = None

        # B. COBA BACA CSV (Jika Excel gagal atau file .csv)
        if df is None or df.shape[1] <= 1:
            file_obj.seek(0)
            
            # Deteksi encoding
            try:
                encoding = detect_encoding(file_obj)
            except:
                encoding = 'utf-8'
            
            # Prioritaskan UTF-8 dengan BOM untuk Tokopedia
            encodings_to_try = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            if encoding and encoding not in encodings_to_try:
                encodings_to_try.insert(0, encoding)
            
            separators = [',', ';', '\t', '|']
            
            for enc in encodings_to_try:
                if df is not None and df.shape[1] > 1:
                    break
                    
                for sep in separators:
                    try:
                        file_obj.seek(0)
                        temp_df = pd.read_csv(
                            file_obj, 
                            sep=sep, 
                            dtype=str, 
                            header=None, 
                            encoding=enc,
                            on_bad_lines='skip',
                            quotechar='"',
                            skipinitialspace=True
                        )
                        
                        # Cek: Apakah kolom lebih dari 1?
                        if temp_df.shape[1] > 1:
                            df = temp_df
                            if DEBUG_MODE:
                                st.sidebar.success(f"✓ CSV loaded: encoding={enc}, separator='{sep}'")
                                st.sidebar.text(f"Shape: {df.shape}")
                            break
                    except Exception as e:
                        if DEBUG_MODE and enc == encodings_to_try[0]:
                            st.sidebar.text(f"Failed: {enc}/{sep} - {str(e)[:50]}")
                        continue
                
                if df is not None and df.shape[1] > 1:
                    break

    except Exception as e:
        return None, f"Gagal membaca file: {str(e)[:200]}"

    if df is None or df.empty:
        return None, "File kosong atau format tidak dikenali."

    if DEBUG_MODE:
        st.sidebar.text(f"Raw data shape before header detection: {df.shape}")

    # 2. CARI BARIS HEADER SEBENARNYA (TOKOPEDIA SPECIFIC)
    header_idx = -1
    # Keywords khusus Tokopedia (case-insensitive)
    keywords_tokopedia = [
        'order status', 'status pesanan',
        'seller sku', 'nomor sku',
        'order id', 'no. pesanan',
        'quantity', 'jumlah',
        'sku id', 'product name'
    ]
    
    # Tampilkan beberapa baris pertama untuk debug
    if DEBUG_MODE and df.shape[0] > 0:
        st.sidebar.text("First 3 rows (raw):")
        for i in range(min(3, df.shape[0])):
            row_preview = " | ".join([str(x)[:30] for x in df.iloc[i].fillna('').tolist()[:5]])
            st.sidebar.text(f"Row {i}: {row_preview}...")
    
    # Scan 50 baris pertama (Tokopedia bisa punya metadata)
    max_scan_rows = min(50, df.shape[0])
    for i in range(max_scan_rows):
        row = df.iloc[i]
        # Gabungkan semua nilai di baris menjadi string lowercase
        row_str = " ".join([str(val).lower().strip() if pd.notna(val) else '' for val in row.values])
        
        # Hitung berapa keyword yang match
        match_count = sum(1 for kw in keywords_tokopedia if kw in row_str)
        
        if match_count >= 2:  # Minimal 2 keyword yang match
            header_idx = i
            if DEBUG_MODE:
                st.sidebar.success(f"✅ Header ditemukan di baris {i+1}")
                st.sidebar.text(f"Header content: {row_str[:100]}...")
            break
    
    if header_idx == -1:
        if DEBUG_MODE:
            st.sidebar.warning("⚠️ Header tidak terdeteksi otomatis, menggunakan baris 0")
        header_idx = 0

    # 3. SET HEADER & BERSIHKAN
    try:
        df_final = df.iloc[header_idx:].copy()
        df_final.columns = df_final.iloc[0]  # Jadikan baris ini nama kolom
        
        # Hapus baris header dari data
        df_final = df_final.iloc[1:].reset_index(drop=True)
        
        # Bersihkan nama kolom
        df_final.columns = [
            str(col).replace('\n', ' ').replace('\r', ' ').strip() 
            if pd.notna(col) else f"Unnamed_{i}" 
            for i, col in enumerate(df_final.columns)
        ]
        
        # Hapus baris kosong di awal
        df_final = df_final.dropna(how='all').reset_index(drop=True)
        
        if DEBUG_MODE:
            st.sidebar.success(f"✅ Final shape: {df_final.shape}")
            st.sidebar.text(f"Columns: {list(df_final.columns)[:10]}")
            if len(df_final.columns) > 0:
                st.sidebar.text(f"First row data: {df_final.iloc[0].to_dict()}")
        
        return df_final, None
        
    except Exception as e:
        error_msg = f"Error saat set header (baris {header_idx}): {str(e)}"
        if DEBUG_MODE:
            st.sidebar.error(error_msg)
        return None, error_msg

# ==========================================
# MAIN PROCESSOR
# ==========================================
def process_universal_data(uploaded_files, kamus_data):
    start_time = time.time()
    
    if DEBUG_MODE:
        st.sidebar.subheader("🔧 Processing Kamus")
    
    # 1. LOAD & MAP KAMUS
    try:
        df_kurir = kamus_data['kurir']
        df_bundle = kamus_data['bundle']
        df_sku = kamus_data['sku']

        # A. Mapping Bundle
        bundle_map = {}
        for _, row in df_bundle.iterrows():
            cols = {c.lower(): c for c in df_bundle.columns}
            kit_c = cols.get('kit_sku') or cols.get('sku bundle') or cols.get('bundle')
            comp_c = cols.get('component_sku') or cols.get('sku component') or cols.get('component')
            qty_c = cols.get('component_qty') or cols.get('component quantity') or cols.get('qty')

            if kit_c and comp_c:
                kit_val = clean_sku(row[kit_c])
                comp_val = clean_sku(row[comp_c])
                try:
                    qty_val = float(str(row[qty_c]).replace(',', '.')) if qty_c else 1.0
                except:
                    qty_val = 1.0
                
                if kit_val:
                    if kit_val not in bundle_map: 
                        bundle_map[kit_val] = []
                    bundle_map[kit_val].append((comp_val, qty_val))

        if DEBUG_MODE:
            st.sidebar.info(f"Bundle mapping: {len(bundle_map)} entries")

        # B. Mapping SKU Name
        sku_name_map = {}
        if len(df_sku.columns) >= 2:
            idx_code = 1 if len(df_sku.columns) > 2 else 0 
            idx_name = 2 if len(df_sku.columns) > 2 else 1 
            
            for _, row in df_sku.iterrows():
                try:
                    code = clean_sku(row.iloc[idx_code])
                    name = str(row.iloc[idx_name])
                    if code and pd.notna(name): 
                        sku_name_map[code] = name
                except:
                    continue

        if DEBUG_MODE:
            st.sidebar.info(f"SKU name mapping: {len(sku_name_map)} entries")

        # C. List Kurir Instant Shopee
        instant_list = []
        if 'Instant/Same Day' in df_kurir.columns:
            k_col = df_kurir.columns[0]
            instant_list = df_kurir[
                df_kurir['Instant/Same Day'].astype(str).str.strip().str.lower().isin(['yes', 'ya', 'true', '1'])
            ][k_col].astype(str).str.strip().tolist()
            
        if DEBUG_MODE:
            st.sidebar.info(f"Instant couriers: {len(instant_list)} entries")
            
    except Exception as e:
        return None, f"Error memproses data Kamus: {e}"

    all_rows = []

    # 2. LOOP SETIAP FILE ORDER
    for mp_type, file_obj in uploaded_files:
        if DEBUG_MODE:
            st.sidebar.subheader(f"📦 Processing {mp_type}")
        
        df_raw, err = load_data_smart(file_obj)
        if err:
            st.error(f"❌ File {mp_type} Gagal: {err}")
            continue
            
        if df_raw.empty:
            st.warning(f"⚠️ File {mp_type} kosong setelah cleaning")
            continue
            
        df_filtered = pd.DataFrame()
        col_sku, col_qty, col_ord = '', '', ''
        
        # --- LOGIC SHOPEE ---
        if mp_type == 'Shopee':
            # Normalize column names
            df_raw.columns = [str(col).strip().lower() for col in df_raw.columns]
            
            status_c = next((c for c in df_raw.columns if 'status' in c), None)
            managed_c = next((c for c in df_raw.columns if 'dikelola' in c), None)
            resi_c = next((c for c in df_raw.columns if 'resi' in c), None)
            kurir_c = next((c for c in df_raw.columns if 'opsi' in c or 'kirim' in c), None)
            
            if DEBUG_MODE:
                st.sidebar.text(f"Shopee columns found: Status={status_c}, Managed={managed_c}, Resi={resi_c}, Kurir={kurir_c}")
                if status_c:
                    st.sidebar.text(f"Status unique: {df_raw[status_c].astype(str).str.strip().unique()[:10]}")
            
            if not all([status_c, managed_c, resi_c, kurir_c]):
                st.error(f"Shopee: Kolom tidak lengkap. Terbaca: {list(df_raw.columns)}")
                continue

            try:
                c1 = df_raw[status_c].astype(str).str.strip() == 'Perlu Dikirim'
                c2 = df_raw[managed_c].astype(str).str.strip().str.lower() == 'no'
                c3 = df_raw[resi_c].fillna('').astype(str).str.strip().isin(['', 'nan', 'None'])
                c4 = df_raw[kurir_c].astype(str).str.strip().isin(instant_list)
                
                df_filtered = df_raw[c1 & c2 & c3 & c4].copy()
                
                if DEBUG_MODE:
                    st.sidebar.text(f"Shopee filtered: {len(df_raw)} → {len(df_filtered)} rows")
            except Exception as e:
                st.error(f"Shopee filter error: {e}")
                continue
            
            col_sku = next((c for c in df_raw.columns if 'sku' in c and 'referensi' in c), None)
            if not col_sku:
                col_sku = next((c for c in df_raw.columns if 'sku' in c), 'Nomor Referensi SKU')
                
            col_qty = next((c for c in df_raw.columns if 'jumlah' in c), 'Jumlah')
            col_ord = next((c for c in df_raw.columns if 'pesanan' in c), 'No. Pesanan')

        # --- LOGIC TOKOPEDIA (UPGRADED) ---
        elif mp_type == 'Tokopedia':
            # Normalize column names (keep original for display, but have lowercase version)
            col_map = {str(col).strip(): col for col in df_raw.columns}
            col_lower_map = {str(col).strip().lower(): col for col in df_raw.columns}
            
            if DEBUG_MODE:
                st.sidebar.text(f"Tokopedia columns: {list(df_raw.columns)[:10]}")
            
            # Cari kolom status dengan beberapa kemungkinan
            status_col = None
            status_keywords = ['order status', 'status pesanan', 'status']
            
            for keyword in status_keywords:
                # Coba exact match (case-insensitive)
                for col in df_raw.columns:
                    if str(col).strip().lower() == keyword.lower():
                        status_col = col
                        break
                if status_col:
                    break
            
            # Fallback: cari partial match
            if not status_col:
                for col in df_raw.columns:
                    col_lower = str(col).lower()
                    if any(kw in col_lower for kw in ['status', 'order']):
                        status_col = col
                        break
            
            if not status_col:
                st.error(f"🚨 Tokopedia: Kolom Status TIDAK DITEMUKAN!")
                st.error(f"Daftar kolom: {list(df_raw.columns)}")
                continue
            
            if DEBUG_MODE:
                st.sidebar.success(f"✅ Status column found: {status_col}")
                unique_status = df_raw[status_col].astype(str).str.strip().str.lower().unique()[:10]
                st.sidebar.text(f"Status values: {unique_status}")
            
            # Filter: Status "Perlu dikirim" (case-insensitive)
            df_filtered = df_raw[
                df_raw[status_col].astype(str).str.strip().str.lower() == 'perlu dikirim'
            ].copy()
            
            if DEBUG_MODE:
                st.sidebar.info(f"Tokopedia filter: {len(df_raw)} → {len(df_filtered)} rows")
                if len(df_filtered) == 0:
                    st.sidebar.warning("⚠️ 0 rows after filtering!")
                    # Show sample of status values
                    sample_status = df_raw[status_col].astype(str).str.strip().unique()[:20]
                    st.sidebar.text(f"Sample status values: {sample_status}")
            
            # Tentukan kolom SKU
            sku_keywords = ['seller sku', 'nomor sku', 'sku']
            col_sku = None
            for keyword in sku_keywords:
                for col in df_raw.columns:
                    if str(col).strip().lower() == keyword.lower():
                        col_sku = col
                        break
                if col_sku:
                    break
            
            if not col_sku:
                col_sku = 'Seller SKU'
            
            # Tentukan kolom Quantity
            qty_keywords = ['quantity', 'jumlah']
            col_qty = None
            for keyword in qty_keywords:
                for col in df_raw.columns:
                    if str(col).strip().lower() == keyword.lower():
                        col_qty = col
                        break
                if col_qty:
                    break
            
            if not col_qty:
                col_qty = 'Quantity'
            
            # Tentukan kolom Order ID
            order_keywords = ['order id', 'orderid', 'invoice', 'pesanan']
            col_ord = None
            for keyword in order_keywords:
                for col in df_raw.columns:
                    if str(col).strip().lower() == keyword.lower():
                        col_ord = col
                        break
                if col_ord:
                    break
            
            if not col_ord:
                col_ord = 'Order ID'
            
            if DEBUG_MODE:
                st.sidebar.info(f"Mapping: SKU={col_sku}, Qty={col_qty}, Order={col_ord}")
                if col_sku in df_filtered.columns and not df_filtered.empty:
                    st.sidebar.text(f"SKU sample: {df_filtered[col_sku].iloc[0] if len(df_filtered) > 0 else 'N/A'}")

        # 3. EXPANSION (BUNDLE -> COMPONENT)
        if df_filtered.empty:
            if DEBUG_MODE:
                st.sidebar.warning(f"⚠️ {mp_type}: No data after filtering")
            continue
        
        rows_processed = 0
        for idx, row in df_filtered.iterrows():
            # Get SKU value safely
            raw_sku = ''
            if col_sku and col_sku in row:
                raw_sku = str(row[col_sku]) if pd.notna(row[col_sku]) else ''
            
            sku_clean = clean_sku(raw_sku)
            
            # Get quantity safely
            try:
                if col_qty and col_qty in row and pd.notna(row[col_qty]):
                    q_val = str(row[col_qty]).replace(',', '.')
                    qty_order = float(q_val)
                else:
                    qty_order = 0
            except:
                qty_order = 0
            
            # Get Order ID safely
            order_id = ''
            if col_ord and col_ord in row and pd.notna(row[col_ord]):
                order_id = str(row[col_ord])
            
            # Logic Bundle
            if sku_clean and sku_clean in bundle_map:
                for comp_sku, comp_qty_unit in bundle_map[sku_clean]:
                    if comp_sku:  # Skip empty component SKU
                        all_rows.append({
                            'Marketplace': mp_type,
                            'Order ID': order_id,
                            'SKU Original': raw_sku,
                            'Is Bundle?': 'Yes',
                            'SKU Component': comp_sku,
                            'Nama Produk': sku_name_map.get(comp_sku, comp_sku),
                            'Qty Total': qty_order * comp_qty_unit
                        })
                        rows_processed += 1
            elif sku_clean:  # Regular SKU
                all_rows.append({
                    'Marketplace': mp_type,
                    'Order ID': order_id,
                    'SKU Original': raw_sku,
                    'Is Bundle?': 'No',
                    'SKU Component': sku_clean,
                    'Nama Produk': sku_name_map.get(sku_clean, sku_clean),
                    'Qty Total': qty_order
                })
                rows_processed += 1
        
        if DEBUG_MODE:
            st.sidebar.success(f"✅ {mp_type}: {rows_processed} rows processed")

    if not all_rows:
        return None, "Data terbaca tapi 0 lolos filter. Cek kembali Status/Kurir/Resi di file order."

    # 4. FINAL AGGREGATION
    try:
        df_detail = pd.DataFrame(all_rows)
        
        # Ensure required columns exist
        for col in ['Marketplace', 'Order ID', 'SKU Original', 'Is Bundle?', 'SKU Component', 'Nama Produk', 'Qty Total']:
            if col not in df_detail.columns:
                df_detail[col] = ''
        
        # Reorder columns
        cols_order = ['Marketplace', 'Order ID', 'SKU Original', 'Is Bundle?', 'SKU Component', 'Nama Produk', 'Qty Total']
        existing_cols = [c for c in cols_order if c in df_detail.columns]
        other_cols = [c for c in df_detail.columns if c not in cols_order]
        df_detail = df_detail[existing_cols + other_cols]
        
        # Group by for summary
        df_summary = df_detail.groupby(['Marketplace', 'SKU Component', 'Nama Produk'], as_index=False).agg({
            'Qty Total': 'sum'
        }).sort_values('Qty Total', ascending=False)
        
        if DEBUG_MODE:
            st.sidebar.success(f"✅ Final: {len(df_detail)} detail rows, {len(df_summary)} summary rows")
            st.sidebar.text(f"Processing time: {time.time() - start_time:.2f}s")
        
        return {'detail': df_detail, 'summary': df_summary}, None
        
    except Exception as e:
        return None, f"Error saat aggregasi final: {e}"

# --- TEST FUNCTION FOR TOKOPEDIA ---
def test_tokopedia_file(file_obj):
    """Test function untuk debug file Tokopedia"""
    if not file_obj:
        st.warning("Upload file Tokopedia dulu")
        return
    
    st.subheader("🧪 Test Baca File Tokopedia")
    
    df_test, err = load_data_smart(file_obj)
    if err:
        st.error(f"❌ Error: {err}")
        return
    
    st.success(f"✅ File terbaca! Shape: {df_test.shape}")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Kolom yang terbaca:**")
        st.write(list(df_test.columns))
        
        st.write("**5 baris pertama:**")
        st.dataframe(df_test.head())
    
    with col2:
        st.write("**Statistik:**")
        st.write(f"Total baris: {len(df_test)}")
        st.write(f"Total kolom: {len(df_test.columns)}")
        
        # Cari kolom status
        status_cols = [c for c in df_test.columns if 'status' in str(c).lower()]
        st.write(f"**Kolom Status ditemukan:** {status_cols}")
        
        if status_cols:
            for col in status_cols[:3]:  # Max 3 kolom status
                st.write(f"\n**{col}**:")
                unique_vals = df_test[col].astype(str).str.strip().unique()[:20]
                st.write(f"Nilai unik: {list(unique_vals)}")
                
                # Count 'Perlu dikirim'
                perlu_dikirim = df_test[df_test[col].astype(str).str.strip().str.lower() == 'perlu dikirim']
                st.write(f"Baris 'Perlu dikirim': {len(perlu_dikirim)}")

# --- UI STREAMLIT ---
st.sidebar.header("📁 1. Upload Kamus (Wajib)")
kamus_f = st.sidebar.file_uploader("Kamus Dashboard.xlsx", type=['xlsx'], key="kamus")

st.sidebar.header("📁 2. Upload Order")
shp_f = st.sidebar.file_uploader("Order Shopee", type=['xlsx', 'csv'], key="shopee")
tok_f = st.sidebar.file_uploader("Order Tokopedia", type=['xlsx', 'csv'], key="tokopedia")

# Test button untuk debug
if DEBUG_MODE and tok_f:
    if st.sidebar.button("🧪 Test Tokopedia File"):
        test_tokopedia_file(tok_f)

# --- LOGIC RESET DASHBOARD ---
if not shp_f and not tok_f:
    if 'result' in st.session_state:
        del st.session_state['result']

# Main process button
if st.sidebar.button("🚀 PROSES DATA", type="primary"):
    if not kamus_f:
        st.error("❌ Upload Kamus dulu!")
    elif not shp_f and not tok_f:
        st.error("❌ Upload minimal satu file order!")
    else:
        with st.spinner("Processing... Mohon tunggu..."):
            try:
                # Load Kamus
                k_excel = pd.ExcelFile(kamus_f, engine='openpyxl')
                
                # Cek sheet yang tersedia
                sheet_names = k_excel.sheet_names
                if DEBUG_MODE:
                    st.sidebar.info(f"Kamus sheets: {sheet_names}")
                
                k_data = {}
                required_sheets = ['Kurir-Shopee', 'Bundle Master', 'SKU Master']
                
                for sheet in required_sheets:
                    if sheet in sheet_names:
                        k_data[sheet.replace('-', '_').replace(' ', '_').lower().split('_')[0]] = pd.read_excel(k_excel, sheet_name=sheet)
                    else:
                        # Cari sheet dengan nama yang mirip
                        matching = [s for s in sheet_names if any(word.lower() in s.lower() for word in sheet.split())]
                        if matching:
                            k_data[sheet.replace('-', '_').replace(' ', '_').lower().split('_')[0]] = pd.read_excel(k_excel, sheet_name=matching[0])
                        else:
                            st.error(f"Sheet '{sheet}' tidak ditemukan di Kamus!")
                            st.stop()
                
                files = []
                if shp_f: 
                    files.append(('Shopee', shp_f))
                if tok_f: 
                    files.append(('Tokopedia', tok_f))
                
                res, err_msg = process_universal_data(files, k_data)
                
                if err_msg:
                    st.warning(f"⚠️ {err_msg}")
                else:
                    total_qty = res['summary']['Qty Total'].sum()
                    st.success(f"✅ Sukses! Total Item: {total_qty}")
                    st.session_state.result = res
                    
            except Exception as e:
                st.error(f"❌ System Error: {str(e)[:500]}")
                if DEBUG_MODE:
                    import traceback
                    st.code(traceback.format_exc())

# --- OUTPUT AREA ---
if 'result' in st.session_state:
    res = st.session_state.result
    
    t1, t2 = st.tabs(["📋 Picking List (Detail)", "📦 Stock Check (Summary)"])
    
    with t1: 
        st.dataframe(res['detail'], use_container_width=True, height=400)
        st.write(f"**Total Rows:** {len(res['detail'])}")
    
    with t2: 
        st.dataframe(res['summary'], use_container_width=True, height=400)
        st.write(f"**Total SKU:** {len(res['summary'])}")
        st.write(f"**Total Quantity:** {res['summary']['Qty Total'].sum()}")
    
    # Download Button
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
        res['detail'].to_excel(writer, sheet_name='Picking List', index=False)
        res['summary'].to_excel(writer, sheet_name='Stock Check', index=False)
        
        # Auto-adjust column widths
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            for i, col in enumerate(res['detail'].columns if sheet_name == 'Picking List' else res['summary'].columns):
                column_len = max(
                    res['detail'][col].astype(str).str.len().max() if sheet_name == 'Picking List' else res['summary'][col].astype(str).str.len().max(),
                    len(str(col))
                )
                worksheet.set_column(i, i, min(column_len + 2, 50))
    
    st.download_button(
        "📥 Download Excel Final",
        data=buf.getvalue(),
        file_name=f"Picking_List_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary"
    )

# --- FOOTER ---
st.sidebar.markdown("---")
st.sidebar.caption("v2.1 - Enhanced Tokopedia Support")
