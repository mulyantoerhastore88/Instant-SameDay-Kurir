import streamlit as st
import pandas as pd
import numpy as np
import io
import time
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# --- CONFIG ---
st.set_page_config(page_title="Universal Order Processor", layout="wide")
st.title("🛒 Universal Marketplace Order Processor (Multi-Channel)")
st.markdown("""
**Logic Applied:**
1. **Shopee Official**: Status='Perlu Dikirim' | Managed='No' | Resi=Blank | Kurir=Instant (Lookup Kamus).
2. **Shopee Inhouse**: Status='Perlu Dikirim' | **Resi=Blank** | **Lookup 'Opsi Pengiriman' ke Kamus (Yes)**.
3. **Tokopedia**: Status='Perlu Dikirim'.
""")

# --- DEBUG MODE ---
st.sidebar.header("🔧 Debug Mode")
DEBUG_MODE = st.sidebar.checkbox("Tampilkan info detil", value=False)

# --- FUNGSI CLEANING SKU ---
def clean_sku(sku):
    if pd.isna(sku): return ""
    sku = str(sku).strip()
    sku = ''.join(char for char in sku if ord(char) >= 32)
    sku_upper = sku.upper()
    
    if sku_upper.startswith('FG-') or sku_upper.startswith('CS-'):
        return sku
        
    if '-' in sku:
        return sku.split('-', 1)[-1].strip()
        
    return sku

# --- FUNGSI SMART LOADER ---
def load_data_smart(file_obj):
    df = None
    filename = file_obj.name.lower()
    
    try:
        if filename.endswith('.xlsx') or filename.endswith('.xls'):
            try:
                df = pd.read_excel(file_obj, dtype=str, header=None, engine='openpyxl')
            except: df = None

        if df is None or df.shape[1] <= 1:
            file_obj.seek(0)
            encodings = ['utf-8-sig', 'utf-8', 'latin-1']
            separators = [',', ';', '\t']
            for enc in encodings:
                if df is not None and df.shape[1] > 1: break
                for sep in separators:
                    try:
                        file_obj.seek(0)
                        temp_df = pd.read_csv(
                            file_obj, sep=sep, dtype=str, header=None, 
                            encoding=enc, on_bad_lines='skip', 
                            quotechar='"', skipinitialspace=True
                        )
                        if temp_df.shape[1] > 1:
                            df = temp_df
                            break
                    except: continue

    except Exception as e:
        return None, f"Gagal membaca file: {str(e)[:100]}"

    if df is None or df.empty:
        return None, "File kosong/format salah."

    # Cari Header
    header_idx = -1
    keywords = ['status pesanan', 'no. pesanan', 'sku', 'product name', 'nama produk', 'no. resi', 'opsi pengiriman']
    
    for i in range(min(30, df.shape[0])):
        row_str = " ".join([str(v).lower() for v in df.iloc[i].dropna().values])
        if sum(1 for k in keywords if k in row_str) >= 2:
            header_idx = i
            break
    
    if header_idx == -1: header_idx = 0

    try:
        df_final = df.iloc[header_idx:].copy()
        df_final.columns = df_final.iloc[0]
        df_final = df_final.iloc[1:].reset_index(drop=True)
        # Bersihkan nama kolom
        df_final.columns = [str(c).replace('\n', ' ').strip().lower() for c in df_final.columns]
        df_final = df_final.dropna(how='all')
        return df_final, None
    except Exception as e:
        return None, f"Error header: {e}"

# ==========================================
# MAIN PROCESSOR
# ==========================================
def process_universal_data(uploaded_files, kamus_data):
    all_rows = []
    
    # 1. SIAPKAN LOOKUP DATA (KAMUS)
    try:
        df_kurir = kamus_data['kurir']
        df_bundle = kamus_data['bundle']
        df_sku = kamus_data['sku']

        # --- A. LOGIC KURIR (LOOKUP TABLE) ---
        # Kita buat Dictionary biar pencocokan cepat
        # Format: { "nama kurir di opsi pengiriman (lowercase)" : True/False }
        
        valid_courier_map = {}
        
        # Deteksi kolom di Kamus Kurir
        col_name_kamus = df_kurir.columns[0] # Kolom A: Opsi Pengiriman
        col_val_kamus = df_kurir.columns[1]  # Kolom B: Instant (Yes/No)
        
        if DEBUG_MODE:
            st.sidebar.markdown("---")
            st.sidebar.markdown(f"**Mapping Kamus Kurir:**")
            st.sidebar.text(f"Kolom Nama: {col_name_kamus}")
            st.sidebar.text(f"Kolom Status: {col_val_kamus}")

        for _, row in df_kurir.iterrows():
            k_name = str(row[col_name_kamus]).strip().lower()
            k_stat = str(row[col_val_kamus]).strip().lower()
            
            # Jika Yes/Ya/True/1 maka True
            is_instant = k_stat in ['yes', 'ya', 'true', '1']
            valid_courier_map[k_name] = is_instant

        # --- B. LOGIC BUNDLE ---
        bundle_map = {}
        b_cols = {str(c).lower(): c for c in df_bundle.columns}
        kit_c = next((b_cols[k] for k in ['kit_sku', 'sku bundle'] if k in b_cols), None)
        comp_c = next((b_cols[k] for k in ['component_sku', 'sku component'] if k in b_cols), None)
        qty_c = next((b_cols[k] for k in ['qty', 'quantity', 'component_qty'] if k in b_cols), None)

        if kit_c and comp_c:
            for _, row in df_bundle.iterrows():
                k = clean_sku(row[kit_c])
                c = clean_sku(row[comp_c])
                try: q = float(str(row[qty_c]).replace(',', '.')) if qty_c else 1.0
                except: q = 1.0
                if k and c:
                    if k not in bundle_map: bundle_map[k] = []
                    bundle_map[k].append((c, q))

        # --- C. SKU NAME ---
        sku_name_map = {}
        for _, row in df_sku.iterrows():
            vals = [v for v in row if pd.notna(v) and str(v).strip()]
            if len(vals) >= 2:
                sku_name_map[clean_sku(vals[0])] = str(vals[1]).strip()

    except Exception as e:
        return None, f"Error Kamus: {e}"

    # 2. LOOP SETIAP FILE UPLOAD
    for mp_type, file_obj in uploaded_files:
        df_raw, err = load_data_smart(file_obj)
        if err:
            st.error(f"❌ {mp_type}: {err}")
            continue
            
        df_filtered = pd.DataFrame()
        
        # ----------------------------------
        # A. SHOPEE OFFICIAL (Logic Lama)
        # ----------------------------------
        if mp_type == 'Shopee Official':
            status_c = next((c for c in df_raw.columns if 'status' in c), None)
            managed_c = next((c for c in df_raw.columns if 'dikelola' in c), None)
            resi_c = next((c for c in df_raw.columns if 'resi' in c), None)
            kurir_c = next((c for c in df_raw.columns if any(x in c for x in ['opsi', 'kirim', 'kurir'])), None)
            
            if not all([status_c, managed_c, resi_c, kurir_c]):
                st.error(f"{mp_type}: Kolom Wajib tidak lengkap.")
                continue

            try:
                # Disini dia pakai logic list manual (opsional, tapi saya samakan dgn lookup biar konsisten)
                # Tapi karena Shopee Official biasa strict kolom 'Dikelola', kita pertahankan logic asli user dulu
                # kecuali bagian kurir saya arahkan ke map juga biar akurat.
                
                c1 = df_raw[status_c].astype(str).str.strip() == 'Perlu Dikirim'
                c2 = df_raw[managed_c].astype(str).str.strip().str.lower() == 'no'
                c3 = df_raw[resi_c].fillna('').astype(str).str.strip().isin(['', 'nan', 'none'])
                
                # Logic Kurir: Cek apakah nama kurir ada di valid_courier_map dan nilainya True
                c4 = df_raw[kurir_c].astype(str).str.strip().str.lower().map(valid_courier_map).fillna(False)
                
                df_filtered = df_raw[c1 & c2 & c3 & c4].copy()
            except: continue

            col_sku = next((c for c in df_raw.columns if 'referensi' in c), 'nomor referensi sku')
            col_qty = next((c for c in df_raw.columns if 'jumlah' in c), 'jumlah')
            col_ord = next((c for c in df_raw.columns if 'pesanan' in c), 'no. pesanan')
            col_prod = next((c for c in df_raw.columns if 'nama produk' in c), 'nama produk')

        # ----------------------------------
        # B. SHOPEE INHOUSE (Logic Request Baru)
        # ----------------------------------
        elif mp_type == 'Shopee Inhouse':
            # Cari kolom spesifik
            status_c = next((c for c in df_raw.columns if 'status' in c), None)
            resi_c = next((c for c in df_raw.columns if 'resi' in c), None)
            opsi_kirim_c = next((c for c in df_raw.columns if 'opsi pengiriman' in c), None) 
            
            if not all([status_c, resi_c, opsi_kirim_c]):
                st.error(f"{mp_type}: Kolom (Status / No. Resi / Opsi Pengiriman) tidak lengkap.")
                if DEBUG_MODE:
                    st.sidebar.warning(f"Columns Found: {list(df_raw.columns)}")
                continue
                
            try:
                # 1. Status = Perlu Dikirim
                c1 = df_raw[status_c].astype(str).str.strip() == 'Perlu Dikirim'
                
                # 2. Resi = BLANK
                c2 = df_raw[resi_c].fillna('').astype(str).str.strip().isin(['', 'nan', 'none', '-'])
                
                # 3. LOOKUP 'Opsi Pengiriman' ke Kamus
                # Ambil text dari file, lowercase, cocokkan dengan dict valid_courier_map
                raw_courier_series = df_raw[opsi_kirim_c].astype(str).str.strip().str.lower()
                c3 = raw_courier_series.map(valid_courier_map).fillna(False)
                
                if DEBUG_MODE:
                    st.sidebar.markdown(f"**Debug {mp_type}:**")
                    st.sidebar.text(f"Total Rows: {len(df_raw)}")
                    st.sidebar.text(f"Lolos Status: {c1.sum()}")
                    st.sidebar.text(f"Lolos Resi Blank: {c2.sum()}")
                    st.sidebar.text(f"Lolos Kurir Lookup: {c3.sum()}")
                    
                    # Tampilkan sample kurir yg GAGAL lolos lookup (untuk cek typo di kamus)
                    failed_couriers = df_raw[~c3][opsi_kirim_c].unique()
                    st.sidebar.text(f"Sample Rejected Couriers: {failed_couriers[:5]}")
                
                df_filtered = df_raw[c1 & c2 & c3].copy()
                
            except Exception as e:
                st.error(f"{mp_type} filter error: {e}")
                continue

            col_sku = next((c for c in df_raw.columns if 'referensi' in c), 'nomor referensi sku')
            col_qty = next((c for c in df_raw.columns if 'jumlah' in c), 'jumlah')
            col_ord = next((c for c in df_raw.columns if 'pesanan' in c), 'no. pesanan')
            col_prod = next((c for c in df_raw.columns if 'nama produk' in c), 'nama produk')

        # ----------------------------------
        # C. TOKOPEDIA
        # ----------------------------------
        elif mp_type == 'Tokopedia':
            status_c = next((c for c in df_raw.columns if 'status' in c), None)
            if not status_c: continue
            
            df_filtered = df_raw[
                df_raw[status_c].astype(str).str.strip().str.lower() == 'perlu dikirim'
            ].copy()
            
            col_sku = next((c for c in df_raw.columns if 'sku' in c), 'seller sku')
            col_qty = next((c for c in df_raw.columns if any(x in c for x in ['quantity', 'jumlah'])), 'quantity')
            col_ord = next((c for c in df_raw.columns if any(x in c for x in ['order', 'invoice', 'pesanan'])), 'order id')
            col_prod = next((c for c in df_raw.columns if 'product' in c), 'product name')

        # ----------------------------------
        # PROCESS ROWS (Mapping SKU & Qty)
        # ----------------------------------
        if df_filtered.empty:
            if DEBUG_MODE: st.sidebar.warning(f"⚠️ {mp_type}: 0 data lolos filter.")
            continue
            
        for _, row in df_filtered.iterrows():
            raw_sku = str(row.get(col_sku, ''))
            sku_clean = clean_sku(raw_sku)
            try: q_val = float(str(row.get(col_qty, 0)).replace(',', '.'))
            except: q_val = 0
            
            if not sku_clean or q_val <= 0: continue
            
            order_id = str(row.get(col_ord, ''))
            p_name = str(row.get(col_prod, ''))

            # Bundle Check
            if sku_clean in bundle_map:
                for c_sku, c_qty in bundle_map[sku_clean]:
                    all_rows.append({
                        'Marketplace': mp_type,
                        'Order ID': order_id,
                        'SKU Original': raw_sku,
                        'Is Bundle?': 'Yes',
                        'SKU Component': c_sku,
                        'Nama Produk': sku_name_map.get(c_sku, c_sku),
                        'Qty Total': q_val * c_qty
                    })
            else:
                all_rows.append({
                    'Marketplace': mp_type,
                    'Order ID': order_id,
                    'SKU Original': raw_sku,
                    'Is Bundle?': 'No',
                    'SKU Component': sku_clean,
                    'Nama Produk': sku_name_map.get(sku_clean, p_name),
                    'Qty Total': q_val
                })

    # 4. FINAL AGGREGATION
    if not all_rows: return None, "Tidak ada data yang lolos filter."

    df_detail = pd.DataFrame(all_rows)
    df_summary = df_detail.groupby(['Marketplace', 'SKU Component', 'Nama Produk'], as_index=False).agg({
        'Qty Total': 'sum'
    }).sort_values('Qty Total', ascending=False)
    
    return {'detail': df_detail, 'summary': df_summary}, None

# --- UI STREAMLIT ---
st.sidebar.header("📁 1. Upload Kamus (Wajib)")
kamus_f = st.sidebar.file_uploader("Kamus Dashboard.xlsx", type=['xlsx'], key="kamus")

st.sidebar.header("📁 2. Upload Order Files")
shp_off_f = st.sidebar.file_uploader("Shopee OFFICIAL (Ada kolom 'Dikelola')", key="shp_off")
shp_inh_f = st.sidebar.file_uploader("Shopee INHOUSE (ErHair - Lookup Opsi Pengiriman)", key="shp_inh")
tok_f = st.sidebar.file_uploader("Tokopedia", key="tok")

# Reset
if not (shp_off_f or shp_inh_f or tok_f):
    if 'result' in st.session_state: del st.session_state['result']

if st.sidebar.button("🚀 PROSES DATA", type="primary"):
    if not kamus_f:
        st.error("❌ Upload Kamus dulu!")
    elif not (shp_off_f or shp_inh_f or tok_f):
        st.error("❌ Upload minimal satu file order!")
    else:
        with st.spinner("Processing..."):
            try:
                k_excel = pd.ExcelFile(kamus_f, engine='openpyxl')
                k_data = {}
                
                # --- LOAD SHEET PRIORITAS ---
                # Mencari sheet yang mengandung kata "Kurir-Shopee" atau "Kurir"
                sheet_kurir = None
                for s in k_excel.sheet_names:
                    if 'kurir-shopee' in s.lower():
                        sheet_kurir = s
                        break
                if not sheet_kurir: 
                    for s in k_excel.sheet_names:
                        if 'kurir' in s.lower():
                            sheet_kurir = s
                            break
                            
                if sheet_kurir:
                    k_data['kurir'] = pd.read_excel(k_excel, sheet_name=sheet_kurir)
                else:
                    raise Exception("Sheet 'Kurir-Shopee' tidak ditemukan di Kamus")

                # Load sheet Bundle & SKU
                for req in ['Bundle', 'SKU']:
                    found = False
                    for s in k_excel.sheet_names:
                        if req.lower() in s.lower():
                            k_data[req.lower()] = pd.read_excel(k_excel, sheet_name=s)
                            found = True
                            break
                    if not found: raise Exception(f"Sheet {req} hilang di Kamus")

                files = []
                if shp_off_f: files.append(('Shopee Official', shp_off_f))
                if shp_inh_f: files.append(('Shopee Inhouse', shp_inh_f))
                if tok_f: files.append(('Tokopedia', tok_f))
                
                res, err = process_universal_data(files, k_data)
                
                if err: st.warning(f"⚠️ {err}")
                else:
                    st.session_state.result = res
                    st.success("✅ Sukses!")
                    
            except Exception as e:
                st.error(f"❌ Error: {e}")

# --- OUTPUT ---
if 'result' in st.session_state:
    res = st.session_state.result
    t1, t2 = st.tabs(["📋 Picking List", "📦 Stock Summary"])
    
    with t1: st.dataframe(res['detail'], use_container_width=True)
    with t2: st.dataframe(res['summary'], use_container_width=True)
    
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
        res['detail'].to_excel(writer, sheet_name='Picking List', index=False)
        res['summary'].to_excel(writer, sheet_name='Stock Check', index=False)
        
    st.download_button("📥 Download Excel", buf.getvalue(), f"Picking_List_{datetime.now().strftime('%H%M')}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
