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
1. **Shopee Official**: Status='Perlu Dikirim' | **Managed='No'** | Resi=Blank | Kurir=Instant (Lookup Kamus).
2. **Shopee Inhouse**: Status='Perlu Dikirim' | **(Skip Managed)** | Resi=Blank | Kurir=Instant (Lookup Kamus 'Kurir-Shopee' -> Yes).
3. **Tokopedia**: Status='Perlu Dikirim'.
""")

# --- DEBUG MODE ---
st.sidebar.header("🔧 Debug Mode")
DEBUG_MODE = st.sidebar.checkbox("Tampilkan info detil", value=False)

# --- FUNGSI CLEANING SKU ---
def clean_sku(sku):
    """Membersihkan SKU, mempertahankan prefix FG-/CS-"""
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
    """Loader pintar untuk membaca Excel/CSV dengan berbagai format"""
    df = None
    filename = file_obj.name.lower()
    
    try:
        # A. Coba Excel
        if filename.endswith('.xlsx') or filename.endswith('.xls'):
            try:
                df = pd.read_excel(file_obj, dtype=str, header=None, engine='openpyxl')
            except:
                df = None

        # B. Coba CSV
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
    keywords = ['status pesanan', 'no. pesanan', 'sku', 'product name', 'nama produk', 'no. resi']
    
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
    
    # 1. LOAD KAMUS
    try:
        df_kurir = kamus_data['kurir']
        df_bundle = kamus_data['bundle']
        df_sku = kamus_data['sku']

        # Mapping Bundle
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

        # Mapping SKU Name
        sku_name_map = {}
        for _, row in df_sku.iterrows():
            vals = [v for v in row if pd.notna(v) and str(v).strip()]
            if len(vals) >= 2:
                sku_name_map[clean_sku(vals[0])] = str(vals[1]).strip()

        # --- LOGIC BARU: GENERATE INSTANT LIST DARI SHEET KURIR-SHOPEE ---
        instant_list = []
        if not df_kurir.empty:
            # Cari kolom yang mengandung kata 'instant' atau 'same day'
            # Asumsi: Kolom A = Nama Kurir, Kolom B = Status Yes/No (Sesuai request user)
            
            # Coba deteksi kolom secara otomatis dulu
            col_kurir_name = None
            col_is_instant = None
            
            for col in df_kurir.columns:
                c_low = str(col).lower()
                if 'opsi' in c_low or 'kurir' in c_low or 'shipping' in c_low:
                    col_kurir_name = col
                if 'instant' in c_low or 'same' in c_low:
                    col_is_instant = col
            
            # Fallback jika nama kolom tidak terdeteksi, pakai Index (A=0, B=1)
            if not col_kurir_name: col_kurir_name = df_kurir.columns[0]
            if not col_is_instant and len(df_kurir.columns) > 1: col_is_instant = df_kurir.columns[1]
            
            if col_kurir_name and col_is_instant:
                # Ambil baris dimana kolom B isinya Yes/Ya/True
                instant_list = df_kurir[
                    df_kurir[col_is_instant].astype(str).str.strip().str.lower().isin(['yes', 'ya', 'true', '1'])
                ][col_kurir_name].astype(str).str.strip().tolist()
                
            if DEBUG_MODE:
                st.sidebar.info(f"📋 Whitelist Kurir ({len(instant_list)}): {instant_list}")

    except Exception as e:
        return None, f"Error Kamus: {e}"

    # 2. LOOP FILES
    for mp_type, file_obj in uploaded_files:
        df_raw, err = load_data_smart(file_obj)
        if err:
            st.error(f"❌ {mp_type}: {err}")
            continue
            
        df_filtered = pd.DataFrame()
        
        # ----------------------------------
        # A. SHOPEE OFFICIAL
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
                c1 = df_raw[status_c].astype(str).str.strip() == 'Perlu Dikirim'
                c2 = df_raw[managed_c].astype(str).str.strip().str.lower() == 'no'
                c3 = df_raw[resi_c].fillna('').astype(str).str.strip().isin(['', 'nan', 'none'])
                c4 = df_raw[kurir_c].astype(str).str.strip().isin(instant_list)
                df_filtered = df_raw[c1 & c2 & c3 & c4].copy()
            except: continue

            col_sku = next((c for c in df_raw.columns if 'referensi' in c), 'nomor referensi sku')
            col_qty = next((c for c in df_raw.columns if 'jumlah' in c), 'jumlah')
            col_ord = next((c for c in df_raw.columns if 'pesanan' in c), 'no. pesanan')
            col_prod = next((c for c in df_raw.columns if 'nama produk' in c), 'nama produk')

        # ----------------------------------
        # B. SHOPEE INHOUSE (UPDATED LOGIC)
        # ----------------------------------
        elif mp_type == 'Shopee Inhouse':
            status_c = next((c for c in df_raw.columns if 'status' in c), None)
            resi_c = next((c for c in df_raw.columns if 'resi' in c), None)
            kurir_c = next((c for c in df_raw.columns if any(x in c for x in ['opsi', 'kirim', 'kurir'])), None)
            
            if not all([status_c, resi_c, kurir_c]):
                st.error(f"{mp_type}: Kolom (Status/Resi/Opsi Pengiriman) tidak lengkap.")
                continue
                
            try:
                # 1. Status = Perlu Dikirim
                c1 = df_raw[status_c].astype(str).str.strip() == 'Perlu Dikirim'
                
                # 2. Resi = BLANK
                c2 = df_raw[resi_c].fillna('').astype(str).str.strip().isin(['', 'nan', 'none', '-'])
                
                # 3. Kurir = LOOKUP KAMUS (YES ONLY)
                # Normalisasi data order agar match dengan kamus
                kurir_order = df_raw[kurir_c].astype(str).str.strip()
                c3 = kurir_order.isin(instant_list)
                
                if DEBUG_MODE:
                    st.sidebar.markdown(f"**Debug {mp_type}:**")
                    st.sidebar.text(f"Total Rows: {len(df_raw)}")
                    st.sidebar.text(f"Lolos Status 'Perlu Dikirim': {c1.sum()}")
                    st.sidebar.text(f"Lolos Resi Blank: {c2.sum()}")
                    st.sidebar.text(f"Lolos Kurir (Kamus Yes): {c3.sum()}")
                
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
        # PROCESS ROWS
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
    if not all_rows: return None, "Tidak ada data yang lolos filter dari semua file."

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
shp_inh_f = st.sidebar.file_uploader("Shopee INHOUSE (ErHair)", key="shp_inh")
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
                
                # LOAD SHEET KURIR PRIORITAS 'Kurir-Shopee'
                sheet_kurir = None
                for s in k_excel.sheet_names:
                    if 'kurir-shopee' in s.lower():
                        sheet_kurir = s
                        break
                if not sheet_kurir: # Fallback cari yg ada kata 'Kurir'
                    for s in k_excel.sheet_names:
                        if 'kurir' in s.lower():
                            sheet_kurir = s
                            break
                            
                if sheet_kurir:
                    k_data['kurir'] = pd.read_excel(k_excel, sheet_name=sheet_kurir)
                else:
                    raise Exception("Sheet Kurir/Kurir-Shopee tidak ditemukan")

                # Load other sheets
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
