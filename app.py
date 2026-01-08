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
st.title("🛒 Universal Marketplace Order Processor-Created By Mulyanto")
st.markdown("""
**Logic Applied:**
1. **Shopee (Official)**: Status='Perlu Dikirim' | Resi=Blank | Managed='No' | **Kurir=Instant (Kamus)**.
2. **Shopee (INHOUSE)**: Status='Perlu Dikirim' | Resi=Blank | **Kurir=Instant (Kamus)** | *(Tanpa Cek Managed)*.
3. **Tokopedia**: Status='Perlu Dikirim'.
4. **SKU Logic**: Prefix **FG-** & **CS-** dipertahankan, sisanya ambil suffix.
5. **SKU Source**: Kolom spesifik **"Nomor Referensi SKU"**.
""")

# --- DEBUG MODE ---
st.sidebar.header("🔧 Debug Mode")
DEBUG_MODE = st.sidebar.checkbox("Tampilkan info detil (Debug)", value=False)

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
        # 1. Coba baca Excel
        if filename.endswith('.xlsx') or filename.endswith('.xls'):
            try: df = pd.read_excel(file_obj, dtype=str, header=None, engine='openpyxl')
            except: df = None

        # 2. Coba baca CSV
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
                            encoding=enc, on_bad_lines='skip', quotechar='"'
                        )
                        if temp_df.shape[1] > 1:
                            df = temp_df
                            break
                    except: continue

    except Exception as e: return None, f"Gagal membaca file: {str(e)[:100]}"

    if df is None or df.empty: return None, "File kosong atau format tidak dikenali."

    # 3. Auto-Detect Header
    header_idx = 0
    keywords = ['status', 'sku', 'order', 'pesanan', 'quantity', 'jumlah', 'product', 'opsi pengiriman', 'shipping']
    
    for i in range(min(20, df.shape[0])):
        row_str = " ".join([str(v).lower() for v in df.iloc[i].dropna().values])
        if sum(1 for kw in keywords if kw in row_str) >= 2:
            header_idx = i
            break
    
    try:
        df_final = df.iloc[header_idx:].copy()
        df_final.columns = df_final.iloc[0]
        df_final = df_final.iloc[1:].reset_index(drop=True)
        df_final.columns = [str(c).strip().replace('\n', ' ') for c in df_final.columns]
        df_final = df_final.dropna(how='all')
        return df_final, None
    except Exception as e: return None, f"Gagal set header: {e}"

# ==========================================
# MAIN PROCESSOR
# ==========================================
def process_universal_data(uploaded_files, kamus_data):
    all_rows = []
    raw_stats_list = [] # Untuk Tab Validasi
    
    # 1. PREPARE KAMUS
    try:
        df_kurir = kamus_data['kurir']
        df_bundle = kamus_data['bundle']
        df_sku = kamus_data['sku']
        
        # Bundle Map
        bundle_map = {}
        k_cols = {str(c).lower(): c for c in df_bundle.columns}
        kit_c = next((v for k,v in k_cols.items() if any(x in k for x in ['kit','bundle','parent'])), None)
        comp_c = next((v for k,v in k_cols.items() if any(x in k for x in ['component','child'])), None)
        qty_c = next((v for k,v in k_cols.items() if any(x in k for x in ['qty','quantity'])), None)
        
        if kit_c and comp_c:
            for _, row in df_bundle.iterrows():
                k_val = clean_sku(row[kit_c])
                c_val = clean_sku(row[comp_c])
                try: q_val = float(str(row[qty_c]).replace(',', '.')) if qty_c else 1.0
                except: q_val = 1.0
                if k_val and c_val:
                    if k_val not in bundle_map: bundle_map[k_val] = []
                    bundle_map[k_val].append((c_val, q_val))

        # SKU Name Map
        sku_name_map = {}
        for _, row in df_sku.iterrows():
            vals = [str(v).strip() for v in row if pd.notna(v) and str(v).strip()]
            if len(vals) >= 2: sku_name_map[clean_sku(vals[0])] = vals[1]

        # Instant List
        instant_list = []
        if not df_kurir.empty:
            ins_col = next((c for c in df_kurir.columns if 'instant' in str(c).lower()), None)
            kur_col = df_kurir.columns[0]
            if ins_col:
                instant_list = df_kurir[
                    df_kurir[ins_col].astype(str).str.lower().isin(['yes','ya','true','1'])
                ][kur_col].astype(str).str.strip().tolist()

    except Exception as e: return None, f"Error Kamus: {e}"

    # 2. PROCESS FILES
    for mp_type, file_obj in uploaded_files:
        df_raw, err = load_data_smart(file_obj)
        if err:
            st.warning(f"⚠️ Skip {mp_type}: {err}")
            continue
            
        df_filtered = pd.DataFrame()
        df_raw.columns = [str(c).strip().lower() for c in df_raw.columns]

        if DEBUG_MODE:
            st.sidebar.markdown(f"**Processing {mp_type}...**")
            st.sidebar.text(f"Kolom tersedia: {list(df_raw.columns)}")

        # --- A. CAPTURE RAW STATS (VALIDATION TAB) ---
        raw_kurir_col = None
        if 'shopee' in mp_type.lower():
            raw_kurir_col = next((c for c in df_raw.columns if any(x in c for x in ['opsi','kirim'])), None)
        elif 'tokopedia' in mp_type.lower():
            # Prioritas: Shipping Provider -> Delivery Option -> Kurir
            raw_kurir_col = next((c for c in df_raw.columns if 'shipping provider' in c), None)
            if not raw_kurir_col:
                raw_kurir_col = next((c for c in df_raw.columns if 'delivery option' in c), None)
            if not raw_kurir_col:
                raw_kurir_col = next((c for c in df_raw.columns if 'kurir' in c), None)
        
        if raw_kurir_col:
            # Hitung per kurir
            stats = df_raw[raw_kurir_col].fillna('BLANK').value_counts().reset_index()
            stats.columns = ['Jenis Kurir', 'Jumlah Order (Raw)']
            stats['Sumber Data'] = mp_type
            # Cek status di kamus (Optional decoration)
            stats['Status Kamus'] = stats['Jenis Kurir'].apply(lambda x: '✅ Instant' if x in instant_list else '❌ Non-Instant')
            raw_stats_list.append(stats)
        else:
            raw_stats_list.append(pd.DataFrame({
                'Sumber Data': [mp_type],
                'Jenis Kurir': ['(Kolom Kurir Tidak Ditemukan)'],
                'Jumlah Order (Raw)': [len(df_raw)],
                'Status Kamus': ['-']
            }))

        # --- B. FILTERING LOGIC ---
        
        # 1. SHOPEE OFFICIAL
        if mp_type == 'Shopee (Official)':
            status_c = next((c for c in df_raw.columns if 'status' in c), None)
            resi_c = next((c for c in df_raw.columns if 'resi' in c), None)
            kurir_c = next((c for c in df_raw.columns if any(x in c for x in ['opsi','kirim'])), None)
            managed_c = next((c for c in df_raw.columns if 'dikelola' in c), None)

            if all([status_c, resi_c, kurir_c, managed_c]):
                c1 = df_raw[status_c].astype(str).str.strip() == 'Perlu Dikirim'
                c2 = df_raw[resi_c].fillna('').astype(str).str.strip().isin(['','nan','none'])
                c3 = df_raw[managed_c].astype(str).str.strip().str.lower() == 'no'
                c4 = df_raw[kurir_c].astype(str).str.strip().isin(instant_list)
                
                df_filtered = df_raw[c1 & c2 & c3 & c4].copy()
                
                if DEBUG_MODE:
                    st.sidebar.text(f"  > Status OK: {c1.sum()}")
                    st.sidebar.text(f"  > Resi Blank: {c2.sum()}")
                    st.sidebar.text(f"  > Managed No: {c3.sum()}")
                    st.sidebar.text(f"  > Kurir Instant: {c4.sum()}")
            else:
                missing = []
                if not status_c: missing.append("Status")
                if not resi_c: missing.append("Resi")
                if not kurir_c: missing.append("Opsi Kirim")
                if not managed_c: missing.append("Dikelola")
                st.error(f"Shopee Official: Kolom tidak lengkap. Tidak ditemukan: {', '.join(missing)}")

        # 2. SHOPEE INHOUSE
        elif mp_type == 'Shopee (INHOUSE)':
            status_c = next((c for c in df_raw.columns if 'status' in c), None)
            resi_c = next((c for c in df_raw.columns if 'resi' in c), None)
            kurir_c = next((c for c in df_raw.columns if any(x in c for x in ['opsi','kirim'])), None)
            
            if all([status_c, resi_c, kurir_c]):
                c1 = df_raw[status_c].astype(str).str.strip() == 'Perlu Dikirim'
                c2 = df_raw[resi_c].fillna('').astype(str).str.strip().isin(['','nan','none'])
                c3 = df_raw[kurir_c].astype(str).str.strip().isin(instant_list)
                
                df_filtered = df_raw[c1 & c2 & c3].copy()
                
                if DEBUG_MODE:
                    st.sidebar.text(f"  > Status OK: {c1.sum()}")
                    st.sidebar.text(f"  > Resi Blank: {c2.sum()}")
                    st.sidebar.text(f"  > Kurir Instant: {c3.sum()}")
            else:
                missing = []
                if not status_c: missing.append("Status")
                if not resi_c: missing.append("Resi")
                if not kurir_c: missing.append("Opsi Kirim")
                st.error(f"Shopee Inhouse: Kolom tidak lengkap. Tidak ditemukan: {', '.join(missing)}")

        # 3. TOKOPEDIA
        elif mp_type == 'Tokopedia':
            status_c = next((c for c in df_raw.columns if 'status' in c), None)
            if status_c:
                c1 = df_raw[status_c].astype(str).str.strip().str.lower() == 'perlu dikirim'
                df_filtered = df_raw[c1].copy()
                if DEBUG_MODE:
                     st.sidebar.text(f"  > Status OK: {c1.sum()}")
            else:
                st.error("Tokopedia: Kolom Status tidak ditemukan")

        # --- C. MAPPING (DENGAN KOLOM SPESIFIK "NOMOR REFERENSI SKU") ---
        if df_filtered.empty:
            if DEBUG_MODE: st.sidebar.warning(f"  > 0 data lolos filter.")
            continue

        if DEBUG_MODE: st.sidebar.success(f"  > {len(df_filtered)} data diproses.")

        # MODIFIKASI DI SINI: Gunakan kolom spesifik "Nomor Referensi SKU"
        # Cari dengan case-insensitive dan berbagai variasi
        col_sku = None
        for col_name in df_raw.columns:
            if 'nomor referensi sku' in col_name.lower():
                col_sku = col_name
                break
        
        # Jika tidak ditemukan, coba variasi lain
        if not col_sku:
            for col_name in df_raw.columns:
                if all(word in col_name.lower() for word in ['nomor', 'referensi', 'sku']):
                    col_sku = col_name
                    break
        
        # Jika masih tidak ditemukan, fallback ke logika lama (hanya untuk debug)
        if not col_sku:
            if DEBUG_MODE:
                st.sidebar.warning(f"  > Kolom 'Nomor Referensi SKU' tidak ditemukan, fallback ke logika lama")
            col_sku = next((c for c in df_raw.columns if 'sku' in c), 'SKU')
        else:
            if DEBUG_MODE:
                st.sidebar.text(f"  > Menggunakan kolom SKU: '{col_sku}'")

        col_qty = next((c for c in df_raw.columns if any(x in c for x in ['jumlah','quantity'])), 'Jumlah')
        col_ord = next((c for c in df_raw.columns if any(x in c for x in ['pesanan','order','invoice'])), 'Order ID')

        if DEBUG_MODE and col_sku not in df_filtered.columns:
            st.sidebar.error(f"  > ERROR: Kolom '{col_sku}' tidak ada dalam data yang difilter!")
        
        for _, row in df_filtered.iterrows():
            # Pastikan kolom SKU ada sebelum mengaksesnya
            if col_sku not in df_filtered.columns:
                if DEBUG_MODE:
                    st.sidebar.error(f"  > SKIP: Kolom '{col_sku}' tidak tersedia")
                continue
                
            raw_sku = str(row.get(col_sku, ''))
            sku_clean = clean_sku(raw_sku)
            order_id = str(row.get(col_ord, ''))
            try: qty = float(str(row.get(col_qty, 0)).replace(',', '.'))
            except: qty = 0
            
            if not sku_clean or qty <= 0: 
                if DEBUG_MODE and raw_sku:
                    st.sidebar.text(f"  > SKIP SKU: '{raw_sku}' -> cleaned: '{sku_clean}'")
                continue
            
            if sku_clean in bundle_map:
                for comp_sku, comp_qty in bundle_map[sku_clean]:
                    all_rows.append({
                        'Marketplace': mp_type,
                        'Order ID': order_id,
                        'SKU Original': raw_sku,
                        'Is Bundle': 'Yes',
                        'SKU Component': comp_sku,
                        'Nama Produk': sku_name_map.get(comp_sku, comp_sku),
                        'Qty Total': qty * comp_qty
                    })
            else:
                all_rows.append({
                    'Marketplace': mp_type,
                    'Order ID': order_id,
                    'SKU Original': raw_sku,
                    'Is Bundle': 'No',
                    'SKU Component': sku_clean,
                    'Nama Produk': sku_name_map.get(sku_clean, sku_clean),
                    'Qty Total': qty
                })

    # FINAL RESULTS
    df_detail = pd.DataFrame(all_rows)
    if not df_detail.empty:
        df_summary = df_detail.groupby(['Marketplace', 'SKU Component', 'Nama Produk'], as_index=False)['Qty Total'].sum()
        df_summary = df_summary.sort_values('Qty Total', ascending=False)
    else:
        df_summary = pd.DataFrame()

    df_raw_stats = pd.concat(raw_stats_list, ignore_index=True) if raw_stats_list else pd.DataFrame()
    
    return {'detail': df_detail, 'summary': df_summary, 'raw_stats': df_raw_stats}, None

# --- UI STREAMLIT ---
st.sidebar.header("📁 1. Upload Kamus")
kamus_f = st.sidebar.file_uploader("Kamus.xlsx", key="k")

st.sidebar.header("📁 2. Upload Order")
shp_off_f = st.sidebar.file_uploader("Shopee (Official)", key="so")
shp_inh_f = st.sidebar.file_uploader("Shopee (INHOUSE)", key="si")
tok_f = st.sidebar.file_uploader("Tokopedia", key="toped")

if st.sidebar.button("🚀 PROSES DATA", type="primary"):
    if not kamus_f:
        st.error("❌ Upload Kamus dulu!")
    else:
        files = []
        if shp_off_f: files.append(('Shopee (Official)', shp_off_f))
        if shp_inh_f: files.append(('Shopee (INHOUSE)', shp_inh_f))
        if tok_f: files.append(('Tokopedia', tok_f))
        
        if not files:
            st.error("❌ Upload minimal satu file order!")
        else:
            with st.spinner("Processing..."):
                try:
                    k_xl = pd.ExcelFile(kamus_f)
                    k_data = {}
                    for key, keywords in [('kurir',['kurir','courier']), ('bundle',['bundle','kit']), ('sku',['sku','product'])]:
                        sheet = next((s for s in k_xl.sheet_names if any(k in s.lower() for k in keywords)), None)
                        if sheet: k_data[key] = pd.read_excel(k_xl, sheet_name=sheet, dtype=str)
                    
                    if len(k_data) < 3:
                        st.error("❌ Kamus tidak lengkap (Cek sheet Kurir, Bundle, SKU)")
                    else:
                        res, err = process_universal_data(files, k_data)
                        
                        if err: st.warning(err)
                        
                        # TABS
                        t1, t2, t3 = st.tabs(["📋 Picking List", "📦 Stock Summary", "🔍 Validasi Kurir"])
                        
                        with t1:
                            if not res['detail'].empty:
                                st.dataframe(res['detail'], use_container_width=True)
                            else: st.info("Tidak ada data picking list.")
                        
                        with t2:
                            if not res['summary'].empty:
                                st.metric("Total Qty", res['summary']['Qty Total'].sum())
                                st.dataframe(res['summary'], use_container_width=True)
                            else: st.info("Tidak ada summary.")

                        with t3:
                            st.markdown("### 🔍 Cek Total Order per Kurir (Data Mentah)")
                            st.caption("Data ini diambil sebelum filter status/resi. Gunakan untuk validasi jika ada order yang 'hilang'.")
                            if not res['raw_stats'].empty:
                                st.dataframe(res['raw_stats'], use_container_width=True)
                            else: st.info("Tidak ada data statistik kurir.")
                        
                        # Download Logic
                        if not res['detail'].empty:
                            buf = io.BytesIO()
                            with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
                                res['detail'].to_excel(writer, sheet_name='Picking List', index=False)
                                res['summary'].to_excel(writer, sheet_name='Stock Check', index=False)
                                if not res['raw_stats'].empty:
                                    res['raw_stats'].to_excel(writer, sheet_name='Validasi Kurir', index=False)
                                
                                # Auto width
                                for sheet in writer.sheets.values():
                                    sheet.set_column(0, 5, 20)

                            st.download_button(
                                "📥 Download Excel Report",
                                data=buf.getvalue(),
                                file_name=f"Report_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                type="primary"
                            )

                except Exception as e:
                    st.error(f"❌ System Error: {e}")

st.sidebar.markdown("---")
st.sidebar.caption("v3.3 - SKU dari kolom 'Nomor Referensi SKU'")
