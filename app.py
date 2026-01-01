import streamlit as st
import pandas as pd
import numpy as np
import io
import time
from datetime import datetime

# --- CONFIG ---
st.set_page_config(page_title="Universal Order Processor", layout="wide")
st.title("🛒 Universal Marketplace Order Processor")
st.markdown("""
**Current Logic:**
1. **Shopee**: Status='Perlu Dikirim' | Resi=Blank | Managed='No' | Kurir=Instant(Kamus).
2. **Tokopedia**: Status='Perlu Dikirim' (Simple Filter).
3. **Features**: Smart Header Detection & Auto-Encoding Fix.
""")

# --- FUNGSI CLEANING SKU ---
def clean_sku(sku):
    """Ambil bagian kanan hyphen, hapus spasi/karakter aneh."""
    if pd.isna(sku): return ""
    sku = str(sku).strip()
    # Hapus karakter non-printable
    sku = ''.join(char for char in sku if ord(char) >= 32)
    if '-' in sku:
        return sku.split('-', 1)[-1].strip()
    return sku

# --- FUNGSI SMART LOADER (SUPER ROBUST) ---
def load_data_smart(file_obj):
    """
    Membaca file dengan berbagai metode (Excel/CSV) dan encoding.
    Otomatis mencari baris header yang valid.
    """
    df = None
    filename = file_obj.name.lower()
    
    # 1. BACA FILE (RAW)
    try:
        if filename.endswith('.csv'):
            # Coba UTF-8 dulu
            try:
                df = pd.read_csv(file_obj, dtype=str, header=None)
            except UnicodeDecodeError:
                # Kalau gagal (kasus file Tokped kamu), pakai Latin-1
                file_obj.seek(0)
                df = pd.read_csv(file_obj, sep=',', dtype=str, header=None, encoding='latin-1')
            
            # Cek separator, kalau kolom cuma 1 berarti salah separator (misal titik koma)
            if len(df.columns) < 2:
                file_obj.seek(0)
                try:
                    df = pd.read_csv(file_obj, sep=';', dtype=str, header=None)
                except UnicodeDecodeError:
                    file_obj.seek(0)
                    df = pd.read_csv(file_obj, sep=';', dtype=str, header=None, encoding='latin-1')
        else:
            # Excel (.xlsx)
            df = pd.read_excel(file_obj, dtype=str, header=None)
    except Exception as e:
        return None, f"Gagal membaca fisik file: {e}"

    if df is None or df.empty:
        return None, "File kosong."

    # 2. CARI BARIS HEADER SEBENARNYA
    # Mencari baris yang mengandung keyword kolom marketplace
    header_idx = -1
    keywords = ['status pesanan', 'order status', 'no. pesanan', 'order id', 'seller sku']
    
    # Scan 20 baris pertama
    for i, row in df.head(20).iterrows():
        row_str = " ".join([str(val).lower() for val in row.values])
        if any(kw in row_str for kw in keywords):
            header_idx = i
            break
    
    if header_idx == -1:
        # Fallback: Coba anggap baris 0 adalah header jika tidak ketemu
        header_idx = 0
        # return None, "Header tidak ditemukan (Kata kunci: Order Status / Status Pesanan)."

    # 3. SET HEADER & BERSIHKAN
    try:
        df_final = df.iloc[header_idx:].copy()
        df_final.columns = df_final.iloc[0] # Jadikan baris ini nama kolom
        df_final = df_final.iloc[1:].reset_index(drop=True) # Hapus baris header dari data
        # Bersihkan nama kolom (hapus spasi depan/belakang)
        df_final.columns = df_final.columns.astype(str).str.strip()
        return df_final, None
    except Exception as e:
        return None, f"Error saat set header: {e}"

# ==========================================
# MAIN PROCESSOR
# ==========================================
def process_universal_data(uploaded_files, kamus_data):
    start_time = time.time()
    
    # 1. LOAD & MAP KAMUS
    try:
        df_kurir = kamus_data['kurir']
        df_bundle = kamus_data['bundle']
        df_sku = kamus_data['sku']

        # A. Mapping Bundle (Kit_Sku -> List Component)
        bundle_map = {}
        for _, row in df_bundle.iterrows():
            # Cari kolom yang namanya mirip (case insensitive)
            cols = {c.lower(): c for c in df_bundle.columns}
            kit_c = cols.get('kit_sku') or cols.get('sku bundle')
            comp_c = cols.get('component_sku') or cols.get('sku component')
            qty_c = cols.get('component_qty') or cols.get('component quantity') or cols.get('qty')

            if kit_c and comp_c:
                kit_val = clean_sku(row[kit_c])
                comp_val = clean_sku(row[comp_c])
                try:
                    qty_val = float(str(row[qty_c]).replace(',', '.')) if qty_c else 1.0
                except:
                    qty_val = 1.0
                
                if kit_val:
                    if kit_val not in bundle_map: bundle_map[kit_val] = []
                    bundle_map[kit_val].append((comp_val, qty_val))

        # B. Mapping SKU Name (Kolom B -> Kolom C)
        # Asumsi: Kolom ke-2 adalah Kode, Kolom ke-3 adalah Nama
        sku_name_map = {}
        if len(df_sku.columns) >= 2:
            # Pakai iloc biar aman dari perubahan nama header
            # Index 1 = Kolom B (Kode), Index 2 = Kolom C (Nama)
            # Cek jika kolom cukup
            idx_code = 1 if len(df_sku.columns) > 2 else 0
            idx_name = 2 if len(df_sku.columns) > 2 else 1
            
            for _, row in df_sku.iterrows():
                try:
                    code = clean_sku(row.iloc[idx_code])
                    name = str(row.iloc[idx_name])
                    if code: sku_name_map[code] = name
                except:
                    continue

        # C. List Kurir Instant Shopee
        instant_list = []
        if 'Instant/Same Day' in df_kurir.columns:
            k_col = df_kurir.columns[0]
            instant_list = df_kurir[
                df_kurir['Instant/Same Day'].astype(str).str.strip().str.lower().isin(['yes', 'ya', 'true'])
            ][k_col].tolist()
            
    except Exception as e:
        return None, f"Error memproses data Kamus: {e}"

    all_rows = []

    # 2. LOOP SETIAP FILE ORDER
    for mp_type, file_obj in uploaded_files:
        # Load dengan Smart Loader (Anti-Error Encoding & Header)
        df_raw, err = load_data_smart(file_obj)
        if err:
            st.error(f"File {mp_type} Gagal: {err}")
            continue
            
        df_filtered = pd.DataFrame()
        col_sku, col_qty, col_ord = '', '', ''
        
        # --- LOGIC SHOPEE ---
        if mp_type == 'Shopee':
            # Cek Kolom Wajib
            req = ['Status Pesanan', 'No. Resi', 'Pesanan yang Dikelola Shopee', 'Opsi Pengiriman']
            if not all(c in df_raw.columns for c in req):
                st.error(f"Shopee: Kolom tidak lengkap. Yang terbaca: {list(df_raw.columns)}")
                continue

            # Filter 1: Status Perlu Dikirim
            c1 = df_raw['Status Pesanan'] == 'Perlu Dikirim'
            # Filter 2: Managed No
            c2 = df_raw['Pesanan yang Dikelola Shopee'].astype(str).str.strip().str.lower() == 'no'
            # Filter 3: Resi Kosong
            c3 = df_raw['No. Resi'].isna() | (df_raw['No. Resi'].astype(str).str.strip() == '') | (df_raw['No. Resi'].astype(str).str.lower() == 'nan')
            # Filter 4: Kurir Instant (Kamus)
            c4 = df_raw['Opsi Pengiriman'].isin(instant_list)
            
            df_filtered = df_raw[c1 & c2 & c3 & c4].copy()
            col_sku = 'Nomor Referensi SKU'
            col_qty = 'Jumlah'
            col_ord = 'No. Pesanan'

        # --- LOGIC TOKOPEDIA ---
        elif mp_type == 'Tokopedia':
            # Cari kolom Order Status yang fleksibel
            status_col = next((c for c in df_raw.columns if 'status' in c.lower()), None)
            
            if not status_col:
                st.error(f"Tokopedia: Kolom 'Order Status' tidak ditemukan. Header: {list(df_raw.columns)}")
                continue

            # Filter: HANYA Status "Perlu dikirim"
            df_filtered = df_raw[
                df_raw[status_col].astype(str).str.strip().str.lower() == 'perlu dikirim'
            ].copy()
            
            # Cari kolom SKU dan Qty
            col_sku = next((c for c in df_raw.columns if 'seller sku' in c.lower() or 'nomor sku' in c.lower()), 'Seller SKU')
            col_qty = next((c for c in df_raw.columns if 'quantity' in c.lower() or 'jumlah' in c.lower()), 'Quantity')
            col_ord = next((c for c in df_raw.columns if 'order id' in c.lower() or 'invoice' in c.lower()), 'Order ID')

        if df_filtered.empty:
            continue

        # 3. EXPANSION (BUNDLE -> COMPONENT)
        for _, row in df_filtered.iterrows():
            raw_sku = str(row.get(col_sku, ''))
            sku_clean = clean_sku(raw_sku)
            
            # Handle Qty (aman dari koma/string)
            try:
                q_val = str(row.get(col_qty, 0)).replace(',', '.')
                qty_order = float(q_val)
            except:
                qty_order = 0

            # Logic Bundle
            if sku_clean in bundle_map:
                # IS BUNDLE -> Expand rows
                for comp_sku, comp_qty_unit in bundle_map[sku_clean]:
                    all_rows.append({
                        'Marketplace': mp_type,
                        'Order ID': row.get(col_ord, ''),
                        'SKU Original': raw_sku,
                        'Is Bundle?': 'Yes',
                        'SKU Component': comp_sku,
                        'Nama Produk': sku_name_map.get(comp_sku, comp_sku), # Lookup Nama
                        'Qty Total': qty_order * comp_qty_unit
                    })
            else:
                # IS SINGLE
                all_rows.append({
                    'Marketplace': mp_type,
                    'Order ID': row.get(col_ord, ''),
                    'SKU Original': raw_sku,
                    'Is Bundle?': 'No',
                    'SKU Component': sku_clean,
                    'Nama Produk': sku_name_map.get(sku_clean, sku_clean), # Lookup Nama
                    'Qty Total': qty_order
                })

    if not all_rows:
        return None, "Data terbaca tapi 0 lolos filter. Cek Status/Kurir/Resi file order kamu."

    # 4. FINAL AGGREGATION
    df_detail = pd.DataFrame(all_rows)
    
    df_summary = df_detail.groupby(['Marketplace', 'SKU Component', 'Nama Produk']).agg({
        'Qty Total': 'sum'
    }).reset_index().sort_values('Qty Total', ascending=False)
    
    return {'detail': df_detail, 'summary': df_summary}, None

# --- UI STREAMLIT ---
st.sidebar.header("📁 1. Upload Kamus (Wajib)")
kamus_f = st.sidebar.file_uploader("Kamus Dashboard.xlsx", type=['xlsx'])

st.sidebar.header("📁 2. Upload Order")
shp_f = st.sidebar.file_uploader("Order Shopee", type=['xlsx', 'csv'])
tok_f = st.sidebar.file_uploader("Order Tokopedia", type=['xlsx', 'csv'])

if st.sidebar.button("🚀 PROSES DATA"):
    if not kamus_f:
        st.error("Upload Kamus dulu bro!")
    elif not shp_f and not tok_f:
        st.error("Upload minimal satu file order!")
    else:
        with st.spinner("Processing..."):
            try:
                # Load Kamus
                k_excel = pd.ExcelFile(kamus_f)
                k_data = {
                    'kurir': pd.read_excel(k_excel, sheet_name='Kurir-Shopee'),
                    'bundle': pd.read_excel(k_excel, sheet_name='Bundle Master'),
                    'sku': pd.read_excel(k_excel, sheet_name='SKU Master')
                }
                
                files = []
                if shp_f: files.append(('Shopee', shp_f))
                if tok_f: files.append(('Tokopedia', tok_f))
                
                res, err_msg = process_universal_data(files, k_data)
                
                if err_msg:
                    st.warning(err_msg)
                else:
                    st.success(f"Sukses! Total Item: {res['summary']['Qty Total'].sum()}")
                    st.session_state.result = res
                    
            except Exception as e:
                st.error(f"System Crash: {e}")

# --- OUTPUT AREA ---
if 'result' in st.session_state:
    res = st.session_state.result
    
    t1, t2 = st.tabs(["📋 Picking List (Detail)", "📦 Stock Check (Summary)"])
    
    with t1: st.dataframe(res['detail'], use_container_width=True)
    with t2: st.dataframe(res['summary'], use_container_width=True)
    
    # Download Button
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
        res['detail'].to_excel(writer, sheet_name='Picking List', index=False)
        res['summary'].to_excel(writer, sheet_name='Stock Check', index=False)
        # Adjust width
        writer.sheets['Picking List'].set_column('A:Z', 18)
        
    st.download_button(
        "📥 Download Excel Final",
        data=buf.getvalue(),
        file_name=f"Picking_List_{datetime.now().strftime('%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
