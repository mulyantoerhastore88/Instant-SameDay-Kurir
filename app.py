import streamlit as st
import pandas as pd
import numpy as np
import io
import time
from datetime import datetime

# --- CONFIG ---
st.set_page_config(page_title="Universal Order Processor (Final)", layout="wide")
st.title("🛒 Universal Marketplace Order Processor")
st.markdown("""
**Logic Applied:**
1. **Shopee**: Status='Perlu Dikirim' | Resi=Blank | Managed='No' | Kurir=Instant(Kamus).
2. **Tokopedia**: Status='Perlu Dikirim'.
3. **Smart Loader**: Support Excel (.xlsx) & CSV, Auto-remove header sampah.
""")

# --- FUNGSI CLEANING SKU ---
def clean_sku(sku):
    """Ambil bagian kanan hyphen, hapus spasi/karakter aneh."""
    if pd.isna(sku): return ""
    sku = str(sku).strip()
    sku = ''.join(char for char in sku if ord(char) >= 32)
    if '-' in sku:
        return sku.split('-', 1)[-1].strip()
    return sku

# --- FUNGSI SMART LOADER (KREATIF) ---
def load_data_smart(file_obj):
    """
    Mencoba membaca file sebagai Excel ATAU CSV.
    Mencari baris header yang benar secara otomatis.
    """
    df = None
    filename = file_obj.name.lower()
    
    # 1. Coba Baca Sesuai Ekstensi
    try:
        if filename.endswith('.csv'):
            try:
                df = pd.read_csv(file_obj, dtype=str, header=None)
            except:
                file_obj.seek(0)
                df = pd.read_csv(file_obj, sep=';', dtype=str, header=None)
        else:
            # Asumsi Excel (.xlsx / .xls)
            df = pd.read_excel(file_obj, dtype=str, header=None)
    except Exception as e:
        return None, f"Gagal baca file dasar: {e}"

    if df is None or df.empty:
        return None, "File kosong atau format rusak."

    # 2. Cari Baris Header Sebenarnya (Anti-Sampah)
    # Kita cari baris yang mengandung keyword spesifik marketplace
    header_idx = -1
    keywords = ['status pesanan', 'order status', 'no. pesanan', 'order id', 'seller sku']
    
    for i, row in df.head(15).iterrows():
        # Gabungkan isi baris jadi satu string lowercase untuk pengecekan
        row_str = " ".join([str(val).lower() for val in row.values])
        if any(kw in row_str for kw in keywords):
            header_idx = i
            break
    
    if header_idx == -1:
        return None, "Header tidak ditemukan (Keyword: Status Pesanan/Order Status)."

    # 3. Set Header dan Rapikan
    try:
        df_final = df.iloc[header_idx:].copy()
        df_final.columns = df_final.iloc[0] # Baris ini jadi nama kolom
        df_final = df_final.iloc[1:].reset_index(drop=True) # Hapus baris header dari data
        df_final.columns = df_final.columns.astype(str).str.strip() # Hapus spasi di nama kolom
        return df_final, None
    except Exception as e:
        return None, f"Error saat set header: {e}"

# ==========================================
# CORE PROCESSING
# ==========================================
def process_universal_data(uploaded_files, kamus_data):
    start_time = time.time()
    
    # 1. LOAD KAMUS
    try:
        df_kurir = kamus_data['kurir']
        df_bundle = kamus_data['bundle']
        df_sku = kamus_data['sku']

        # Pre-clean Bundle Keys
        # Asumsi kolom kamus: Kit_Sku, Component_Sku, Component_Qty
        bundle_map = {}
        for _, row in df_bundle.iterrows():
            # Handle nama kolom variasi (huruf besar/kecil)
            kit_col = next((c for c in row.index if 'kit_sku' in c.lower()), None)
            comp_col = next((c for c in row.index if 'component_sku' in c.lower()), None)
            qty_col = next((c for c in row.index if 'qty' in c.lower()), None)
            
            if kit_col and comp_col:
                kit_sku = clean_sku(row[kit_col])
                comp_sku = clean_sku(row[comp_col])
                try:
                    qty = float(row[qty_col]) if qty_col else 1.0
                except:
                    qty = 1.0
                
                if kit_sku:
                    if kit_sku not in bundle_map: bundle_map[kit_sku] = []
                    bundle_map[kit_sku].append((comp_sku, qty))

        # Pre-clean SKU Name Map
        # Asumsi kolom: Product_Sku, Product_Name
        name_map = {}
        # Cari kolom yg pas (biar robust)
        psku_col = next((c for c in df_sku.columns if 'product_sku' in c.lower() or 'sku' in c.lower()), df_sku.columns[0])
        pname_col = next((c for c in df_sku.columns if 'product_name' in c.lower() or 'name' in c.lower()), df_sku.columns[1])
        
        for _, row in df_sku.iterrows():
            k = clean_sku(row[psku_col])
            name_map[k] = str(row[pname_col])

        # List Kurir Instant Shopee (Yes only)
        instant_list = []
        if 'Instant/Same Day' in df_kurir.columns:
            # Ambil kolom pertama sebagai nama kurir
            k_col = df_kurir.columns[0]
            instant_list = df_kurir[
                df_kurir['Instant/Same Day'].astype(str).str.strip().str.lower().isin(['yes', 'ya', 'true'])
            ][k_col].tolist()
            
    except Exception as e:
        return None, f"Error memproses data Kamus: {e}"

    all_rows = []

    # 2. PROCESS FILES
    for mp_type, file_obj in uploaded_files:
        # Load File dengan Smart Loader
        df_raw, err = load_data_smart(file_obj)
        if err:
            st.error(f"File {mp_type} error: {err}")
            continue
            
        df_filtered = pd.DataFrame()
        
        # --- SHOPEE LOGIC ---
        if mp_type == 'Shopee':
            # Pastikan kolom wajib ada
            req_cols = ['Status Pesanan', 'No. Resi', 'Pesanan yang Dikelola Shopee', 'Opsi Pengiriman', 'Nomor Referensi SKU', 'Jumlah']
            missing = [c for c in req_cols if c not in df_raw.columns]
            if missing:
                st.error(f"Shopee Missing Columns: {missing}")
                continue

            # FILTER 4 LAPIS (AND)
            # 1. Status == Perlu Dikirim
            c1 = df_raw['Status Pesanan'] == 'Perlu Dikirim'
            
            # 2. Managed == No (Case insensitive just in case)
            c2 = df_raw['Pesanan yang Dikelola Shopee'].astype(str).str.strip().str.lower() == 'no'
            
            # 3. Resi == Kosong/NaN
            c3 = df_raw['No. Resi'].isna() | (df_raw['No. Resi'].astype(str).str.strip() == '') | (df_raw['No. Resi'].astype(str).str.lower() == 'nan')
            
            # 4. Kurir == Ada di list Kamus (Yes)
            c4 = df_raw['Opsi Pengiriman'].isin(instant_list)
            
            df_filtered = df_raw[c1 & c2 & c3 & c4].copy()
            
            # Mapping
            col_sku = 'Nomor Referensi SKU'
            col_qty = 'Jumlah'
            col_ord = 'No. Pesanan'

        # --- TOKOPEDIA LOGIC ---
        elif mp_type == 'Tokopedia':
            # Cari nama kolom yang pas (fleksibel)
            status_col = next((c for c in df_raw.columns if 'status' in c.lower()), None)
            sku_col = next((c for c in df_raw.columns if 'seller sku' in c.lower() or 'nomor sku' in c.lower()), None)
            qty_col = next((c for c in df_raw.columns if 'quantity' in c.lower() or 'jumlah' in c.lower()), None)
            ord_col = next((c for c in df_raw.columns if 'order id' in c.lower() or 'invoice' in c.lower()), None)

            if not (status_col and sku_col and qty_col):
                st.error(f"Tokopedia Missing Columns. Detected: {list(df_raw.columns)}")
                continue

            # FILTER: Status == Perlu Dikirim (Case Insensitive)
            df_filtered = df_raw[
                df_raw[status_col].astype(str).str.strip().str.lower() == 'perlu dikirim'
            ].copy()
            
            col_sku = sku_col
            col_qty = qty_col
            col_ord = ord_col

        if df_filtered.empty:
            continue

        # 3. EXPANSION LOOP
        for _, row in df_filtered.iterrows():
            raw_sku = str(row.get(col_sku, ''))
            sku_clean = clean_sku(raw_sku)
            
            # Handle Qty (String with comma -> Float)
            try:
                q_str = str(row.get(col_qty, 0)).replace(',', '.')
                qty_order = float(q_str)
            except:
                qty_order = 0

            # Logic Bundle
            if sku_clean in bundle_map:
                # IS BUNDLE
                comps = bundle_map[sku_clean]
                for comp_sku, comp_qty_unit in comps:
                    all_rows.append({
                        'Marketplace': mp_type,
                        'Order ID': row[col_ord],
                        'SKU Original': raw_sku,
                        'Is Bundle?': 'Yes',
                        'SKU Component': comp_sku,
                        'Nama Produk': name_map.get(comp_sku, comp_sku),
                        'Qty Total': qty_order * comp_qty_unit
                    })
            else:
                # IS SINGLE
                all_rows.append({
                    'Marketplace': mp_type,
                    'Order ID': row[col_ord],
                    'SKU Original': raw_sku,
                    'Is Bundle?': 'No',
                    'SKU Component': sku_clean,
                    'Nama Produk': name_map.get(sku_clean, sku_clean),
                    'Qty Total': qty_order
                })

    if not all_rows:
        return None, "Data berhasil dibaca, tapi TIDAK ADA yang lolos Filter. Cek kembali Status/Kurir/Resi."

    # 4. FINAL DATAFRAME
    df_detail = pd.DataFrame(all_rows)
    
    # Summary
    df_summary = df_detail.groupby(['Marketplace', 'SKU Component', 'Nama Produk']).agg({
        'Qty Total': 'sum'
    }).reset_index().sort_values('Qty Total', ascending=False)
    
    return {'detail': df_detail, 'summary': df_summary}, None

# --- UI ---
st.sidebar.header("📁 Upload Kamus (Wajib)")
kamus_f = st.sidebar.file_uploader("Kamus Dashboard.xlsx", type=['xlsx'])

st.sidebar.header("📁 Upload Order (Excel/CSV)")
shp_f = st.sidebar.file_uploader("Order Shopee", type=['xlsx', 'csv'])
tok_f = st.sidebar.file_uploader("Order Tokopedia", type=['xlsx', 'csv'])

if st.sidebar.button("🚀 PROSES DATA"):
    if not kamus_f:
        st.error("Upload Kamus dulu bro!")
    elif not shp_f and not tok_f:
        st.error("Minimal satu file order bro!")
    else:
        with st.spinner("Processing..."):
            try:
                # Load Kamus
                k_excel = pd.ExcelFile(kamus_f)
                k_data = {
                    'kurir': pd.read_excel(k_excel, 'Kurir-Shopee'),
                    'bundle': pd.read_excel(k_excel, 'Bundle Master'),
                    'sku': pd.read_excel(k_excel, 'SKU Master')
                }
                
                files = []
                if shp_f: files.append(('Shopee', shp_f))
                if tok_f: files.append(('Tokopedia', tok_f))
                
                res, err_msg = process_universal_data(files, k_data)
                
                if err_msg:
                    st.warning(err_msg)
                else:
                    st.success("Mantap! Data berhasil diproses.")
                    st.session_state.result = res
                    
            except Exception as e:
                st.error(f"System Error: {e}")

# --- OUTPUT ---
if 'result' in st.session_state:
    res = st.session_state.result
    
    t1, t2 = st.tabs(["📋 Detail Order", "📦 Total per SKU"])
    
    with t1: st.dataframe(res['detail'], use_container_width=True)
    with t2: st.dataframe(res['summary'], use_container_width=True)
    
    # Download
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='xlsxwriter') as writer:
        res['detail'].to_excel(writer, sheet_name='Picking List', index=False)
        res['summary'].to_excel(writer, sheet_name='Stock Check', index=False)
        
    st.download_button(
        "📥 Download Excel", 
        data=buf.getvalue(), 
        file_name="Picking_List_Final.xlsx", 
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
