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
**Logic Applied:**
1. **Shopee**: Status='Perlu Dikirim' | Resi=Blank | Managed='No' | Kurir=Instant(Kamus).
2. **Tokopedia**: Status='Perlu Dikirim'.
3. **Fitur**: Prioritas baca file Excel (.xlsx).
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

# --- FUNGSI SMART LOADER (PRIORITAS EXCEL) ---
def load_data_smart(file_obj):
    """
    Mencoba membaca file. Prioritas: Excel -> CSV.
    Lalu mencari baris header yang benar.
    """
    df = None
    filename = file_obj.name.lower()
    
    # 1. COBA BACA FILE (RAW)
    try:
        # Prioritas 1: Baca sebagai EXCEL (.xlsx)
        # Kita paksa coba baca excel dulu karena user bilang datanya xlsx
        try:
            df = pd.read_excel(file_obj, dtype=str, header=None, engine='openpyxl')
        except:
            # Fallback: Jika gagal, mungkin itu CSV yang dinamai xlsx, atau CSV beneran
            file_obj.seek(0)
            try:
                df = pd.read_csv(file_obj, dtype=str, header=None) # Coba UTF-8
            except:
                file_obj.seek(0)
                df = pd.read_csv(file_obj, sep=';', dtype=str, header=None) # Coba Separator ;
                
    except Exception as e:
        return None, f"Gagal membaca fisik file. Pastikan format .xlsx valid: {e}"

    if df is None or df.empty:
        return None, "File kosong atau rusak."

    # 2. CARI BARIS HEADER SEBENARNYA (Anti-Sampah)
    # Kita cari baris yang mengandung keyword kolom marketplace
    header_idx = -1
    keywords = ['status pesanan', 'order status', 'no. pesanan', 'order id', 'seller sku']
    
    # Scan 20 baris pertama
    for i, row in df.head(20).iterrows():
        # Gabungkan isi baris jadi satu string lowercase untuk pengecekan
        row_str = " ".join([str(val).lower() for val in row.values])
        if any(kw in row_str for kw in keywords):
            header_idx = i
            break
    
    if header_idx == -1:
        # Fallback: Jika tidak ketemu keyword, asumsikan baris pertama adalah header
        header_idx = 0

    # 3. SET HEADER & BERSIHKAN
    try:
        df_final = df.iloc[header_idx:].copy()
        df_final.columns = df_final.iloc[0] # Jadikan baris ini nama kolom
        df_final = df_final.iloc[1:].reset_index(drop=True) # Hapus baris header dari data
        
        # Bersihkan nama kolom (hapus spasi depan/belakang/enter)
        df_final.columns = df_final.columns.astype(str).str.replace('\n', ' ').str.strip()
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
            cols = {c.lower(): c for c in df_bundle.columns}
            kit_c = cols.get('kit_sku') or cols.get('sku bundle')
            comp_c = cols.get('component_sku') or cols.get('sku component')
            qty_c = cols.get('component_qty') or cols.get('component quantity') or cols.get('qty')

            if kit_c and comp_c:
                kit_val = clean_sku(row[kit_c])
                comp_val = clean_sku(row[comp_c])
                try:
                    # Handle qty desimal (koma jadi titik)
                    qty_val = float(str(row[qty_c]).replace(',', '.')) if qty_c else 1.0
                except:
                    qty_val = 1.0
                
                if kit_val:
                    if kit_val not in bundle_map: bundle_map[kit_val] = []
                    bundle_map[kit_val].append((comp_val, qty_val))

        # B. Mapping SKU Name (Kolom B -> Kolom C)
        sku_name_map = {}
        if len(df_sku.columns) >= 2:
            idx_code = 1 if len(df_sku.columns) > 2 else 0 # Ambil Kolom ke-2 (B)
            idx_name = 2 if len(df_sku.columns) > 2 else 1 # Ambil Kolom ke-3 (C)
            
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
        # Load dengan Smart Loader (Prioritas Excel)
        df_raw, err = load_data_smart(file_obj)
        if err:
            st.error(f"File {mp_type} ({file_obj.name}) Gagal: {err}")
            continue
            
        df_filtered = pd.DataFrame()
        col_sku, col_qty, col_ord = '', '', ''
        
        # --- LOGIC SHOPEE ---
        if mp_type == 'Shopee':
            # Cek Kolom Wajib (Case insensitive search)
            raw_cols_lower = [c.lower() for c in df_raw.columns]
            
            # Cari nama kolom yang tepat
            status_c = next((c for c in df_raw.columns if 'status' in c.lower()), None)
            managed_c = next((c for c in df_raw.columns if 'dikelola' in c.lower()), None)
            resi_c = next((c for c in df_raw.columns if 'resi' in c.lower()), None)
            kurir_c = next((c for c in df_raw.columns if 'opsi' in c.lower() or 'kirim' in c.lower()), None)
            
            if not (status_c and managed_c and resi_c and kurir_c):
                st.error(f"Shopee: Kolom tidak lengkap. Terbaca: {list(df_raw.columns)}")
                continue

            # Filter 1: Status Perlu Dikirim
            c1 = df_raw[status_c].astype(str).str.strip() == 'Perlu Dikirim'
            # Filter 2: Managed No
            c2 = df_raw[managed_c].astype(str).str.strip().str.lower() == 'no'
            # Filter 3: Resi Kosong
            c3 = df_raw[resi_c].isna() | (df_raw[resi_c].astype(str).str.strip() == '') | (df_raw[resi_c].astype(str).str.lower() == 'nan')
            # Filter 4: Kurir Instant (Kamus)
            c4 = df_raw[kurir_c].isin(instant_list)
            
            df_filtered = df_raw[c1 & c2 & c3 & c4].copy()
            
            col_sku = next((c for c in df_raw.columns if 'sku' in c.lower() and 'referensi' in c.lower()), 'Nomor Referensi SKU')
            col_qty = next((c for c in df_raw.columns if 'jumlah' in c.lower()), 'Jumlah')
            col_ord = next((c for c in df_raw.columns if 'pesanan' in c.lower()), 'No. Pesanan')

        # --- LOGIC TOKOPEDIA ---
        elif mp_type == 'Tokopedia':
            # Cari kolom fleksibel
            status_col = next((c for c in df_raw.columns if 'status' in c.lower()), None)
            
            if not status_col:
                st.error(f"Tokopedia: Kolom 'Order Status' tidak ditemukan. Header terbaca: {list(df_raw.columns)}")
                continue

            # FILTER: HANYA Status "Perlu dikirim" (Case insensitive)
            df_filtered = df_raw[
                df_raw[status_col].astype(str).str.strip().str.lower() == 'perlu dikirim'
            ].copy()
            
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
        return None, "Data terbaca tapi 0 lolos filter. Cek kembali Status/Kurir/Resi di file order."

    # 4. FINAL AGGREGATION
    df_detail = pd.DataFrame(all_rows)
    
    # Sortir kolom biar rapi
    cols_display = ['Marketplace', 'Order ID', 'SKU Original', 'Is Bundle?', 'SKU Component', 'Nama Produk', 'Qty Total']
    # Filter kolom yg ada aja
    final_cols = [c for c in cols_display if c in df_detail.columns]
    df_detail = df_detail[final_cols]

    df_summary = df_detail.groupby(['Marketplace', 'SKU Component', 'Nama Produk']).agg({
        'Qty Total': 'sum'
    }).reset_index().sort_values('Qty Total', ascending=False)
    
    return {'detail': df_detail, 'summary': df_summary}, None

# --- UI STREAMLIT ---
st.sidebar.header("📁 1. Upload Kamus (Wajib)")
kamus_f = st.sidebar.file_uploader("Kamus Dashboard.xlsx", type=['xlsx'])

st.sidebar.header("📁 2. Upload Order (Excel/CSV)")
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
                k_excel = pd.ExcelFile(kamus_f, engine='openpyxl')
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
        
    st.download_button(
        "📥 Download Excel Final",
        data=buf.getvalue(),
        file_name=f"Picking_List_{datetime.now().strftime('%H%M')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
