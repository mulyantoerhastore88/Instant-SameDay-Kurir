import streamlit as st
import pandas as pd
import numpy as np
import io
import time
from datetime import datetime

# --- CONFIG ---
st.set_page_config(page_title="Universal Order Processor v2", layout="wide")
st.title("🛒 Universal Marketplace Order Processor")
st.markdown("Update: **Kamus New Structure** | **Shopee Instant** | **Tokopedia Perlu Dikirim**")

# --- FUNGSI CLEANING SKU ---
def clean_sku(sku):
    if pd.isna(sku): return ""
    sku = str(sku).strip()
    sku = ''.join(char for char in sku if ord(char) >= 32)
    if '-' in sku:
        return sku.split('-', 1)[-1].strip()
    return sku

# --- FUNGSI CARI HEADER (UNTUK TOKPED/BIGSELLER) ---
def get_clean_df(file_obj):
    if file_obj.name.endswith('.csv'):
        df = pd.read_csv(file_obj, dtype=str, header=None)
    else:
        df = pd.read_excel(file_obj, dtype=str, header=None)
    
    # Cari baris yang berisi keyword order
    header_idx = 0
    for i, row in df.head(10).iterrows():
        row_str = " ".join([str(x) for x in row.values]).lower()
        if 'order id' in row_str or 'no. pesanan' in row_str or 'status pesanan' in row_str:
            header_idx = i
            break
            
    df_actual = df.iloc[header_idx:].copy()
    df_actual.columns = df_actual.iloc[0]
    df_actual = df_actual.iloc[1:].reset_index(drop=True)
    df_actual.columns = df_actual.columns.str.strip()
    return df_actual

# ==========================================
# CORE PROCESSING
# ==========================================
def process_data(uploaded_files, kamus_data):
    start_time = time.time()
    
    # 1. Persiapan Kamus (Sesuai Kolom Baru)
    df_kurir = kamus_data['kurir']
    df_bundle = kamus_data['bundle']
    df_sku = kamus_data['sku']

    # Mapping Bundle: Kit_Sku -> List of (Component_Sku, Component_Qty)
    bundle_dict = {}
    for _, r in df_bundle.iterrows():
        k_sku = clean_sku(r['Kit_Sku'])
        if k_sku not in bundle_dict: bundle_dict[k_sku] = []
        bundle_dict[k_sku].append((clean_sku(r['Component_Sku']), float(r['Component_Qty'])))
    
    # Mapping Nama: Product_Sku -> Product_Name
    name_dict = pd.Series(df_sku.Product_Name.values, index=df_sku.Product_Sku.apply(clean_sku)).to_dict()

    # Get List Kurir Instant Shopee
    instant_list = []
    if 'Instant/Same Day' in df_kurir.columns:
        instant_list = df_kurir[df_kurir['Instant/Same Day'].astype(str).str.upper().isin(['YES', 'YA', 'TRUE', '1'])]['Opsi Pengiriman'].tolist()

    all_expanded_rows = []

    # 2. Proses File Order
    for mp_type, file_obj in uploaded_files:
        try:
            df_actual = get_clean_df(file_obj)

            # --- LOGIC SHOPEE ---
            if mp_type == 'Shopee':
                # Filter: Perlu Dikirim, Not Managed, Kurir Instant, No Resi
                df_filtered = df_actual[
                    (df_actual['Status Pesanan'].str.strip() == 'Perlu Dikirim') &
                    (df_actual['Pesanan yang Dikelola Shopee'].str.strip().str.upper() == 'NO') &
                    (df_actual['Opsi Pengiriman'].isin(instant_list)) &
                    (df_actual['No. Resi'].isna() | (df_actual['No. Resi'].astype(str).str.strip() == ''))
                ].copy()
                sku_col, qty_col, order_id_col = 'Nomor Referensi SKU', 'Jumlah', 'No. Pesanan'

            # --- LOGIC TOKOPEDIA ---
            elif mp_type == 'Tokopedia':
                # Filter: Perlu dikirim saja (Sesuai Request)
                df_filtered = df_actual[df_actual['Order Status'].astype(str).str.strip().str.lower() == 'perlu dikirim'].copy()
                sku_col, qty_col, order_id_col = 'Seller SKU', 'Quantity', 'Order ID'

            if df_filtered.empty: continue

            # 3. Bundle Expansion
            for _, row in df_filtered.iterrows():
                sku_key = clean_sku(row[sku_col])
                qty_order = float(str(row[qty_col]).replace(',', '.'))
                
                if sku_key in bundle_dict:
                    # Expand Bundle
                    for comp_sku, comp_qty in bundle_dict[sku_key]:
                        all_expanded_rows.append({
                            'Marketplace': mp_type,
                            'Order ID': row[order_id_col],
                            'SKU Original': row[sku_col],
                            'Is Bundle?': 'Yes',
                            'SKU Component': comp_sku,
                            'Product Name': name_dict.get(comp_sku, comp_sku),
                            'Qty': qty_order * comp_qty
                        })
                else:
                    # Satuan
                    all_expanded_rows.append({
                        'Marketplace': mp_type,
                        'Order ID': row[order_id_col],
                        'SKU Original': row[sku_col],
                        'Is Bundle?': 'No',
                        'SKU Component': sku_key,
                        'Product Name': name_dict.get(sku_key, sku_key),
                        'Qty': qty_order
                    })

        except Exception as e:
            st.error(f"Gagal memproses {file_obj.name}: {e}")

    if not all_expanded_rows: return None

    df_final = pd.DataFrame(all_expanded_rows)
    df_summary = df_final.groupby(['Marketplace', 'SKU Component', 'Product Name']).agg({'Qty': 'sum'}).reset_index().sort_values('Qty', ascending=False)
    
    return {'detail': df_final, 'summary': df_summary}

# --- SIDEBAR ---
st.sidebar.header("📁 Step 1: Upload Kamus")
kamus_file = st.sidebar.file_uploader("Kamus Dashboard.xlsx", type=['xlsx'])

st.sidebar.header("📁 Step 2: Upload Order")
shp_f = st.sidebar.file_uploader("Data-Order (Shopee)", type=['csv', 'xlsx'])
tok_f = st.sidebar.file_uploader("OrderSKUList (Tokopedia)", type=['csv', 'xlsx'])

if st.sidebar.button("🚀 PROSES SEKARANG", type="primary", use_container_width=True):
    if not kamus_file:
        st.error("Upload Kamus Master dulu!")
    elif not shp_f and not tok_f:
        st.error("Upload minimal 1 file order!")
    else:
        try:
            # Load Kamus with new sheet names/structure
            excel = pd.ExcelFile(kamus_file)
            k_dict = {
                'kurir': pd.read_excel(excel, sheet_name='Kurir-Shopee'),
                'bundle': pd.read_excel(excel, sheet_name='Bundle Master'),
                'sku': pd.read_excel(excel, sheet_name='SKU Master')
            }
            
            files = []
            if shp_f: files.append(('Shopee', shp_f))
            if tok_f: files.append(('Tokopedia', tok_f))
            
            res = process_data(files, k_dict)
            if res:
                st.session_state.res = res
                st.success("Berhasil! Cek tab di bawah.")
            else:
                st.warning("Tidak ada data yang lolos filter.")
        except Exception as e:
            st.error(f"Error Kamus: {e}")

# --- DISPLAY ---
if 'res' in st.session_state:
    res = st.session_state.res
    tab1, tab2, tab3 = st.tabs(["📋 Picking List (Detail)", "📦 Stock Check (Ringkasan SKU)", "📥 Download"])
    
    with tab1: st.dataframe(res['detail'], use_container_width=True)
    with tab2: st.dataframe(res['summary'], use_container_width=True)
    with tab3:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            res['detail'].to_excel(writer, sheet_name='Picking List', index=False)
            res['summary'].to_excel(writer, sheet_name='Stock Check', index=False)
        st.download_button("📥 Download Excel Hasil", data=output.getvalue(), file_name=f"Hasil_Order_{datetime.now().strftime('%d%m_%H%M')}.xlsx")
