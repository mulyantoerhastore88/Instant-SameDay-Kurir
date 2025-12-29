import streamlit as st
import pandas as pd
import numpy as np
import io
from datetime import datetime
import plotly.express as px

# Page config
st.set_page_config(
    page_title="Instant & SameDay Processor",
    page_icon="🚚",
    layout="wide"
)

# Title
st.title("🚚 Instant & SameDay Order Processor")
st.markdown("Upload file order dan kamus, proses langsung di sini!")

# Session state
if 'processed' not in st.session_state:
    st.session_state.processed = False
if 'results' not in st.session_state:
    st.session_state.results = {}

# --- FUNGSI CLEANING (SAMA DENGAN MILIKMU) ---
def clean_sku(sku):
    """Konversi ke string, strip, dan ambil bagian kanan hyphen."""
    sku = str(sku).strip()
    if '-' in sku:
        return sku.split('-', 1)[-1].strip()
    return sku

def clean_df_strings(df):
    """Membersihkan semua kolom string dari spasi/karakter tak terlihat."""
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
    return df

# --- FUNGSI PROCESSING UTAMA ---
def process_data(df_orders, df_kurir, df_bundle, df_sku):
    """Logika processing utama"""
    
    # Cleaning data master
    df_bundle = clean_df_strings(df_bundle)
    df_sku = clean_df_strings(df_sku)
    
    # Clean SKU columns
    df_bundle['SKU Bundle'] = df_bundle['SKU Bundle'].apply(clean_sku)
    df_sku.rename(columns={'Material': 'SKU Component', 'Material description': 'Nama Produk Master'}, inplace=True)
    df_sku['SKU Component'] = df_sku['SKU Component'].apply(clean_sku)
    
    # Clean orders data
    df_orders.columns = df_orders.columns.astype(str).str.strip()
    df_orders['Status Pesanan'] = df_orders['Status Pesanan'].astype(str).str.strip()
    df_orders['Pesanan yang Dikelola Shopee'] = df_orders['Pesanan yang Dikelola Shopee'].astype(str).str.strip()
    df_orders['Opsi Pengiriman Key'] = df_orders['Opsi Pengiriman'].astype(str).str.strip()
    df_orders['No. Resi'] = df_orders['No. Resi'].astype(str).str.strip()
    df_orders['Is Resi Blank'] = (df_orders['No. Resi'] == '') | (df_orders['No. Resi'] == 'nan')
    
    # Clean kurir data
    df_kurir['Opsi Pengiriman'] = df_kurir['Opsi Pengiriman'].astype(str).str.strip()
    instant_same_day_options = df_kurir[
        df_kurir['Instant/Same Day'].astype(str).str.strip() == 'Yes'
    ]['Opsi Pengiriman'].unique()
    
    # Filter utama
    df_filtered = df_orders[
        (df_orders['Status Pesanan'] == 'Perlu Dikirim') &
        (df_orders['Pesanan yang Dikelola Shopee'] == 'No') &
        (df_orders['Opsi Pengiriman Key'].isin(instant_same_day_options)) &
        (df_orders['Is Resi Blank'] == True)
    ].copy()
    
    if df_filtered.empty:
        return {"error": "Tidak ada data yang memenuhi kriteria filter."}
    
    # Output 3: Order Summarize
    df_output3 = df_filtered.groupby('Opsi Pengiriman').agg(
        {'No. Pesanan': 'nunique'}
    ).reset_index()
    df_output3.rename(columns={'No. Pesanan': 'Total Order'}, inplace=True)
    
    # Clean SKU Awal
    df_filtered['SKU Awal'] = df_filtered['Nomor Referensi SKU'].fillna(
        df_filtered['SKU Induk']).fillna(df_filtered['Nama Produk']).apply(clean_sku)
    df_filtered.rename(columns={'Jumlah': 'Kuantitas Order'}, inplace=True)
    
    # Bundle Expansion
    sku_bundle_list = df_bundle['SKU Bundle'].unique()
    expanded_rows = []
    
    for index, row in df_filtered.iterrows():
        sku_awal_cleaned = row['SKU Awal']
        original_sku_raw = row['Nomor Referensi SKU']
        
        if sku_awal_cleaned in sku_bundle_list:
            # Bundle ditemukan
            bundle_components = df_bundle[df_bundle['SKU Bundle'] == sku_awal_cleaned]
            for _, comp_row in bundle_components.iterrows():
                expanded_rows.append({
                    'No. Pesanan': row['No. Pesanan'],
                    'Status Pesanan': row['Status Pesanan'],
                    'Opsi Pengiriman': row['Opsi Pengiriman'],
                    'Pesanan Harus Dikirimkan Sebelum': row.get('Pesanan Harus Dikirimkan Sebelum (Menghindari keterlambatan)', ''),
                    'Nomor Referensi SKU Original': original_sku_raw,
                    'Is Resi Blank': row['Is Resi Blank'],
                    'Is Bundle?': 'Yes',
                    'SKU Component': clean_sku(comp_row['SKU Component']),
                    'Jumlah Final': row['Kuantitas Order'] * comp_row['Component Quantity'],
                })
        else:
            # Item satuan
            expanded_rows.append({
                'No. Pesanan': row['No. Pesanan'],
                'Status Pesanan': row['Status Pesanan'],
                'Opsi Pengiriman': row['Opsi Pengiriman'],
                'Pesanan Harus Dikirimkan Sebelum': row.get('Pesanan Harus Dikirimkan Sebelum (Menghindari keterlambatan)', ''),
                'Nomor Referensi SKU Original': row['Nomor Referensi SKU'],
                'Is Resi Blank': row['Is Resi Blank'],
                'Is Bundle?': 'No',
                'SKU Component': sku_awal_cleaned,
                'Jumlah Final': row['Kuantitas Order'] * 1,
            })
    
    df_bundle_expanded = pd.DataFrame(expanded_rows)
    
    # Output 1: Detail Order
    df_output1 = df_bundle_expanded.rename(columns={
        'SKU Component': 'Nomor Referensi SKU (Component/Cleaned)',
        'Jumlah Final': 'Jumlah dikalikan Component Quantity (Grand Total)'
    })
    
    # Output 2: Grand Total per SKU
    df_output2 = df_bundle_expanded.groupby('SKU Component').agg(
        {'Jumlah Final': 'sum'}
    ).reset_index().rename(columns={
        'SKU Component': 'Nomor Referensi SKU (Cleaned)',
        'Jumlah Final': 'Jumlah (Grand total by SKU)'
    })
    
    return {
        'output1': df_output1,
        'output2': df_output2,
        'output3': df_output3,
        'filtered_count': len(df_filtered),
        'expanded_count': len(df_bundle_expanded)
    }

# --- SIDEBAR UPLOAD ---
with st.sidebar:
    st.header("📁 Upload Files")
    
    st.subheader("1. File Order")
    order_file = st.file_uploader(
        "Upload file order (CSV/Excel)",
        type=['csv', 'xlsx', 'xls'],
        key="order"
    )
    
    st.subheader("2. File Kamus")
    col1, col2 = st.columns(2)
    
    with col1:
        kurir_file = st.file_uploader(
            "Kurir-Shopee",
            type=['csv', 'xlsx'],
            key="kurir"
        )
        
    with col2:
        bundle_file = st.file_uploader(
            "Bundle Master",
            type=['csv', 'xlsx'],
            key="bundle"
        )
    
    sku_file = st.file_uploader(
        "SKU Master",
        type=['csv', 'xlsx'],
        key="sku"
    )
    
    st.divider()
    
    # Process button
    if st.button("🚀 Process Data", type="primary", use_container_width=True):
        if all([order_file, kurir_file, bundle_file, sku_file]):
            with st.spinner("Memproses data..."):
                try:
                    # Read uploaded files
                    if order_file.name.endswith('.csv'):
                        df_orders = pd.read_csv(order_file)
                    else:
                        df_orders = pd.read_excel(order_file)
                    
                    if kurir_file.name.endswith('.csv'):
                        df_kurir = pd.read_csv(kurir_file)
                    else:
                        df_kurir = pd.read_excel(kurir_file)
                    
                    if bundle_file.name.endswith('.csv'):
                        df_bundle = pd.read_csv(bundle_file)
                    else:
                        df_bundle = pd.read_excel(bundle_file)
                    
                    if sku_file.name.endswith('.csv'):
                        df_sku = pd.read_csv(sku_file)
                    else:
                        df_sku = pd.read_excel(sku_file)
                    
                    # Process data
                    results = process_data(df_orders, df_kurir, df_bundle, df_sku)
                    
                    if "error" in results:
                        st.error(results["error"])
                    else:
                        st.session_state.results = results
                        st.session_state.processed = True
                        st.success("✅ Data berhasil diproses!")
                        
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
        else:
            st.warning("⚠️ Silakan upload semua file terlebih dahulu!")
    
    st.divider()
    st.caption("Version 1.0 | Made with Streamlit")

# --- MAIN CONTENT ---
if st.session_state.processed:
    results = st.session_state.results
    
    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Orders Filtered", results['filtered_count'])
    with col2:
        st.metric("Total Items Expanded", results['expanded_count'])
    with col3:
        st.metric("Unique SKUs", len(results['output2']))
    with col4:
        total_orders = results['output3']['Total Order'].sum()
        st.metric("Total Orders", total_orders)
    
    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📋 Detail Order", 
        "📦 SKU Summary", 
        "📊 Statistics", 
        "💾 Download"
    ])
    
    with tab1:
        st.subheader("Detail Order (Expanded)")
        st.dataframe(
            results['output1'],
            use_container_width=True,
            hide_index=True,
            height=400
        )
        
        # Quick stats
        st.write(f"**Total Rows:** {len(results['output1'])}")
        st.write(f"**Bundle Items:** {results['output1']['Is Bundle?'].value_counts().get('Yes', 0)}")
        st.write(f"**Single Items:** {results['output1']['Is Bundle?'].value_counts().get('No', 0)}")
    
    with tab2:
        st.subheader("Grand Total by SKU")
        st.dataframe(
            results['output2'],
            use_container_width=True,
            hide_index=True
        )
        
        # Bar chart
        if len(results['output2']) > 0:
            fig = px.bar(
                results['output2'].head(20),
                x='Nomor Referensi SKU (Cleaned)',
                y='Jumlah (Grand total by SKU)',
                title="Top 20 SKUs by Quantity"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.subheader("Order Summary by Courier")
        st.dataframe(
            results['output3'],
            use_container_width=True,
            hide_index=True
        )
        
        # Pie chart
        if len(results['output3']) > 0:
            fig = px.pie(
                results['output3'],
                values='Total Order',
                names='Opsi Pengiriman',
                title="Distribution by Courier Service"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.subheader("Download Results")
        
        # Generate timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Download Output 1
            csv1 = results['output1'].to_csv(index=False)
            st.download_button(
                label="📥 Download Detail Order",
                data=csv1,
                file_name=f"detail_order_{timestamp}.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with col2:
            # Download Output 2
            csv2 = results['output2'].to_csv(index=False)
            st.download_button(
                label="📥 Download SKU Summary",
                data=csv2,
                file_name=f"sku_summary_{timestamp}.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with col3:
            # Download Output 3
            csv3 = results['output3'].to_csv(index=False)
            st.download_button(
                label="📥 Download Courier Summary",
                data=csv3,
                file_name=f"courier_summary_{timestamp}.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        # Download All as Excel
        st.divider()
        st.subheader("Download All in One Excel File")
        
        # Create Excel file with multiple sheets
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            results['output1'].to_excel(writer, sheet_name='Detail Order', index=False)
            results['output2'].to_excel(writer, sheet_name='SKU Summary', index=False)
            results['output3'].to_excel(writer, sheet_name='Courier Summary', index=False)
        
        st.download_button(
            label="📊 Download All (Excel)",
            data=output.getvalue(),
            file_name=f"instant_sameday_report_{timestamp}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

else:
    # Show instructions if no data processed
    st.info("""
    ## 📋 Instruksi Penggunaan:
    
    1. **Upload 4 file** di sidebar kiri:
       - File Order (dari Shopee/Marketplace)
       - File Kurir-Shopee (Kamus)
       - File Bundle Master (Kamus)  
       - File SKU Master (Kamus)
    
    2. Klik tombol **"Process Data"**
    
    3. Lihat hasil di dashboard
    
    4. Download hasil dalam format CSV atau Excel
    """)
    
    # Sample data structure
    with st.expander("📝 Contoh Struktur File yang Diperlukan"):
        st.write("""
        **File Order harus memiliki kolom:**
        - No. Pesanan
        - Status Pesanan
        - Pesanan yang Dikelola Shopee
        - Opsi Pengiriman
        - No. Resi
        - Nomor Referensi SKU
        - SKU Induk
        - Nama Produk
        - Jumlah
        - Pesanan Harus Dikirimkan Sebelum (Menghindari keterlambatan)
        """)

# Footer
st.divider()
st.caption("💡 Tips: File harus dalam format CSV atau Excel. Pastikan struktur kolom sesuai dengan contoh.")
