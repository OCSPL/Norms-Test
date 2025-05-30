import numpy as np
import pandas as pd
import time
import concurrent.futures
from datetime import datetime
from sqlalchemy import text
from Utils.sql_queries import get_stock_query, get_job_work2,get_job_work, get_sys_output, get_sys_con, get_sys_bipro, get_bom_query
from Utils.utils import multiply_with_percentage
from Products_Files.calculate_2_3_dichloro_pyridine import calculate_2_3_dichloro_pyridine, calculate_23dcp
from Products_Files.calculate_nndi_iso_propyl_ethyl_amine import calculate_nndi_iso_propyl_ethyl_amine, calculate_dpi
from Products_Files.calculate_2_4_6_trimethyl_phenyl_acetyl_chlotide import calculate_2_4_6_trimethyl_phenyl_acetyl_chlotide, Calculate_246
from Products_Files.calculate_2_5_dimethyl_phenyl_acetyl_chloride import calculate_2_5_dimethyl_phenyl_acetyl_chloride, Calculate_25
from Products_Files.calculate_amido_chloride import calculate_amido_chloride
from Products_Files.calculate_metcamifen import calculate_metcamifen, Calculate_met
from Products_Files.calculate_Spiro import calculate_Spiro,calculate_sipro_con
from Products_Files.Calculate_24dcbc import Calculate_24dcbc
from Products_Files.calculate_m2cp import calculate_m2cp
from Products_Files.calculate_26DCBC import calculate_26DCBC
from Products_Files.calculate_ocdb import calculate_ocdb
from Products_Files.calculate_26DMBA import calculate_26DMBA,Calculate_26
from Products_Files.calculate_DCAT import calculate_DCAT, calculate_con_dcat
from Products_Files.calculate_pick1 import calculate_pick1
from Products_Files.calculate_dmpm import calculate_dmpm , calculate_dmpm_con
from Products_Files.calculate_tpa import calculate_tpa, calculate_con_tpa
from Products_Files.calculate_4ftmp import calculate_4ftmp
from Products_Files.calculate_24dcbd import calculate_24dcbd
from Utils.Bom import fetch_bom_details
from Utils.FG_Names import fg_name_to_items
from Utils.FG_Name_Stage import fg_name_stage_mapping
from Config import engine_norms, engine_eres

# -------------------- Helper Functions for Parameterized SQL Queries --------------------

def get_sys_output_filtered(from_date, to_date, fg_name):
    """
    Convert from_date/to_date to YYYYMMDD (integer),
    then bind them to the sys_output query.
    """
    from_date_int = int(pd.to_datetime(from_date).strftime('%Y%m%d'))
    to_date_int   = int(pd.to_datetime(to_date).strftime('%Y%m%d'))
    query         = get_sys_output(from_date_int, to_date_int, fg_name)
    return query.bindparams(from_date=from_date_int, to_date=to_date_int, fg_name=fg_name)


def get_sys_con_filtered(from_date, to_date, fg_name):
    """
    Convert from_date/to_date to YYYYMMDD (integer),
    then bind them to the sys_con query.
    """
    from_date_int = int(pd.to_datetime(from_date).strftime('%Y%m%d'))
    to_date_int   = int(pd.to_datetime(to_date).strftime('%Y%m%d'))
    base_query    = get_sys_con(from_date_int, to_date_int, fg_name)
    return base_query.bindparams(from_date=from_date_int, to_date=to_date_int, fg_name=fg_name)


def get_sys_bipro_filtered(from_date, to_date, fg_name):
    """
    Convert from_date/to_date to YYYYMMDD (integer),
    then bind them to the sys_bipro query.
    """
    from_date_int = int(pd.to_datetime(from_date).strftime('%Y%m%d'))
    to_date_int   = int(pd.to_datetime(to_date).strftime('%Y%m%d'))
    base_query    = get_sys_bipro(from_date_int, to_date_int, fg_name)
    return base_query.bindparams(from_date=from_date_int, to_date=to_date_int, fg_name=fg_name)

# def get_jobwork_filtered(from_date, to_date, fg_name):
#     """
#     Convert from_date/to_date to YYYYMMDD (integer),
#     then bind them to the sys_bipro query.
#     """
#     from_date_int = int(pd.to_datetime(from_date).strftime('%Y%m%d'))
#     to_date_int   = int(pd.to_datetime(to_date).strftime('%Y%m%d'))
#     base_query    = get_job_work2()
#     return base_query.bindparams()


def get_output_df_and_max_date(from_date, to_date, fg_name):
    """
    Convert from_date/to_date to YYYYMMDD (integer),
    then run the sys_output query to get a DataFrame.
    """
    from_date_int = int(pd.to_datetime(from_date).strftime('%Y%m%d'))
    to_date_int   = int(pd.to_datetime(to_date).strftime('%Y%m%d'))

    query = get_sys_output_filtered(from_date_int, to_date_int, fg_name)
    df = pd.read_sql(
        query,
        engine_eres,
        params={'from_date': from_date_int, 'to_date': to_date_int, 'fg_name': fg_name}
    )
    # Convert 'Output_Voucher_Date' from dd-mm-yyyy to a proper datetime
    df['Output_Voucher_Date'] = pd.to_datetime(df['Output_Voucher_Date'], dayfirst=True)
    max_date = df['Output_Voucher_Date'].max()
    return df, max_date


def get_output_df_and_max_date_default():
    """
    Default function that uses '2024-04-01' as the fiscal date and
    today's date as the 'to_date'. Also uses the first FG from fg_names.
    """
    max_date=pd.read_sql(get_maxdate(), engine_eres)
    return  max_date

# -------------------- Main Process Data Function --------------------

def process_data(start_date, end_date, fg_name):
    # Initialize variables and a dictionary to hold section timings.
    timings = {}
    total_start = time.time()
    
    tables = []
    output_df = pd.DataFrame()
    stage_details = []
    bom_summaries_df = pd.DataFrame(columns=['Name', 'Quantity', 'BOMQty', 'RM WIP QTY', 'Highlight', 'Stage Name'])
    stock_summary = None
    batch_range = None
    final_output_quantity = None
    highlighted_items = []
    
    # --- Optimized Section: Fetch data concurrently from Norms database ---
    section_start = time.time()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_bi_product = executor.submit(pd.read_sql, get_sys_bipro_filtered(start_date, end_date, fg_name), engine_eres)
        future_consumption = executor.submit(pd.read_sql, get_sys_con_filtered(start_date, end_date,fg_name), engine_eres)
        future_out_product = executor.submit(pd.read_sql, get_sys_output_filtered(start_date, end_date, fg_name), engine_eres)
        future_job_work = executor.submit(pd.read_sql, get_job_work(), engine_eres)
        future_job_work2 = executor.submit(pd.read_sql, get_job_work2(), engine_eres)
    df_bi_product = future_bi_product.result()
    df_consumption = future_consumption.result()
    df_out_product = future_out_product.result()
    job_work_df = future_job_work.result()
    # Use a copy for job_work2
    df_job_work2 = future_job_work2.result()
   
    timings["fetch_data"] = time.time() - section_start

    # --- Section: Date conversion and consumption name fix ---
    section_start = time.time()
    df_bi_product['BiProduct_Voucher_Date'] = pd.to_datetime(df_bi_product['BiProduct_Voucher_Date'],format='%d %b %Y', dayfirst=True, errors='coerce')
    df_consumption['Consume_Voucher_Date'] = pd.to_datetime( df_consumption['Consume_Voucher_Date'].astype(str), format='%Y%m%d' )
    df_out_product['Output_Voucher_Date'] = pd.to_datetime(df_out_product['Output_Voucher_Date'], format='%Y-%m-%d',errors='coerce')
    job_work_df['Consume_Voucher_Date'] = pd.to_datetime(job_work_df['Consume_Voucher_Date'], format='%d/%m/%Y')    
    df_job_work2['Output_Voucher_Date'] = pd.to_datetime(df_job_work2['Output_Voucher_Date'], dayfirst=True)

    df_consumption['Consume_Item_Name'] = (
        df_consumption['Consume_Item_Name']
        .str.replace(r'\bTANKER\b', 'M', regex=True)
        .str.replace(r'( \(M\))+', ' (M)', regex=True)
    )

    timings["date_conversion"] = time.time() - section_start

    # --- Section: Preprocessing: merge and replacement (vectorized) ---
    section_start = time.time()
    df_out_product_reduced = df_out_product[['Output_Batch_No', 'Output_Item_Name', 'Output_Item_Type']]
    df_out_product_unique = df_out_product_reduced.drop_duplicates(subset='Output_Batch_No')
    merged_df = pd.merge(df_consumption, df_out_product_unique, on='Output_Batch_No', how='left')
    replacement_dict = {
        '2,3 DI CHLORO PYRIDINE (M)': '2,3 DI CHLORO PYRIDINE',
        'DIPEA SFG': 'N,N DI ISO PROPYL ETHYL AMINE',
        'AMIDO CHLORIDE (M)': 'AMIDO CHLORIDE sam',
        '2,6 DICHLORO BENZOYL CHLORIDE (M)':'2,6 DCBC SFG',
    }
    replacement_dict2 = {'AMIDO CHLORO SFG': 'AMIDO CHLORIDE sam'}
    merged_df['Output_Item_Name'] = merged_df['Output_Item_Name'].replace(replacement_dict2)
    merged_df['Consume_Item_Name'] = merged_df['Consume_Item_Name'].replace(replacement_dict)
    merged_df['Consume Quantity'] = np.where(
        merged_df['Consume_Item_Name'] == merged_df['Output_Item_Name'],
        0,
        merged_df['Consume_Quantity']
    )

    cond = (merged_df['Consume_Item_Name'] == merged_df['Output_Item_Name']) & (
           ((merged_df['Output_Item_Type'] == 'Finished Good') & (merged_df['Consume_Item_Name'] == '2,3 DI CHLORO PYRIDINE')) |
           ((merged_df['Output_Item_Type'] == 'Semi Finished Good') & (merged_df['Consume_Item_Name'] != '2,3 DI CHLORO PYRIDINE'))
    )
    
    merged_df['Same_QTY'] = np.where(cond, merged_df['Consume_Quantity'], 0)
    
    merged_df2 = df_consumption.copy()
    merged_df2.rename(columns={'FG_Name': 'FG_Name'}, inplace=True)
    timings["preprocessing_merge"] = time.time() - section_start

    # --- Section: Read activation data ---
    section_start = time.time()
    activation_data = pd.read_csv('Activation.csv', encoding='ISO-8859-1')
    timings["read_activation"] = time.time() - section_start

    if start_date and end_date:
        # --- Optimized Section: Date filtering using Boolean masks with detailed timing ---
        date_filtering_timings = {}
        section_start = time.time()
        
        # Convert dates and compute integer date bounds
        t0 = time.time()
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        lFromDate = int(start_date.strftime('%Y%m%d'))
        lToDate = int(end_date.strftime('%Y%m%d'))
        date_filtering_timings['date_conversion'] = time.time() - t0
        
        # Filter Output DataFrame (df_out_product)
        t1 = time.time()
        mask_out = (df_out_product['Output_Voucher_Date'] >= start_date) & \
                   (df_out_product['Output_Voucher_Date'] <= end_date) & \
                   (df_out_product['FG_Name'] == fg_name)
        filtered_out_product = df_out_product.loc[mask_out]
        filtered_out_product.to_csv('filtered_out_product.csv')
     
        date_filtering_timings['filtered_out_product'] = time.time() - t1
        
        # Filter BiProduct DataFrame (df_bi_product)
        t2 = time.time()
        mask_bi = (df_bi_product['BiProduct_Voucher_Date'] >= start_date) & \
                  (df_bi_product['BiProduct_Voucher_Date'] <= end_date)
        filtered_bi_product = df_bi_product.loc[mask_bi]
        date_filtering_timings['filtered_bi_product'] = time.time() - t2
        
        # Filter Consumption DataFrame (merged_df2)
        t3 = time.time()
        mask_cons = (merged_df2['Consume_Voucher_Date'] >= start_date) & \
                    (merged_df2['Consume_Voucher_Date'] <= end_date) & \
                    (merged_df2['FG_Name'] == fg_name)
        filtered_consumption = merged_df2.loc[mask_cons]
        filtered_consumption.to_csv('filtered_consumption.csv')
        date_filtering_timings['filtered_consumption'] = time.time() - t3
        
        # Filter Job Work DataFrame (job_work_df)
        t4 = time.time()
        mask_job = (job_work_df['Consume_Voucher_Date'] >= start_date) & \
                   (job_work_df['Consume_Voucher_Date'] <= end_date) & \
                   (job_work_df['FG_name'] == fg_name) 
        job_work_df_filtered = job_work_df.loc[mask_job]
       
        
        
        
        date_filtering_timings['job_work_filtered'] = time.time() - t4
        
        # Filter Second Job Work DataFrame (df_job_work2)
        t5 = time.time()
        mask_job2 = (df_job_work2['Output_Voucher_Date'] >= start_date) & \
                    (df_job_work2['Output_Voucher_Date'] <= end_date) & \
                    (df_job_work2['FG_name'] == fg_name) & \
                    (df_job_work2['Output_Quantity'] != 0)
        df_job_work2_filtered = df_job_work2.loc[mask_job2]
       
        date_filtering_timings['job_work2_filtered'] = time.time() - t5
        
        # Query Stock Data from Database
        t6 = time.time()
        with engine_eres.connect() as connection:
            Stock_df = pd.read_sql_query(get_stock_query(lFromDate, lToDate,fg_name), connection)
            # Stock_df.to_csv('stock.csv')
        date_filtering_timings['stock_query'] = time.time() - t6
        
        # Group Job Work Data
        t7 = time.time()
        # grouped_df = job_work_df_filtered.groupby('Output_Item_Name')['Output_Quantity'].sum().reset_index()
        date_filtering_timings['grouped_job_work'] = time.time() - t7
        
        total_date_filtering_time = time.time() - section_start
        timings["date_filtering"] = total_date_filtering_time
        
        # Log detailed date filtering timings to a file
        with open("date_filtering_timings.txt", "a") as f:
            f.write(f"{datetime.now()} - Date Filtering Section Timings:\n")
            for key, t in date_filtering_timings.items():
                f.write(f"    {key}: {t:.4f} seconds\n")
            f.write(f"Total date_filtering time: {total_date_filtering_time:.4f} seconds\n\n")

        # --- Section: Process stock data (Optimized using vectorized lookups) ---
        section_start = time.time()
        if fg_name in fg_name_to_items:
            item_mapping = fg_name_to_items[fg_name]
            item_names = list(item_mapping.keys())
            stock_summary = pd.DataFrame({'Item Name': item_names})
            openings = Stock_df[Stock_df['Item Name'].isin(item_names)].groupby('Item Name')['Opening'].sum()
            stock_summary = stock_summary.merge(openings, on='Item Name', how='left')
            stock_summary['Opening'] = stock_summary['Opening'].fillna(0)
            bi_sum = filtered_bi_product.groupby('BiProduct_Item_Name')['BiProduct_Quantity'].sum().to_dict()
            out_sum = filtered_out_product.groupby('Output_Item_Name')['Output_Quantity'].sum().to_dict()           
            job_sum = df_job_work2_filtered.groupby('Output_Item_Name')['Output_Quantity'].sum().to_dict()
            

            stock_summary['WIP PRODUCED'] = stock_summary['Item Name'].apply(
                lambda item: (bi_sum.get(item, 0) if 'bi_product' in item_mapping[item]['source'] else 0) +
                             (out_sum.get(item, 0) if 'out_product' in item_mapping[item]['source'] else 0)+
                             (job_sum.get(item, 0) if 'out_product' in item_mapping[item]['source'] else 0)
            )
            
            cons_sum = filtered_consumption.groupby('Consume_Item_Name')['Consume_Quantity'].sum().to_dict()
            job_cons_sum = job_work_df_filtered.groupby('Consume_Item_Name')['Consume_Quantity'].sum().to_dict()
            
            stock_summary['WIP CONSUMED IN PRODUCTION'] = stock_summary['Item Name'].apply(
                lambda item: cons_sum.get(item, 0) + job_cons_sum.get(item, 0)
            )
            stock_summary['Closing'] = stock_summary['Opening'] + stock_summary['WIP PRODUCED'] - stock_summary['WIP CONSUMED IN PRODUCTION']
            stock_summary['Quantities Consumed from Opening Stock'] = stock_summary['Opening'].apply(lambda x: -abs(x) if x > 0 else abs(x))
            stock_summary['ADDITIONAL QTY CONSUMED IN OTHER WIP'] = 0
            stock_summary['Stage'] = stock_summary['Item Name'].map(lambda x: item_mapping[x]['stage'])
            stock_summary = stock_summary.merge(activation_data, on='Item Name', how='left', suffixes=('', '_activation'))
            stock_summary['Closing'] = pd.to_numeric(stock_summary['Closing'], errors='coerce').fillna(0)
            stock_summary['Quantities Consumed from Opening Stock'] = pd.to_numeric(stock_summary['Quantities Consumed from Opening Stock'], errors='coerce').fillna(0)
            stock_summary['ADDITIONAL QTY CONSUMED IN OTHER WIP'] = pd.to_numeric(stock_summary['ADDITIONAL QTY CONSUMED IN OTHER WIP'], errors='coerce').fillna(0)
            net_qtys = []
            for _, row in stock_summary.iterrows():
                net = multiply_with_percentage(row['Closing'], row['Closing_activation'])
                net += multiply_with_percentage(row['Quantities Consumed from Opening Stock'], row['Quantities Consumed from Opening Stock_activation'])
                net += multiply_with_percentage(row['ADDITIONAL QTY CONSUMED IN OTHER WIP'], row['ADDITIONAL QTY CONSUMED IN OTHER WIP_activation'])
                net_qtys.append(round(net, 2))
            stock_summary['NET QTY'] = net_qtys
            stock_summary['Closing_mult'] = stock_summary.apply(lambda row: round(multiply_with_percentage(row['Closing'], row['Closing_activation']), 2), axis=1)
            stock_summary['Quantities_Consumed_mult'] = stock_summary.apply(lambda row: round(multiply_with_percentage(row['Quantities Consumed from Opening Stock'], row['Quantities Consumed from Opening Stock_activation']), 2), axis=1)
            stock_summary['Additional_QTY_mult'] = stock_summary.apply(lambda row: round(multiply_with_percentage(row['ADDITIONAL QTY CONSUMED IN OTHER WIP'], row['ADDITIONAL QTY CONSUMED IN OTHER WIP_activation']), 2), axis=1)
            for item_name, item_details in item_mapping.items():
                if item_details.get('Bom'):
                    bom_summary = fetch_bom_details(item_name, stock_summary, engine_eres, fg_name)
                    bom_summary['Stage Name'] = item_name
                    bom_summary = bom_summary[['Name', 'Quantity', 'BOMQty', 'RM WIP QTY', 'Highlight', 'Stage Name']]
                    bom_summaries_df = pd.concat([bom_summaries_df, bom_summary], ignore_index=True)
            bom_summaries_df = bom_summaries_df.round(2)
            stock_summary = stock_summary[['Item Name', 'Stage', 'Opening', 'WIP PRODUCED', 'WIP CONSUMED IN PRODUCTION',
                                           'Closing', 'Quantities Consumed from Opening Stock', 'ADDITIONAL QTY CONSUMED IN OTHER WIP',
                                           'Closing_activation', 'Quantities Consumed from Opening Stock_activation',
                                           'ADDITIONAL QTY CONSUMED IN OTHER WIP_activation', 'Closing_mult',
                                           'Quantities_Consumed_mult', 'Additional_QTY_mult', 'NET QTY']]
            stock_summary = stock_summary.sort_values(by='Stage').round(2)
            timings["stock_processing"] = time.time() - section_start
        else:
            stock_summary = pd.DataFrame()

        # --- Section: Specific calculations based on fg_name ---
        stock_summary.to_csv('stock_summary1234.csv')
        section_start = time.time()
        stage_details = []
        if fg_name == '2,3 DI CHLORO PYRIDINE':
            stage_3_net_qty, stage_4_net_qty = calculate_2_3_dichloro_pyridine(stock_summary)
        elif fg_name == 'N,N DI ISO PROPYL ETHYL AMINE':
            stock_summary = calculate_nndi_iso_propyl_ethyl_amine(stock_summary)
        elif fg_name == '2,4,6 TRIMETHYL PHENYL ACETYL CHLORIDE':
            stock_summary, bom_summaries_df = calculate_2_4_6_trimethyl_phenyl_acetyl_chlotide(stock_summary, bom_summaries_df)
        elif fg_name == '2,5 DIMETHYL PHENYL ACETYL CHLORIDE':
            stock_summary, bom_summaries_df = calculate_2_5_dimethyl_phenyl_acetyl_chloride(stock_summary, bom_summaries_df)
        elif fg_name == 'AMIDO CHLORIDE':
            stock_summary = calculate_amido_chloride(stock_summary, job_work_df)
        elif fg_name == 'METCAMIFEN TECH.':
            stock_summary, bom_summaries_df = calculate_metcamifen(stock_summary, bom_summaries_df)
        elif fg_name == '4-HYDROXY-3-(2,4,6-TRIMETHYLPHENYL)-1-OXASPIRO[4.4]NON-3-EN-2-ONE':
            stock_summary, bom_summaries_df = calculate_Spiro(stock_summary, bom_summaries_df)
        elif fg_name == 'METHYL-2-CHLORO PROPIONATE':
            stock_summary, bom_summaries_df = calculate_m2cp(stock_summary, bom_summaries_df)
        elif fg_name == '2,6 DIMETHOXY BENZOIC ACID':
            stock_summary, bom_summaries_df = calculate_26DMBA(stock_summary, bom_summaries_df)
        elif fg_name == 'DICHLORO ACETIC ACID':
            stock_summary, bom_summaries_df = calculate_DCAT(stock_summary, bom_summaries_df)
        elif fg_name == '2-CHLORO-4-FLUORO-5-NITROPHENYL ETHYL CARBONATE':
            stock_summary, bom_summaries_df = calculate_pick1(stock_summary, bom_summaries_df)
        elif fg_name == '1,2 DIMETHYL PROPYL AMINE':
            stock_summary, bom_summaries_df = calculate_dmpm(stock_summary, bom_summaries_df)
        elif fg_name == 'TPA':
            stock_summary, bom_summaries_df = calculate_tpa(stock_summary, bom_summaries_df)
        timings["specific_calculations"] = time.time() - section_start

        # --- Section: Group BOM summaries ---
        section_start = time.time()
        final_bom_summary = bom_summaries_df.groupby('Name').agg({'RM WIP QTY': 'sum'}).reset_index()
        timings["bom_grouping"] = time.time() - section_start

        # --- Optimized Section: Filter consumption data using boolean masks ---
        section_start = time.time()
        if fg_name == '2,3 DI CHLORO PYRIDINE':
            mask_cons_df = (merged_df['Consume_Voucher_Date'] >= start_date) & (merged_df['Consume_Voucher_Date'] <= end_date)
            Con_df = merged_df.loc[mask_cons_df]
        else:
            mask_cons_df2 = (merged_df2['Consume_Voucher_Date'] >= start_date) & (merged_df2['Consume_Voucher_Date'] <= end_date)
            Con_df = merged_df2.loc[mask_cons_df2]
        timings["consumption_filter"] = time.time() - section_start

        # --- Section: Adjust consumption filter and replacement ---
        section_start = time.time()
        items_to_remove = ['METCAMIFEN STAGE IV SAM-IV SFG OFF SPEC', 'METCAMIFEN STAGE IV SAM-IV SFG SPEC']
        Con_df = Con_df[~Con_df['Consume_Item_Name'].isin(items_to_remove)]
        sfg = ['4-HYDROXY-3-(2,4,6-TRIMETHYLPHENYL)-1-OXASPIRO[4.4]NON-3-EN-2-ONE']
        FG = ['METCAMIFEN TECH.', 'AMIDO CHLORIDE']
        if fg_name in sfg:
            Con_df = Con_df[Con_df['Consume_Item_Type'].isin(['Key Raw Material', 'Raw Material', 'Semi Finished Good'])]
        elif fg_name in FG:
            Con_df = Con_df[Con_df['Consume_Item_Type'].isin(['Key Raw Material', 'Raw Material', 'Semi Finished Good', 'Finished Good'])]
        else:
            Con_df = Con_df[Con_df['Consume_Item_Type'].isin(['Key Raw Material', 'Raw Material'])]
        if fg_name == '2,3 DI CHLORO PYRIDINE':
            Con_df = Con_df[Con_df['FG_Name'] == fg_name]
        elif fg_name == 'METCAMIFEN TECH.':
            Con_df = Con_df[Con_df['FG_Name'] == fg_name]
            Con_df['Consume_Item_Name'] = Con_df['Consume_Item_Name'].replace('2-METHOXY BENZOIC ACID', '2-MBA SFG')
        elif fg_name == 'AMIDO CHLORIDE':
            Con_df = Con_df[Con_df['FG_Name'] == fg_name]
            Con_df['Consume_Item_Name'] = Con_df['Consume_Item_Name'].replace('2-METHOXY BENZOIC ACID', '2-MBA SFG')
        elif fg_name == 'DICHLORO ACETIC ACID':
            Con_df = Con_df[Con_df['FG_Name'] == fg_name]
            Con_df['Consume_Item_Name'] = Con_df['Consume_Item_Name'].replace('CHLORAL ANHYDROUS STABILIZED 99% M (M)', 'CHLORAL ANHYDROUS STABILIZED 99% (M)')
        else:
            Con_df = Con_df[Con_df['FG_Name'] == fg_name]
        
        timings["consumption_filter_adjust"] = time.time() - section_start

        # --- Section: Group consumption data ---
        section_start = time.time()
        
        Con_qty = Con_df.groupby('Consume_Item_Name').agg(
            Net_Qty=('Consume_Quantity', 'sum'),
            Rate=('Consume_Value', 'sum'),
            Value2=('Consume_Value', 'sum')
        ).reset_index()
       
        Con_qty = Con_qty[Con_qty['Net_Qty'] != 0]
        Con_qty['Rate'] = (Con_qty['Rate'] / Con_qty['Net_Qty']).round(2)
        # Con_qty.to_csv('Con_qty.csv', index=False)
        
        # if fg_name:
        #     filtered_job_work_df = job_work_df.query("FG_name == @fg_name")          
        #     if not filtered_job_work_df.empty:
        #         grouped_job_work_df = filtered_job_work_df.groupby('Consume_Item_Name').agg(
        #             Net_Qty_jw=('Consume_Quantity', 'sum'),
        #             Value2_jw=('Consume_Value', 'sum')
        #         ).reset_index()
        #         Con_qty = pd.merge(Con_qty, grouped_job_work_df, on='Consume_Item_Name', how='outer')
        #         Con_qty['Net_Qty'] = Con_qty['Net_Qty'].fillna(0) + Con_qty['Net_Qty_jw'].fillna(0)
        #         Con_qty['Value2'] = Con_qty['Value2'].fillna(0) + Con_qty['Value2_jw'].fillna(0)
        #         Con_qty['Rate'] = (Con_qty['Value2'] / Con_qty['Net_Qty']).round(2)
        #         Con_qty.drop(columns=['Net_Qty_jw', 'Value2_jw'], inplace=True)
        
        # timings["consumption_grouping"] = time.time() - section_start

        # --- Section: Calculate output quantity ---
        section_start = time.time()
        if fg_name:
            output_quantity_df = df_out_product.query(
                "FG_Name == @fg_name and Output_Voucher_Date >= @start_date and Output_Voucher_Date <= @end_date"
            )
            special_fgs = ['2,3 DI CHLORO PYRIDINE', '2,4 DICHLORO BENZALDEHYDE']

            if fg_name.strip() in special_fgs:
                output_quantity_df = output_quantity_df.query(
                    "Output_Item_Type == 'Finished Good'"
                )
            else:
                output_quantity_df = output_quantity_df.query(
                    "Output_Item_Type == 'Semi Finished Good'"
                )

            dcat_qty = pd.to_numeric(filtered_bi_product.query("BiProduct_Item_Name == 'DCAT SFG'")['BiProduct_Quantity'], errors='coerce').sum()
            if fg_name == 'DICHLORO ACETIC ACID':
                output_quantity = output_quantity_df['Output_Quantity'].sum() + dcat_qty
            else:
                output_quantity = output_quantity_df['Output_Quantity'].sum()
            Total_batch = output_quantity_df['Output_Batch_No'].nunique()
            batch_range = f"No of Batch {Total_batch}"
            MQTY_df = merged_df.query("Consume_Voucher_Date >= @start_date and Consume_Voucher_Date <= @end_date")
            MQTY = MQTY_df.groupby('FG_Name')['Same_QTY'].sum().reset_index()
            MQTY = MQTY[MQTY['Same_QTY'] != 0]
            same_qty = MQTY.query("FG_Name == @fg_name")['Same_QTY'].sum()
            if fg_name == '2,3 DI CHLORO PYRIDINE':
                final_output_quantity = output_quantity
                same_qty = 0
            elif fg_name == 'METHYL-2-CHLORO PROPIONATE':
                M2cpqtyt = df_job_work2_filtered.loc[df_job_work2_filtered['Output_Item_Name'] == 'M2CP SFG', 'Output_Quantity'].sum()
                final_output_quantity = output_quantity - same_qty + M2cpqtyt
            else:
                final_output_quantity = output_quantity - same_qty
            if fg_name in fg_name_stage_mapping:
                mapping = fg_name_stage_mapping[fg_name]
                highlighted_items = [item_info['item_name'] for item_info in mapping.get('items', [])]
                for item_info in mapping.get('items', []):
                    item_name = item_info['item_name']
                    stage = item_info['stage']
                    if item_name in stock_summary['Item Name'].values:
                        quantity = stock_summary.loc[stock_summary['Item Name'] == item_name, 'NET QTY'].values[0]
                        final_output_quantity += quantity
                        stage_details.append({'stage': stage, 'quantity': quantity})
            else:
                final_output_quantity = output_quantity - same_qty
            output_data = {'Output Quantity': [output_quantity], 'Same QTY': [-same_qty]}
            for stage_detail in stage_details:
                output_data[stage_detail['stage']] = [stage_detail['quantity']]
            final_output_quantity = round(final_output_quantity, 2)
            output_data['Final Output QTY'] = [final_output_quantity]
            output_df = pd.DataFrame(output_data)
        timings["output_calculation"] = time.time() - section_start

        # --- Section: Merge with BOM summary ---
      
        section_start = time.time()
        if fg_name == '2,6 DICHLORO BENZOYL CHLORIDE':
            job_work_df= job_work_df_filtered.query("FG_name == @fg_name")

            Con_qty = calculate_26DCBC(Con_qty, job_work_df,fg_name)
        if fg_name == '4-FLUORO-3-TRIFLUOROMETHYL PHENOL':
            Con_qty = calculate_4ftmp(Con_qty, job_work_df_filtered)
        if fg_name == '2,4 DICHLORO BENZALDEHYDE':
            Con_qty = calculate_24dcbd(Con_qty, job_work_df_filtered)
        if fg_name == '1,2 DIMETHYL PROPYL AMINE':            
            Con_qty = calculate_dmpm_con(Con_qty, job_work_df_filtered)
            
        if final_bom_summary is not None:
            
            Con_qty = Con_qty.merge(final_bom_summary[['Name', 'RM WIP QTY']], left_on='Consume_Item_Name', right_on='Name', how='left')
            Con_qty['WIP-RM'] = Con_qty['RM WIP QTY'].fillna(0)
            Con_qty = Con_qty.drop(columns=['Name', 'RM WIP QTY'])
        if 'WIP-RM' not in Con_qty.columns:
            Con_qty['WIP-RM'] = 0
        timings["bom_merge"] = time.time() - section_start

        # --- Section: Additional logic based on fg_name ---
        section_start = time.time()
        if fg_name == 'AMIDO CHLORIDE':
            net_qty_amido_recovered = stock_summary.loc[stock_summary['Item Name'] == 'AMIDO RECOVERED IPAC INTERCUT-1', 'NET QTY'].values[0]
            Con_qty.loc[Con_qty['Consume_Item_Name'] == 'ISO PROPYL ACETATE (M)', 'WIP-RM'] = net_qty_amido_recovered   
        elif fg_name == '2,4,6 TRIMETHYL PHENYL ACETYL CHLORIDE':
            Con_qty = Calculate_246(Con_qty, stock_summary)
        elif fg_name == '2,5 DIMETHYL PHENYL ACETYL CHLORIDE':
            Con_qty = Calculate_25(Con_qty, stock_summary)
        elif fg_name == '2,4 DICHLORO BENZOYL CHLORIDE':
            Con_qty = Calculate_24dcbc(Con_qty, job_work_df_filtered, stock_summary)
        elif fg_name == 'N,N DI ISO PROPYL ETHYL AMINE':
            Con_qty = calculate_dpi(Con_qty, stock_summary)
        elif fg_name == 'METCAMIFEN TECH.':
            Con_qty = Calculate_met(Con_qty, bom_summaries_df)
        elif fg_name == '2,3 DI CHLORO PYRIDINE':
            Con_qty = calculate_23dcp(Con_qty, stock_summary)
        elif fg_name == 'DICHLORO ACETIC ACID':
            Con_qty = calculate_con_dcat(Con_qty, stock_summary)
        elif fg_name == 'TPA':
            Con_qty = calculate_con_tpa(Con_qty, stock_summary)
        elif fg_name == '2,6 DIMETHOXY BENZOIC ACID':
            Con_qty = Calculate_26(Con_qty, stock_summary,job_work_df_filtered)
        elif fg_name == '4-HYDROXY-3-(2,4,6-TRIMETHYLPHENYL)-1-OXASPIRO[4.4]NON-3-EN-2-ONE':
            Con_qty = calculate_sipro_con(Con_qty, stock_summary)
        elif fg_name == 'DMA-CHLORIDE LAN':
            Con_qty = calculate_ocdb(Con_qty, stock_summary)
        # elif fg_name == '1,2 DIMETHYL PROPYL AMINE':
        #     Con_qty = calculate_dmpm(stock_summary, job_work_df_filtered)

            
            
        timings["additional_logic"] = time.time() - section_start

        # --- Section: Calculate QTY and Norms ---
        section_start = time.time()
        Con_qty['QTY'] = Con_qty['Net_Qty'].astype(float) - Con_qty['WIP-RM'].astype(float)
        exclude_items = ['DICHLORO ACETIC ACID (M)', 'METCAMIFEN SFG', '2,3 DI CHLORO PYRIDINE',
                          'AMIDO CHLORIDE SFG', 'SPIR SFG',
                         'AMIDO CHLORIDE (M)', 'METHYL-2-CHLORO PROPIONATE (M)','2,6 DICHLORO BENZOYL CHLORIDE (M)']
        Con_qty = Con_qty[~Con_qty['Consume_Item_Name'].isin(exclude_items)]
        Con_qty['Norms'] = (Con_qty['QTY'] / final_output_quantity)
        Con_qty['Value'] = (Con_qty['Norms'] * Con_qty['Rate']).round(2)
        Con_qty = Con_qty.sort_values(by='Value', ascending=False)
        total_value = Con_qty['Value'].sum()
        # Con_qty.to_csv('result.csv')
        total_row = pd.DataFrame({
            'Consume_Item_Name': ['Total'],
            'Net_Qty': [''],
            'WIP-RM': [''],
            'QTY': [''],
            'Norms': [''],
            'Rate': [''],
            'Value': [total_value]
        })
        Con_qty = pd.concat([Con_qty, total_row], ignore_index=True)
        Con_qty = Con_qty[['Consume_Item_Name', 'Net_Qty', 'WIP-RM', 'QTY', 'Norms', 'Rate', 'Value']]
        Con_qty = Con_qty.rename(columns={'Net_Qty': 'Temp_QTY', 'QTY': 'Net_Qty'})
        Con_qty = Con_qty.rename(columns={'Temp_QTY': 'QTY'})
        for col in ['Net_Qty', 'WIP-RM', 'QTY', 'Rate', 'Value']:
            Con_qty[col] = Con_qty[col].apply(lambda x: f"{float(x):,.2f}" if x != '' else x)
        Con_qty['Norms'] = Con_qty['Norms'].apply(lambda x: f"{float(x):,.3f}" if x != '' else x)
        if not Con_qty.empty:
            numeric_cols = Con_qty.select_dtypes(include=['float64', 'int64']).columns
            Con_qty[numeric_cols] = Con_qty[numeric_cols].round(2)
        if stock_summary is not None and not stock_summary.empty:
            numeric_cols = stock_summary.select_dtypes(include=['float64', 'int64']).columns
            stock_summary[numeric_cols] = stock_summary[numeric_cols].round(2)
        if bom_summaries_df is not None and not bom_summaries_df.empty:
            numeric_cols = bom_summaries_df.select_dtypes(include=['float64', 'int64']).columns
            bom_summaries_df[numeric_cols] = bom_summaries_df[numeric_cols].round(2)
        STD_Norms = pd.read_excel("STD Norms File.xlsx")
        STD_Norms = STD_Norms[STD_Norms["Product Name"] == fg_name]
        Con_qty = Con_qty.merge(STD_Norms[['Consume_Item_Name', 'STD Norms']], on='Consume_Item_Name', how='left')
        Con_qty['STD Norms'] = Con_qty['STD Norms'].fillna('')
        Con_qty = Con_qty[['Consume_Item_Name', 'QTY', 'WIP-RM', 'Net_Qty', 'Norms', 'STD Norms', 'Rate', 'Value']]
        timings["qty_norms"] = time.time() - section_start

        # --- Section: Final timing and logging ---
        total_time = time.time() - total_start
        timings["total"] = total_time
        target_sections = {k: v for k, v in timings.items() if k != "total"}
        max_section = max(target_sections, key=target_sections.get)
        max_time = target_sections[max_section]
        with open("timing_log.txt", "a") as f:
            f.write(f"{datetime.now()} - Section timings:\n")
            for section, t in timings.items():
                f.write(f"    {section}: {t:.4f} seconds\n")
            f.write(f"Target section (excluding total): {max_section} took {max_time:.4f} seconds\n")
            f.write(f"Overall total: {total_time:.4f} seconds\n\n")

        data = {
            'Con_qty': Con_qty,
            'output_df': output_df.to_dict(orient='records'),
            'batch_range': batch_range,
            'stock_summary': stock_summary.to_dict(orient='records') if stock_summary is not None else None,
            'bom_summaries_df': bom_summaries_df.to_dict(orient='records') if not bom_summaries_df.empty else None,
            'fg_name_to_items': fg_name_to_items,
            'highlighted_items': highlighted_items,
            'timings': timings
        }
        return data
    else:
        data = {
            'Con_qty': pd.DataFrame(),
            'output_df': [],
            'batch_range': None,
            'stock_summary': None,
            'bom_summaries_df': None,
            'fg_name_to_items': fg_name_to_items
        }
        return data
