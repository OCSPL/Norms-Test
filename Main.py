from Utils.sql_queries import get_stock_query, get_job_work
from Utils.utils import multiply_with_percentage
from Products_Files.calculate_2_3_dichloro_pyridine import calculate_2_3_dichloro_pyridine
from Products_Files.calculate_nndi_iso_propyl_ethyl_amine import calculate_nndi_iso_propyl_ethyl_amine,calculate_dpi
from Products_Files.calculate_2_4_6_trimethyl_phenyl_acetyl_chlotide import calculate_2_4_6_trimethyl_phenyl_acetyl_chlotide, Calculate_246
from Products_Files.calculate_2_5_dimethyl_phenyl_acetyl_chloride import calculate_2_5_dimethyl_phenyl_acetyl_chloride,Calculate_25
from Products_Files.calculate_amido_chloride import calculate_amido_chloride
from Products_Files.calculate_metcamifen import calculate_metcamifen
from Products_Files.calculate_Spiro import calculate_Spiro
from Products_Files.Calculate_24dcbc import Calculate_24dcbc
from Products_Files.calculate_m2cp import calculate_m2cp
from Products_Files.calculate_26DCBC import calculate_26DCBC
from Products_Files.calculate_26DMBA import calculate_26DMBA
from Utils.Bom import fetch_bom_details
from Utils.FG_Names import fg_name_to_items
from Utils.FG_Name_Stage import fg_name_stage_mapping
import pandas as pd
from sqlalchemy import text
from Config import engine_norms, engine_eres

def process_data(start_date, end_date, fg_name):
    # Initialize variables
    tables = []
    output_df = pd.DataFrame()
    stage_details = []
    bom_summaries_df = pd.DataFrame(columns=['Name', 'Quantity', 'BOMQty', 'RM WIP QTY', 'Highlight', 'Stage Name']) 
    stock_summary = None
    batch_range = None
    final_output_quantity = None
    highlighted_items = []
   

    # Fetch data from Norms database
    query_bi_product = 'SELECT * FROM Bi_Production;'
    df_bi_product = pd.read_sql(query_bi_product, engine_norms)

    query_consumption = 'SELECT * FROM Con_Production;'
    df_consumption = pd.read_sql(query_consumption, engine_norms)

    query_out_product = 'SELECT * FROM Output_Production;'
    df_out_product = pd.read_sql(query_out_product, engine_norms)

    query_job_work2 = 'SELECT * FROM JobWork_Production;'
    df_job_work2 = pd.read_sql(query_job_work2, engine_norms)

    query_job_work = 'SELECT * FROM JobWork_Production;'
    job_work_df = pd.read_sql(query_job_work, engine_norms)

    # # Fetch data and load into a pandas DataFrame
    # with engine_eres.connect() as connection:
    #     query = text(get_job_work())
    #     job_work_df = pd.read_sql(query, connection)

    # Convert columns to date format
    df_bi_product['BiProduct_Voucher_Date'] = pd.to_datetime(df_bi_product['BiProduct_Voucher_Date'], dayfirst=True)
    df_consumption['Consume_Voucher_Date'] = pd.to_datetime(df_consumption['Consume_Voucher_Date'], dayfirst=True)
    df_out_product['Output_Voucher_Date'] = pd.to_datetime(df_out_product['Output_Voucher_Date'], dayfirst=True)
    job_work_df['Consume_Voucher_Date'] = pd.to_datetime(job_work_df['Consume_Voucher_Date'], format='%d/%m/%Y')
    df_job_work2['Output_Voucher_Date'] = pd.to_datetime(df_job_work2['Output_Voucher_Date'], dayfirst=True)  

    # Preprocessing steps
    df_out_product_reduced = df_out_product[['Output_Batch_No', 'Output_Item_Name', 'Output_Item_Type']]
    df_out_product_unique = df_out_product_reduced.drop_duplicates(subset='Output_Batch_No')

    merged_df = pd.merge(
        df_consumption, 
        df_out_product_unique,
        on='Output_Batch_No',
        how='left'
    )

    # Replacement dictionary
    replacement_dict = {
        '2,3 DI CHLORO PYRIDINE (M)': '2,3 DI CHLORO PYRIDINE',
        'DIPEA SFG': 'N,N DI ISO PROPYL ETHYL AMINE',
        'AMIDO CHLORIDE (M)': 'AMIDO CHLORIDE sam',
       
    }
    replacement_dict2 = {
        'AMIDO CHLORIDE SFG':'AMIDO CHLORIDE sam'
    }

    # Replace values in columns
    merged_df['Output_Item_Name'] = merged_df['Output_Item_Name'].replace(replacement_dict2)
    merged_df['Consume_Item_Name'] = merged_df['Consume_Item_Name'].replace(replacement_dict)

    # Apply condition on Consume_Quantity
    merged_df['Consume Quantity'] = merged_df.apply(
        lambda row: row['Consume_Quantity'] - row['Consume_Quantity'] if row['Consume_Item_Name'] == row['Output_Item_Name'] else row['Consume_Quantity'], axis=1
    )

    # Filter DataFrame
    merged_df = merged_df[
        (
            (merged_df['Output_Item_Type'] == 'Finished Good') & 
            (merged_df['Consume_Item_Name'] == '2,3 DI CHLORO PYRIDINE')
        ) |
        (
            (merged_df['Output_Item_Type'] == 'Semi Finished Good') & 
            (merged_df['Consume_Item_Name'] != '2,3 DI CHLORO PYRIDINE')
        )
    ]

    # Add Same_QTY column
    merged_df['Same_QTY'] = merged_df.apply(
        lambda row: row['Consume_Quantity'] if row['Consume_Item_Name'] == row['Output_Item_Name'] and (
            (row['Output_Item_Type'] == 'Finished Good' and row['Consume_Item_Name'] == '2,3 DI CHLORO PYRIDINE') or 
            (row['Output_Item_Type'] == 'Semi Finished Good' and row['Consume_Item_Name'] != '2,3 DI CHLORO PYRIDINE')
        ) else 0, axis=1
    )
    # merged_df.to_csv('merged_df2.csv')

    # Rename 'FG_Name_CON' to 'FG_Name' in df_consumption
    merged_df2 = df_consumption.copy()
    merged_df2.rename(columns={'FG_Name_CON': 'FG_Name'}, inplace=True)

    # Read the activation data
    activation_data = pd.read_csv('Activation.csv', encoding='ISO-8859-1')

    # Now process the data with the given start_date and end_date
    if start_date and end_date:
        # Convert start and end dates to datetime format
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        # Convert start and end dates to integer format for SQL query
        lFromDate = int(start_date.strftime('%Y%m%d'))
        lToDate = int(end_date.strftime('%Y%m%d'))

        # Filter data based on date range
        filtered_out_product = df_out_product[(df_out_product['Output_Voucher_Date'] >= start_date) & (df_out_product['Output_Voucher_Date'] <= end_date) & (df_out_product['FG_Name'] == fg_name)]
        # filtered_out_product.to_csv('filtered_out_product.csv')
        filtered_bi_product = df_bi_product[(df_bi_product['BiProduct_Voucher_Date'] >= start_date) & (df_bi_product['BiProduct_Voucher_Date'] <= end_date)]
        filtered_consumption = merged_df2[(merged_df2['Consume_Voucher_Date'] >= start_date) & (merged_df2['Consume_Voucher_Date'] <= end_date) & (merged_df2['FG_Name'] == fg_name)]
        # filtered_consumption.to_csv('filtered_consumption.csv')
        # job_work_df.to_csv('job_work_df.csv')
        job_work_df = job_work_df[(job_work_df['Consume_Voucher_Date'] >= start_date) & (job_work_df['Consume_Voucher_Date'] <= end_date) & (job_work_df['FG_Name'] == fg_name) & (job_work_df['Output_Quantity'] != 0)]
        # job_work_df.to_csv('job_work_df.csv')
        df_job_work2 = df_job_work2[(df_job_work2['Output_Voucher_Date'] >= start_date) & (df_job_work2['Output_Voucher_Date'] <= end_date) & (df_job_work2['FG_Name'] == fg_name) & (df_job_work2['Output_Quantity'] != 0)]    

        # Fetch stock data
        sql_query = get_stock_query(lFromDate, lToDate)
        with engine_eres.connect() as connection:
            Stock_df = pd.read_sql_query(sql_query, connection)

        # Group by Output_Item_Name and sum the Output_Quantity
        grouped_df = job_work_df.groupby('Output_Item_Name')['Output_Quantity'].sum().reset_index()   

        # Process stock data
        if fg_name in fg_name_to_items:
            item_mapping = fg_name_to_items[fg_name]
            item_names = list(item_mapping.keys())

            filtered_stock_df = Stock_df[Stock_df['Item Name'].isin(item_names)]

            # Calculate stock summary
            stock_summary = filtered_stock_df.groupby('Item Name').agg(
                Opening=('Opening', 'sum')
            ).reset_index()

            # Ensure all stages are included
            for item_name, item_details in item_mapping.items():
                if item_name not in stock_summary['Item Name'].values:
                    stock_summary = pd.concat([stock_summary, pd.DataFrame([{'Item Name': item_name, 'Opening': 0}])], ignore_index=True)

            # Add WIP PRODUCED column
            stock_summary['WIP PRODUCED'] = stock_summary['Item Name'].apply(lambda item: (
                (
                    filtered_bi_product[filtered_bi_product['BiProduct_Item_Name'] == item]['BiProduct_Quantity'].sum() if 'bi_product' in item_mapping[item]['source'] else 0
                ) + 
                (
                    filtered_out_product[filtered_out_product['Output_Item_Name'] == item]['Output_Quantity'].sum() if 'out_product' in item_mapping[item]['source'] else 0
                ) +
                (
                    df_job_work2[df_job_work2['Output_Item_Name'] == item]['Output_Quantity'].sum() if 'out_product' in item_mapping[item]['source'] else 0
                )
            ))
            

            # Add WIP CONSUMED IN PRODUCTION column
            stock_summary['WIP CONSUMED IN PRODUCTION'] = stock_summary['Item Name'].apply(lambda item:
             (
                filtered_consumption[filtered_consumption['Consume_Item_Name'] == item]['Consume_Quantity'].sum()
             ) +
             (
                job_work_df[job_work_df['Consume_Item_Name'] == item]['Consume_Quantity'].sum()
             )
            
            )

            # Calculate Closing
            stock_summary['Closing'] = stock_summary['Opening'] + stock_summary['WIP PRODUCED'] - stock_summary['WIP CONSUMED IN PRODUCTION']

            # Add Quantities Consumed from Opening Stock
            stock_summary['Quantities Consumed from Opening Stock'] = stock_summary['Opening'].apply(lambda x: -abs(x) if x > 0 else abs(x))

            # Add ADDITIONAL QTY CONSUMED IN OTHER WIP
            stock_summary['ADDITIONAL QTY CONSUMED IN OTHER WIP'] = 0  # Adjust if necessary

            # Add Stage column
            stock_summary['Stage'] = stock_summary['Item Name'].apply(lambda item: item_mapping[item]['stage'])

            # Merge with activation data
            stock_summary = stock_summary.merge(activation_data, on='Item Name', how='left', suffixes=('', '_activation'))

            # Convert columns to numeric
            stock_summary['Closing'] = pd.to_numeric(stock_summary['Closing'], errors='coerce').fillna(0)
            stock_summary['Quantities Consumed from Opening Stock'] = pd.to_numeric(stock_summary['Quantities Consumed from Opening Stock'], errors='coerce').fillna(0)
            stock_summary['ADDITIONAL QTY CONSUMED IN OTHER WIP'] = pd.to_numeric(stock_summary['ADDITIONAL QTY CONSUMED IN OTHER WIP'], errors='coerce').fillna(0)

            # Calculate NET QTY
            net_qtys = []
            for _, row in stock_summary.iterrows():
                net_qty = multiply_with_percentage(row['Closing'], row['Closing_activation'])
                net_qty += multiply_with_percentage(row['Quantities Consumed from Opening Stock'], row['Quantities Consumed from Opening Stock_activation'])
                net_qty += multiply_with_percentage(row['ADDITIONAL QTY CONSUMED IN OTHER WIP'], row['ADDITIONAL QTY CONSUMED IN OTHER WIP_activation'])
                net_qtys.append(round(net_qty, 2))
            stock_summary['NET QTY'] = net_qtys

            # Calculate intermediate multiplication results
            stock_summary['Closing_mult'] = stock_summary.apply(lambda row: round(multiply_with_percentage(row['Closing'], row['Closing_activation']), 2), axis=1)
            stock_summary['Quantities_Consumed_mult'] = stock_summary.apply(lambda row: round(multiply_with_percentage(row['Quantities Consumed from Opening Stock'], row['Quantities Consumed from Opening Stock_activation']), 2), axis=1)
            stock_summary['Additional_QTY_mult'] = stock_summary.apply(lambda row: round(multiply_with_percentage(row['ADDITIONAL QTY CONSUMED IN OTHER WIP'], row['ADDITIONAL QTY CONSUMED IN OTHER WIP_activation']), 2), axis=1)

            # Fetch BOM details if required
            for item_name, item_details in item_mapping.items():
                if item_details.get('Bom'):
                    bom_summary = fetch_bom_details(item_name, stock_summary, engine_eres, fg_name)
                    bom_summary['Stage Name'] = item_name
                    bom_summary = bom_summary[['Name', 'Quantity', 'BOMQty', 'RM WIP QTY', 'Highlight', 'Stage Name']]
                    bom_summaries_df = pd.concat([bom_summaries_df, bom_summary], ignore_index=True)

            # Round numerical columns
            bom_summaries_df = bom_summaries_df.round(2)

            # Rearrange stock summary columns
            stock_summary = stock_summary[['Item Name', 'Stage', 'Opening', 'WIP PRODUCED', 'WIP CONSUMED IN PRODUCTION', 'Closing', 'Quantities Consumed from Opening Stock', 'ADDITIONAL QTY CONSUMED IN OTHER WIP', 'Closing_activation', 'Quantities Consumed from Opening Stock_activation', 'ADDITIONAL QTY CONSUMED IN OTHER WIP_activation', 'Closing_mult', 'Quantities_Consumed_mult', 'Additional_QTY_mult', 'NET QTY']]
            stock_summary = stock_summary.sort_values(by='Stage')
            stock_summary = stock_summary.round(2)

            # Specific calculations based on fg_name
            stage_3_net_qty, stage_4_net_qty = (0, 0)
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

            # Group BOM summaries
            final_bom_summary = bom_summaries_df.groupby('Name').agg({'RM WIP QTY': 'sum'}).reset_index()

            # Filter consumption data
            if fg_name == '2,3 DI CHLORO PYRIDINE':
                Con_df = merged_df[(merged_df['Consume_Voucher_Date'] >= start_date) & (merged_df['Consume_Voucher_Date'] <= end_date)]
            else:
                Con_df = merged_df2[(merged_df2['Consume_Voucher_Date'] >= start_date) & (merged_df2['Consume_Voucher_Date'] <= end_date)]

            # Items to remove
            items_to_remove = [
                'METCAMIFEN STAGE IV SAM-IV SFG OFF SPEC',
                'METCAMIFEN STAGE IV SAM-IV SFG SPEC'
            ]
            Con_df = Con_df[~Con_df['Consume_Item_Name'].isin(items_to_remove)]

            # Apply filtering condition
            sfg = ['AMIDO CHLORIDE', 'METCAMIFEN TECH.', '4-HYDROXY-3-(2,4,6-TRIMETHYLPHENYL)-1-OXASPIRO[4.4]NON-3-EN-2-ONE']
            if fg_name in sfg:
                Con_df = Con_df[Con_df['Consume_Item_Type'].isin(['Key Raw Material', 'Raw Material', 'Semi Finished Good'])]
            else:
                Con_df = Con_df[Con_df['Consume_Item_Type'].isin(['Key Raw Material', 'Raw Material'])]

            if fg_name == '2,3 DI CHLORO PYRIDINE':
                Con_df = Con_df[Con_df['FG_Name_CON'] == fg_name]
            else:
                Con_df = Con_df[Con_df['FG_Name'] == fg_name]

            # Group consumption data
            Con_qty = Con_df.groupby('Consume_Item_Name').agg(
                Net_Qty=('Consume_Quantity', 'sum'),
                Rate=('Consume_Value', 'sum'),
                Value2=('Consume_Value', 'sum')
            ).reset_index()
            Con_qty = Con_qty[Con_qty['Net_Qty'] != 0]
            Con_qty['Rate'] = (Con_qty['Rate'] / Con_qty['Net_Qty']).round(2)

            # Calculate output quantity
            # df_out_product.to_csv('output_quantity_df.csv')
            if fg_name:
                output_quantity_df = df_out_product[(df_out_product['FG_Name'] == fg_name) &
                                                    (df_out_product['Output_Voucher_Date'] >= start_date) &
                                                    (df_out_product['Output_Voucher_Date'] <= end_date)]
                # output_quantity_df.to_csv('output_quantity_df.csv')                                   
                if fg_name == '2,3 DI CHLORO PYRIDINE':
                    output_quantity_df = output_quantity_df[output_quantity_df['Output_Item_Type'] == 'Finished Good']
                else:
                    output_quantity_df = output_quantity_df[output_quantity_df['Output_Item_Type'] == 'Semi Finished Good']
                
                dcat_qty = pd.to_numeric(filtered_bi_product[filtered_bi_product['BiProduct_Item_Name'] == 'DCAT SFG']['BiProduct_Quantity'], errors='coerce').sum()
                if fg_name == 'DICHLORO ACETIC ACID':
                    output_quantity = output_quantity_df['Output_Quantity'].sum() + dcat_qty
                else:
                    output_quantity = output_quantity_df['Output_Quantity'].sum()
                    # output_quantity_df.to_csv('output_quantity_df.csv')

                output_quantity_df['Batch_No'] = output_quantity_df['Output_Batch_No'].str[-3:].astype(int)
                min_batch_no = output_quantity_df['Batch_No'].min()
                max_batch_no = output_quantity_df['Batch_No'].max()
                batch_range = f"Batch {min_batch_no} to {max_batch_no}"

                MQTY_df = merged_df[(merged_df['Consume_Voucher_Date'] >= start_date) & (merged_df['Consume_Voucher_Date'] <= end_date)]
                MQTY = MQTY_df.groupby('FG_Name_CON')['Same_QTY'].sum().reset_index()
                MQTY = MQTY[MQTY['Same_QTY'] != 0]
                same_qty = MQTY[MQTY['FG_Name_CON'] == fg_name]['Same_QTY'].sum()

              
                

                # Initialize final_output_quantity
                final_output_quantity = output_quantity - same_qty

                if fg_name in fg_name_stage_mapping:
                    mapping = fg_name_stage_mapping[fg_name]
                    highlighted_items = [item_info['item_name'] for item_info in mapping.get('items', [])]

                    # Special handling for '2,3 DI CHLORO PYRIDINE'
                    if fg_name == '2,3 DI CHLORO PYRIDINE':
                        for adj in mapping.get('item_name', []):
                            quantity_var = adj['quantity_var']
                            quantity = locals().get(quantity_var, 0)
                            final_output_quantity += quantity
                            stage_details.append({'stage': adj['stage'], 'quantity': quantity})
                    else:
                        for item_info in mapping.get('items', []):
                            item_name = item_info['item_name']
                            stage = item_info['stage']
                            if item_name in stock_summary['Item Name'].values:
                                quantity = stock_summary.loc[stock_summary['Item Name'] == item_name, 'NET QTY'].values[0]
                                final_output_quantity += quantity
                                stage_details.append({'stage': stage, 'quantity': quantity})
                else:
                    final_output_quantity = output_quantity - same_qty


                # Construct output DataFrame
                output_data = {
                    'Output Quantity': [output_quantity],
                    'Same QTY': [-same_qty],
                }

                for stage_detail in stage_details:
                    output_data[stage_detail['stage']] = [stage_detail['quantity']]

                final_output_quantity = round(final_output_quantity, 2)
                output_data['Final Output QTY'] = [final_output_quantity]
                output_df = pd.DataFrame(output_data)

            # Merge with BOM summary
            if fg_name == '2,6 DICHLORO BENZOYL CHLORIDE':
                Con_qty = calculate_26DCBC(Con_qty,job_work_df)
            if final_bom_summary is not None:
                Con_qty
                Con_qty = Con_qty.merge(final_bom_summary[['Name', 'RM WIP QTY']], left_on='Consume_Item_Name', right_on='Name', how='left')
                Con_qty['WIP-RM'] = Con_qty['RM WIP QTY'].fillna(0)
                Con_qty = Con_qty.drop(columns=['Name', 'RM WIP QTY'])

            if 'WIP-RM' not in Con_qty.columns:
                Con_qty['WIP-RM'] = 0

            # Additional logic based on fg_name
            if fg_name == 'AMIDO CHLORIDE':
                net_qty_amido_recovered = stock_summary.loc[stock_summary['Item Name'] == 'AMIDO RECOVERED IPAC INTERCUT-1', 'NET QTY'].values[0]
                Con_qty.loc[Con_qty['Consume_Item_Name'] == 'ISO PROPYL ACETATE (M)', 'WIP-RM'] = net_qty_amido_recovered   
            elif fg_name == '2,4,6 TRIMETHYL PHENYL ACETYL CHLORIDE':
                Con_qty = Calculate_246(Con_qty, stock_summary)
            elif fg_name == '2,5 DIMETHYL PHENYL ACETYL CHLORIDE':
                Con_qty = Calculate_25(Con_qty, stock_summary)
            elif fg_name == '2,4 DICHLORO BENZOYL CHLORIDE':
                Con_qty = Calculate_24dcbc(Con_qty, job_work_df,stock_summary)
            elif fg_name == 'N,N DI ISO PROPYL ETHYL AMINE':
                Con_qty = calculate_dpi(Con_qty,stock_summary)       

            # Calculate QTY and Norms
            Con_qty['QTY'] = Con_qty['Net_Qty'].astype(float) - Con_qty['WIP-RM'].astype(float)
            exclude_items = ['2,3 DI CHLORO PYRIDINE', 'DICHLORO ACETYL CHLORIDE (M)', 'AMIDO CHLORIDE SFG', 'SPIR SFG','AMIDO CHLORIDE (M)','METHYL-2-CHLORO PROPIONATE (M)']
            Con_qty = Con_qty[~Con_qty['Consume_Item_Name'].isin(exclude_items)]
            Con_qty['Norms'] = (Con_qty['QTY'] / final_output_quantity)
            Con_qty['Value'] = (Con_qty['Norms'] * Con_qty['Rate']).round(2)
            Con_qty = Con_qty.sort_values(by='Value', ascending=False)

            # Calculate total value
            total_value = Con_qty['Value'].sum()
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

            # Reorder and format columns
            Con_qty = Con_qty[['Consume_Item_Name', 'Net_Qty', 'WIP-RM', 'QTY','Norms', 'Rate', 'Value']]
            Con_qty = Con_qty.rename(columns={'Net_Qty': 'Temp_QTY', 'QTY': 'Net_Qty'})
            Con_qty = Con_qty.rename(columns={'Temp_QTY': 'QTY'})

            for col in ['Net_Qty', 'WIP-RM', 'QTY', 'Norms', 'Rate', 'Value']:
                Con_qty[col] = Con_qty[col].apply(lambda x: f"{float(x):,.2f}" if x != '' else x)
                
            # Round numerical columns to 2 decimal places
            if not Con_qty.empty:
                # Exclude non-numeric columns from rounding
                numeric_cols = Con_qty.select_dtypes(include=['float64', 'int64']).columns
                Con_qty[numeric_cols] = Con_qty[numeric_cols].round(2)

            if stock_summary is not None and not stock_summary.empty:
                numeric_cols = stock_summary.select_dtypes(include=['float64', 'int64']).columns
                stock_summary[numeric_cols] = stock_summary[numeric_cols].round(2)

            if bom_summaries_df is not None and not bom_summaries_df.empty:
                numeric_cols = bom_summaries_df.select_dtypes(include=['float64', 'int64']).columns
                bom_summaries_df[numeric_cols] = bom_summaries_df[numeric_cols].round(2)

            # Prepare data to return
            data = {
                'Con_qty': Con_qty,
                'output_df': output_df.to_dict(orient='records'),
                'batch_range': batch_range,
                'stock_summary': stock_summary.to_dict(orient='records') if stock_summary is not None else None,
                'bom_summaries_df': bom_summaries_df.to_dict(orient='records') if not bom_summaries_df.empty else None,
                'fg_name_to_items': fg_name_to_items,
                'highlighted_items': highlighted_items
            }

            return data
    else:
        # If dates are not provided
        data = {
            'Con_qty': pd.DataFrame(),
            'output_df': [],
            'batch_range': None,
            'stock_summary': None,
            'bom_summaries_df': None,
            'fg_name_to_items': fg_name_to_items
        }
        return data