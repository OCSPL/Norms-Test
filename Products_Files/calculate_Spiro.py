import pandas as pd
from Utils.utils import multiply_with_percentage
import numpy as np

def update_bom_summaries_with_net_qty(stock_summary, bom_summaries_df, stage_name):
    # Sum the NET QTY values from the different sources (name_filters)
    net_qty_value = sum(
        stock_summary.loc[
            stock_summary['Item Name']== stage_name,'NET QTY'].values)

    # Update the RM WIP QTY in bom_summaries_df with the summed NET QTY value
    bom_summaries_df.loc[
        (bom_summaries_df['Stage Name'] == stage_name) &
        (bom_summaries_df['Name'] == stage_name),
        'RM WIP QTY'
    ] = net_qty_value

    # Extract and convert the Quantity to a numeric value for the specific item from bom_summaries_df
    Quantity_uniq = pd.to_numeric(bom_summaries_df.loc[
        (bom_summaries_df['Stage Name'] == stage_name) &
        (bom_summaries_df['Name'] == stage_name),
        'Quantity'
    ].values[0])

    # Update the RM WIP QTY in bom_summaries_df where Highlight is False
    bom_summaries_df.loc[
        (bom_summaries_df['Stage Name'] == stage_name) &
        (bom_summaries_df['Highlight'] == False), 
        'RM WIP QTY'
    ] = bom_summaries_df.apply(
        lambda row: (row['BOMQty'] / Quantity_uniq) * net_qty_value 
        if row['Highlight'] == False else row['RM WIP QTY'], 
        axis=1
    )
    
    return bom_summaries_df

def calculate_Spiro(stock_summary, bom_summaries_df):
    # Define the stages and corresponding name filters to process
    stages = [
        ('SPIR STAGE-I', ['SPIR (STAGE-II) WET CAKE']),
    ]
    
    for stage_name, name_filters in stages:
        # Sum RM WIP QTY for the given name_filters, with special case handling
        additional_qty_consumed = bom_summaries_df.loc[
            (bom_summaries_df['Stage Name'].isin(name_filters)) &
            (bom_summaries_df['Name'] == stage_name),
            'RM WIP QTY'
        ].sum()

        # Update ADDITIONAL QTY CONSUMED IN OTHER WIP
        stock_summary.loc[
            stock_summary['Item Name'] == stage_name, 
            'ADDITIONAL QTY CONSUMED IN OTHER WIP'
        ] = additional_qty_consumed

        # Calculate Additional_QTY_mult
        stock_summary['Additional_QTY_mult'] = stock_summary.apply(
            lambda row: round(
                multiply_with_percentage(row['ADDITIONAL QTY CONSUMED IN OTHER WIP'], 
                                         row['ADDITIONAL QTY CONSUMED IN OTHER WIP_activation']), 
                2
            ), 
            axis=1
        )
        
        # Calculate NET QTY for each row in stock_summary
        net_qtys = []
        for _, row in stock_summary.iterrows():
            net_qty = multiply_with_percentage(row['Closing'], row['Closing_activation'])
            net_qty += multiply_with_percentage(row['Quantities Consumed from Opening Stock'], row['Quantities Consumed from Opening Stock_activation'])
            net_qty += multiply_with_percentage(row['ADDITIONAL QTY CONSUMED IN OTHER WIP'], row['ADDITIONAL QTY CONSUMED IN OTHER WIP_activation'])
            net_qtys.append(round(net_qty, 2))
        
        stock_summary['NET QTY'] = net_qtys

        # Update bom_summaries_df with the new RM WIP QTY calculations where Highlight is False
        bom_summaries_df = update_bom_summaries_with_net_qty(stock_summary, bom_summaries_df, stage_name)

    return stock_summary, bom_summaries_df

def calculate_sipro_con(Con_qty, stock_summary,job_work_df_filtered):

        # Filter the stock_summary DataFrame for '2,4,6 RECOVERED MESITYLENE'
        DMA_row_1 = stock_summary[stock_summary['Item Name'] == 'SPIR DISTILLED DI METHYL ACETAMIDE']
        DMA_row_2 = stock_summary[stock_summary['Item Name'] == 'SPIR RECOVERED DI METHYL ACETAMIDE']
        Tol_row_1 = stock_summary[stock_summary['Item Name'] == 'SPIR RECOVERED TOLUENE']

        # Extract the 'NET QTY' value and store it in a variable
        mesitylene_net_qty = (DMA_row_1['NET QTY'].values[0] if not DMA_row_1.empty else None)+(DMA_row_2['NET QTY'].values[0] if not DMA_row_2.empty else None)
       
        # Filter the Con_qty DataFrame for 'MESITYLENE (M)'
        mesitylene_con_qty_row = Con_qty[Con_qty['Consume_Item_Name'] == 'DI METHYL ACETAMIDE (M)']

        # Add the mesitylene_net_qty value to 'WIP-RM' in the filtered row
        if not mesitylene_con_qty_row.empty:
            Con_qty.loc[mesitylene_con_qty_row.index, 'WIP-RM'] += mesitylene_net_qty
        
        # Extract the 'NET QTY' value and store it in a variable
        Tol_net_qty = (Tol_row_1['NET QTY'].values[0] if not Tol_row_1.empty else None)
        # Filter the Con_qty DataFrame for 'MESITYLENE (M)'
        Tol_con_qty_row = Con_qty[Con_qty['Consume_Item_Name'] == 'TOLUENE (M)']

        # Add the mesitylene_net_qty value to 'WIP-RM' in the filtered row
        if not Tol_con_qty_row.empty:
            Con_qty.loc[Tol_con_qty_row.index, 'WIP-RM'] += Tol_net_qty

        # 1) filter to only Key Raw Material & Raw Material
        df_filtered = job_work_df_filtered[
            job_work_df_filtered['Consume_Item_Type'].isin([
                'Key Raw Material',
                'Raw Material','Semi Finished Good'
            ])
        ].copy()

        # 2) group by item name, summing quantity & value
        grouped = (
            df_filtered
            .groupby('Consume_Item_Name', as_index=False)
            .agg(
                Consume_Quantity=('Consume_Quantity', 'sum'),
                Consume_Value   =('Consume_Value',    'sum')
            )
        )
        

        # 3) full outer join with Con_qty
        merged = pd.merge(
            Con_qty,
            grouped,
            on='Consume_Item_Name',
            how='outer'
        )
       

        # 4) coerce any stringy numbers into floats
        numeric_cols = ['Net_Qty', 'Value2', 'Consume_Quantity', 'Consume_Value', 'Rate']
        for col in numeric_cols:
            if col in merged.columns:
                merged[col] = pd.to_numeric(merged[col], errors='coerce').astype(float)

        # 5) fill NaNs for arithmetic
        merged['Net_Qty'] = merged['Net_Qty'].fillna(0) + merged['Consume_Quantity'].fillna(0)
        merged['Value2']  = merged['Value2'].fillna(0)  + merged['Consume_Value'].fillna(0)

        # 6) fill any blank WIP-RM as zero
        merged['WIP-RM'] = merged.get('WIP-RM', 0)

        # 7) recompute Rate
        merged['Rate'] = merged['Value2'] / merged['Net_Qty'].replace({0: np.nan})

        # 8) drop helper cols and return
        result = merged.drop(columns=['Consume_Quantity', 'Consume_Value'])
        numeric_cols = ['Net_Qty', 'Rate', 'Value2', 'WIP-RM'] 
        numeric_cols = [c for c in numeric_cols if c in result.columns]
        # Treat empty strings / whitespace as NA, coerce to numbers, then fill with 0 
        result[numeric_cols] = ( result[numeric_cols] .replace(r'^\s*$', pd.NA, regex=True) .apply(pd.to_numeric, errors='coerce') .fillna(0.0) )
       
        return result
