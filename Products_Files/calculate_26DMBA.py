import pandas as pd
from Utils.utils import multiply_with_percentage

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

def calculate_26DMBA(stock_summary, bom_summaries_df):
    # Define the stages and corresponding name filters to process
    stages = [
        ('2,6 DMBN (STAGE-II) WET POWDER', ['2,6 DMBA (STAGE-III) WET POWDER']),
        ('2,6 DCBN (STAGE-I) DRY POWDER', ['2,6 DMBN (STAGE-II) WET POWDER'])
        
    ]
    
    for stage_name, name_filters in stages:
        # Sum RM WIP QTY for the given name_filters, with special case handling
        additional_qty_consumed = bom_summaries_df.loc[
            (bom_summaries_df['Stage Name'].isin(name_filters)) &
            (bom_summaries_df['Name'] == stage_name),
            'RM WIP QTY'
        ].sum()

        if stage_name == '2,4,6 TMBCL STAGE-I':
            # Add the NET QTY of '2,4,6 TMBCL (STAGE-I) INTERCUT-1'
            intercut_net_qty = stock_summary.loc[
                stock_summary['Item Name'] == '2,4,6 TMBCL (STAGE-I) INTERCUT-1', 
                'NET QTY'
            ].values[0]
            additional_qty_consumed += intercut_net_qty

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

def Calculate_26(Con_qty,stock_summary,job_work_df_filtered):
    job_work_df_filtered=job_work_df_filtered[job_work_df_filtered['Consume_Item_Type'] == 'Raw Material']
    # job_work_df_filtered.to_csv('job_work_df_filtered.csv')

    # Filter the stock_summary DataFrame for '2,4,6 RECOVERED MESITYLENE'
    MLR_row = stock_summary[stock_summary['Item Name'] == '2,6 DMBA (STAGE-III) EDC MLR']
    EDC_row = stock_summary[stock_summary['Item Name'] == '2,6 DMBA (STAGE-III) RECOVERED EDC']
    Tol_row_1 = stock_summary[stock_summary['Item Name'] == '2,6 DMBN (STAGE-II) RECOVERED TOLUENE']
    Tol_row_2 = stock_summary[stock_summary['Item Name'] == '2,6 DMBN (STAGE-II) TOLUENE MLR']

    # Extract the 'NET QTY' value and store it in a variable
    mesitylene_net_qty = (MLR_row['NET QTY'].values[0] if not MLR_row.empty else None)+(EDC_row['NET QTY'].values[0] if not EDC_row.empty else None)
    # Filter the Con_qty DataFrame for 'MESITYLENE (M)'
    mesitylene_con_qty_row = Con_qty[Con_qty['Consume_Item_Name'] == 'ETHYLENE DI CHLORIDE (M)']

    # Add the mesitylene_net_qty value to 'WIP-RM' in the filtered row
    if not mesitylene_con_qty_row.empty:
        Con_qty.loc[mesitylene_con_qty_row.index, 'WIP-RM'] += mesitylene_net_qty
    
    # Extract the 'NET QTY' value and store it in a variable
    Toluene_net_qty = (Tol_row_1['NET QTY'].values[0] if not Tol_row_1.empty else None)+(Tol_row_2['NET QTY'].values[0] if not Tol_row_2.empty else None)
    # Filter the Con_qty DataFrame for 'MESITYLENE (M)'
    Toluene_con_qty_row = Con_qty[Con_qty['Consume_Item_Name'] == 'TOLUENE (M)']

    # Add the mesitylene_net_qty value to 'WIP-RM' in the filtered row
    if not Toluene_con_qty_row.empty:
        Con_qty.loc[Toluene_con_qty_row.index, 'WIP-RM'] += Toluene_net_qty

        # Group by 'Consume_Item_Name' and aggregate
    grouped_job_work_df = job_work_df_filtered.groupby('Consume_Item_Name').agg({
        'Consume_Quantity': 'sum',
        'Consume_Value': 'sum'
    }).reset_index()
    # grouped_job_work_df.to_csv('grouped_job_work_df.csv')
    # Merge the grouped_job_work_df with Con_qty based on 'Consume_Item_Name'
    Con_qty = pd.merge(Con_qty, grouped_job_work_df, on='Consume_Item_Name', how='left')
    
    # Add 'Consume_Quantity' to 'Net_Qty' and 'Consume_Value' to 'Value2'
    Con_qty['Net_Qty'] = Con_qty['Net_Qty'] + Con_qty['Consume_Quantity'].fillna(0)
    Con_qty['Value2'] = Con_qty['Value2'] + Con_qty['Consume_Value'].fillna(0)

    # Update the 'Rate' column as Value2 divided by Net_Qty
    Con_qty['Rate'] = Con_qty['Value2'] / Con_qty['Net_Qty']
    return Con_qty