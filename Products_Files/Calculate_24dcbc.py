import pandas as pd


def Calculate_24dcbc(Con_qty, job_work_df,stock_summary):
    # Group by 'Consume_Item_Name' and aggregate
    grouped_job_work_df = job_work_df.groupby('Consume_Item_Name').agg({
        'Consume_Quantity': 'sum',
        'Consume_Value': 'sum'
    }).reset_index()
    # Merge the grouped_job_work_df with Con_qty based on 'Consume_Item_Name'
    Con_qty = pd.merge(Con_qty, grouped_job_work_df, on='Consume_Item_Name', how='left')
    
    # Add 'Consume_Quantity' to 'Net_Qty' and 'Consume_Value' to 'Value2'
    Con_qty['Net_Qty'] = Con_qty['Net_Qty'] + Con_qty['Consume_Quantity'].fillna(0)
    Con_qty['Value2'] = Con_qty['Value2'] + Con_qty['Consume_Value'].fillna(0)

    # Update the 'Rate' column as Value2 divided by Net_Qty
    Con_qty['Rate'] = Con_qty['Value2'] / Con_qty['Net_Qty']

    ##########################################################################################

    # Filter the stock_summary DataFrame for '2,4,6 RECOVERED MESITYLENE'
    mesitylene_row = stock_summary[stock_summary['Item Name'] == 'RECOVERED 2,4 DCT']

    # Extract the 'NET QTY' value and store it in a variable
    mesitylene_net_qty = mesitylene_row['NET QTY'].values[0] if not mesitylene_row.empty else None

    # Filter the Con_qty DataFrame for 'MESITYLENE (M)'
    mesitylene_con_qty_row = Con_qty[Con_qty['Consume_Item_Name'] == '2,4 DICHLORO TOLUENE (M)']

    # Add the mesitylene_net_qty value to 'WIP-RM' in the filtered row
    if not mesitylene_con_qty_row.empty:
        Con_qty.loc[mesitylene_con_qty_row.index, 'WIP-RM'] += mesitylene_net_qty

    return  Con_qty