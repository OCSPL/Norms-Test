import pandas as pd
from Utils.utils import multiply_with_percentage

def update_bom_summaries_with_net_qty(stock_summary, bom_summaries_df, stage_name):
    # Sum the NET QTY values from the different sources
    net_qty_value = stock_summary.loc[
        stock_summary['Item Name'] == stage_name, 'NET QTY'
    ].sum()

    # Only proceed if there is at least one matching RM row in bom_summaries_df
    mask_exact = (
        (bom_summaries_df['Stage Name'] == stage_name) &
        (bom_summaries_df['Name'] == stage_name)
    )
    if not mask_exact.any():
        print(f"[update_bom] no exact rows for stage '{stage_name}', skipping.")
        return bom_summaries_df

    # Update the “highlight” row first
    bom_summaries_df.loc[mask_exact, 'RM WIP QTY'] = net_qty_value

    # Safely extract Quantity_uniq
    qty_series = bom_summaries_df.loc[mask_exact, 'Quantity']
    try:
        Quantity_uniq = float(qty_series.values[0])
    except (IndexError, ValueError):
        print(f"[update_bom] could not parse Quantity for '{stage_name}', skipping proportional update.")
        return bom_summaries_df

    # Now update all non-highlight rows proportionally
    def compute_row(row):
        if (row['Stage Name'] == stage_name) and (row['Highlight'] is False):
            return (row['BOMQty'] / Quantity_uniq) * net_qty_value
        else:
            return row.get('RM WIP QTY', 0)

    bom_summaries_df['RM WIP QTY'] = bom_summaries_df.apply(compute_row, axis=1)
    return bom_summaries_df

def calculate_24dc(stock_summary, bom_summaries_df):
    # Define the stages and corresponding name filters to process
    stages = [
        ('2,4 DCBC ORGANIC LAYER', ['RECOVERED 2,4 DCT']),
    ]
    
    for stage_name, name_filters in stages:
        # Sum RM WIP QTY for the given name_filters, with special case handling
        # additional_qty_consumed = bom_summaries_df.loc[
        #     (bom_summaries_df['Stage Name'].isin(name_filters)) &
        #     (bom_summaries_df['Name'] == stage_name),
        #     'RM WIP QTY'
        # ].sum()
        additional_qty_consumed = bom_summaries_df.loc[
            (bom_summaries_df['Stage Name'] == '2,4 DCBC ORGANIC LAYER') &
            (bom_summaries_df['Name'] == 'RECOVERED 2,4 DCT'),
            'RM WIP QTY'
        ].sum()
        # print(additional_qty_consumed)  # should be 5800.82

        # print(test)

        # Update ADDITIONAL QTY CONSUMED IN OTHER WIP
        stock_summary.loc[
            stock_summary['Item Name'] == 'RECOVERED 2,4 DCT', 
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
    # print(mesitylene_row)

    # Extract the 'NET QTY' value and store it in a variable
    mesitylene_net_qty = mesitylene_row['NET QTY'].values[0] if not mesitylene_row.empty else None

    # Filter the Con_qty DataFrame for 'MESITYLENE (M)'
    mesitylene_con_qty_row = Con_qty[Con_qty['Consume_Item_Name'] == '2,4 DICHLORO TOLUENE (M)']

    # Add the mesitylene_net_qty value to 'WIP-RM' in the filtered row
    if not mesitylene_con_qty_row.empty:
        Con_qty.loc[mesitylene_con_qty_row.index, 'WIP-RM'] += mesitylene_net_qty

    return  Con_qty