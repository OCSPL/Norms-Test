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

    Quantity_uniq = pd.to_numeric(
    bom_summaries_df.loc[
        (bom_summaries_df['Stage Name'] == stage_name) & 
        (bom_summaries_df['Name'] == stage_name), 
        'Quantity'
    ].values[0] if not bom_summaries_df.loc[
        (bom_summaries_df['Stage Name'] == stage_name) & 
        (bom_summaries_df['Name'] == stage_name), 
        'Quantity'
    ].empty else 0
    )

    # Update the RM WIP QTY in bom_summaries_df where Highlight is False
    bom_summaries_df.loc[
    (bom_summaries_df['Stage Name'] == stage_name) & 
    (bom_summaries_df['Highlight'] == False), 
    'RM WIP QTY'
    ] = bom_summaries_df.apply(
        lambda row: (row['BOMQty'] / Quantity_uniq) * net_qty_value 
        if Quantity_uniq != 0 and row['Highlight'] == False else row['RM WIP QTY'], 
        axis=1
    )

    
    return bom_summaries_df

def calculate_metcamifen(stock_summary, bom_summaries_df):
    # Define the stages and corresponding name filters to process
    stages = [
        ('METCAMIFEN STAGE-II OA-CI CRUDE', ['METCAMIFEN STAGE-II- OA-Cl MAINCUT']),
        ('METCAMIFEN STAGE-II OA-CI INTERCUT-1',['METCAMIFEN STAGE-II- OA-Cl MAINCUT']),
        # ('2,4,6 TMBCN STAGE-II', ['2,4,6 TMPACL (STAGE-III) DRY POWDER','2,4,6 TMPACL (STAGE-III) WET POWDER']),
        # ('2,4,6 TMBCL STAGE-I', ['2,4,6 TMBCN STAGE-II', '2,4,6 TMBCN (STAGE-II) WET CAKE', '2,4,6 TMBCL (STAGE-I) INTERCUT-1']),
        # ('2,4,6 TMBCL (STAGE-I) ORGANIC LAYER', ['2,4,6 TMBCL STAGE-I']),
    ]
    
    for stage_name, name_filters in stages:
        # Sum RM WIP QTY for the given name_filters, with special case handling
        additional_qty_consumed = bom_summaries_df.loc[
            (bom_summaries_df['Stage Name'].isin(name_filters)) &
            (bom_summaries_df['Name'] == stage_name),
            'RM WIP QTY'
        ].sum()
        # print(stage_name)
        # print(name_filters)
        # print(additional_qty_consumed)

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

def Calculate_met(Con_qty,bom_summaries_df):
    # Filter the stock_summary DataFrame for '2,4,6 RECOVERED MESITYLENE'
    mesitylene_row = bom_summaries_df[bom_summaries_df['Name'] == '2-METHOXY BENZOIC ACID']

    # Extract the 'NET QTY' value and store it in a variable
    mesitylene_net_qty = mesitylene_row['RM WIP QTY'].values[0] if not mesitylene_row.empty else None

    # Filter the Con_qty DataFrame for 'MESITYLENE (M)'
    mesitylene_con_qty_row = Con_qty[Con_qty['Consume_Item_Name'] == '2-MBA SFG']

    # Add the mesitylene_net_qty value to 'WIP-RM' in the filtered row
    if not mesitylene_con_qty_row.empty:
        Con_qty.loc[mesitylene_con_qty_row.index, 'WIP-RM'] += mesitylene_net_qty
    return Con_qty    