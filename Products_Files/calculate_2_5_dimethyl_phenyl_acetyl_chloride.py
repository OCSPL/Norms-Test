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

def calculate_2_5_dimethyl_phenyl_acetyl_chloride(stock_summary, bom_summaries_df):
    
    
   
    stages = [
        ('2,5 DMPAA (STAGE-III) DRY POWDER', ['2,5 DMPAC (STAGE-IV) CRUDE']),
        ('2,5 DMBCN STAGE-II', ['2,5 DMPAA (STAGE-III) DRY POWDER']),
        ('2,5 DMBCN (STAGE-II) CRUDE', ['2,5 DMBCN STAGE-II']),
        ('2,5 DMBCL STAGE-I', ['2,5 DMBCN (STAGE-II) CRUDE']),
        ('2,5 DMBCL (STAGE-I) CRUDE', ['2,5 DMBCL STAGE-I']), 
     
    ]
    
    for stage_name, name_filters in stages:
        # Sum RM WIP QTY for the given name_filters, with special case handling
        additional_qty_consumed = bom_summaries_df.loc[
            (bom_summaries_df['Stage Name'].isin(name_filters)) &
            (bom_summaries_df['Name'] == stage_name),
            'RM WIP QTY'
        ].sum()
        print(f"Additional QTY for {name_filters}: {additional_qty_consumed}")

        # Update ADDITIONAL QTY CONSUMED IN OTHER WIP
        stock_summary.loc[
            stock_summary['Item Name'] == stage_name, 
            'ADDITIONAL QTY CONSUMED IN OTHER WIP'
        ] = additional_qty_consumed

        # Step 1 & 2: Sum the NET QTY of '2,5 DMPACL (STAGE-IV) INTERCUT-1' and '2,5 DMPACL (STAGE-IV) INTERCUT-2'
        intercut_net_qty_sum = stock_summary.loc[
            stock_summary['Item Name'].isin([
                '2,5 DMPACL (STAGE-IV) INTERCUT-1',
                '2,5 DMPACL (STAGE-IV) INTERCUT-2'
            ]),
            'NET QTY'
        ].sum()

        # Step 3: Update 'ADDITIONAL QTY CONSUMED IN OTHER WIP' for '2,5 DMPAC SFG'
        stock_summary.loc[
            stock_summary['Item Name'] == '2,5 DMPAC SFG',
            'ADDITIONAL QTY CONSUMED IN OTHER WIP'
        ] = intercut_net_qty_sum

        # Handle special cases
        # if stage_name == '2,5 DMBCN STAGE-II':
        #     # Existing logic for '2,5 DMBCN STAGE-II'
        #     intercut_net_qty = stock_summary.loc[
        #         stock_summary['Item Name'] == '2,5 DMBCN (STAGE II) INTERCUT',
        #         'NET QTY'
        #     ]
        #     intercut_net_qty = intercut_net_qty.values[0] if not intercut_net_qty.empty else 0
        #     additional_qty_consumed += intercut_net_qty

        if stage_name == '2,5 DMBCL STAGE-I':
            # New logic for '2,5 DMBCL STAGE-I'
            intercut_net_qty = stock_summary.loc[
                stock_summary['Item Name'] == '2,5 DMBCL (STAGE-I) INTERCUT',
                'NET QTY'
            ]
            intercut_net_qty = intercut_net_qty.values[0] if not intercut_net_qty.empty else 0
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

def Calculate_25(Con_qty,stock_summary):
    # Filter the stock_summary DataFrame for '2,4,6 RECOVERED MESITYLENE'
    mesitylene_row = stock_summary[stock_summary['Item Name'] == '2,5 DMBCL RECOVERED P- XYLENE']

    # Extract the 'NET QTY' value and store it in a variable
    mesitylene_net_qty = mesitylene_row['NET QTY'].values[0] if not mesitylene_row.empty else None

    # Filter the Con_qty DataFrame for 'MESITYLENE (M)'
    mesitylene_con_qty_row = Con_qty[Con_qty['Consume_Item_Name'] == 'PARA XYLENE (M)']

    # Add the mesitylene_net_qty value to 'WIP-RM' in the filtered row
    if not mesitylene_con_qty_row.empty:
        Con_qty.loc[mesitylene_con_qty_row.index, 'WIP-RM'] += mesitylene_net_qty
    

    #----------------------------------------------------------------------------------------------------
    tol_row = stock_summary[stock_summary['Item Name'] == '2,5 RECOVERED TOLUENE']

    # Extract the 'NET QTY' value and store it in a variable
    tol_net_qty = tol_row['NET QTY'].values[0] if not tol_row.empty else 0

    # Filter the Con_qty DataFrame for 'MESITYLENE (M)'
    tol_con_qty_row = Con_qty[Con_qty['Consume_Item_Name'] == 'TOLUENE (M)']

    # Add the mesitylene_net_qty value to 'WIP-RM' in the filtered row
    if not tol_con_qty_row.empty:
        Con_qty.loc[tol_con_qty_row.index, 'WIP-RM'] += tol_net_qty
    return Con_qty