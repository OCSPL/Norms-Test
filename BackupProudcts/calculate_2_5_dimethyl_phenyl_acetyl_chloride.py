import pandas as pd
from Utils.utils import multiply_with_percentage



def calculate_2_5_dimethyl_phenyl_acetyl_chloride(stock_summary, bom_summaries_df): 
    # Define a list for item names used in multiple places
    item_names = {
        'dmpac_sfg': '2,5 DMPAC SFG',
        'dmbcn_stage_ii': '2,5 DMBCN STAGE-II',
        'dmbcl_stage_i_intercut': '2,5 DMBCL (STAGE-I) INTERCUT',
        'dmbcl_stage_i': '2,5 DMBCL STAGE-I',
        'dmbcn_stage_ii_crude': '2,5 DMBCN (STAGE-II) CRUDE',
        'dmbcl_stage_i_crude': '2,5 DMBCL (STAGE-I) CRUDE'  # Added this for the last step
    }
    
    # Function to calculate NET QTY and Additional_QTY_mult
    def calculate_quantities(row):
        row['Additional_QTY_mult'] = round(
            multiply_with_percentage(
                row['ADDITIONAL QTY CONSUMED IN OTHER WIP'], 
                row['ADDITIONAL QTY CONSUMED IN OTHER WIP_activation']
            ), 
            2
        )
        row['NET QTY'] = round(
            multiply_with_percentage(row['Closing'], row['Closing_activation']) +
            multiply_with_percentage(row['Quantities Consumed from Opening Stock'], row['Quantities Consumed from Opening Stock_activation']) +
            multiply_with_percentage(row['ADDITIONAL QTY CONSUMED IN OTHER WIP'], row['ADDITIONAL QTY CONSUMED IN OTHER WIP_activation']), 
            2
        )
        return row
    
    # Sum NET QTY for specific items and update 'ADDITIONAL QTY CONSUMED IN OTHER WIP'
    net_qty_sum = stock_summary.loc[
        stock_summary['Item Name'].isin(['2,5 DMPACL (STAGE-IV) INTERCUT-1', '2,5 DMPACL (STAGE-IV) INTERCUT-2']),
        'NET QTY'
    ].sum()
    stock_summary.loc[
        stock_summary['Item Name'] == item_names['dmpac_sfg'], 
        'ADDITIONAL QTY CONSUMED IN OTHER WIP'
    ] = net_qty_sum

    # Calculate Additional_QTY_mult and NET QTY for each row
    stock_summary = stock_summary.apply(calculate_quantities, axis=1)

    # Fetch and sum 'RM WIP QTY' from bom_summaries_df and 'NET QTY' from stock_summary with null handling
    total_value = (
        bom_summaries_df.loc[
            (bom_summaries_df['Stage Name'] == '2,5 DMPAA (STAGE-III) DRY POWDER') & 
            (bom_summaries_df['Name'] == '2,5 DMBCN STAGE-II'), 
            'RM WIP QTY'
        ].fillna(0).squeeze()
        # +
        # stock_summary.loc[
        #     stock_summary['Item Name'] == item_names['dmbcn_stage_ii'], 
        #     'NET QTY'
        # ].fillna(0).squeeze()
    )

    # Update the 'ADDITIONAL QTY CONSUMED IN OTHER WIP' in 'stock_summary' for '2,5 DMBCN STAGE-II'
    stock_summary.loc[
        stock_summary['Item Name'] == item_names['dmbcn_stage_ii'], 
        'ADDITIONAL QTY CONSUMED IN OTHER WIP'
    ] = total_value

    # Now, filter 'bom_summaries_df' for '2,5 DMBCN STAGE-II' and '2,5 DMBCN (STAGE-II) CRUDE' and get 'RM WIP QTY'
    rm_wip_qty_value = bom_summaries_df.loc[
        (bom_summaries_df['Stage Name'] == item_names['dmbcn_stage_ii']) & 
        (bom_summaries_df['Name'] == item_names['dmbcn_stage_ii_crude']), 
        'RM WIP QTY'
    ].fillna(0).squeeze()

    # Update 'ADDITIONAL QTY CONSUMED IN OTHER WIP' in 'stock_summary' for '2,5 DMBCN (STAGE-II) CRUDE'
    stock_summary.loc[
        stock_summary['Item Name'] == item_names['dmbcn_stage_ii_crude'], 
        'ADDITIONAL QTY CONSUMED IN OTHER WIP'
    ] = rm_wip_qty_value

    # Fetch 'NET QTY' for '2,5 DMBCL (STAGE-I) INTERCUT' from stock_summary
    dmbcl_stage_i_intercut_net_qty = stock_summary.loc[
        stock_summary['Item Name'] == item_names['dmbcl_stage_i_intercut'], 
        'NET QTY'
    ].fillna(0).squeeze()

    # Fetch 'RM WIP QTY' for '2,5 DMBCN (STAGE-II) CRUDE' from bom_summaries_df
    dmbcn_stage_ii_crude_rm_wip_qty = bom_summaries_df.loc[
        (bom_summaries_df['Stage Name'] == item_names['dmbcn_stage_ii_crude']) & 
        (bom_summaries_df['Name'] == item_names['dmbcl_stage_i']), 
        'RM WIP QTY'
    ].fillna(0).squeeze()

    # Add the above 'NET QTY' and 'RM WIP QTY' values
    combined_value = dmbcl_stage_i_intercut_net_qty + dmbcn_stage_ii_crude_rm_wip_qty

    # Update the combined value in 'ADDITIONAL QTY CONSUMED IN OTHER WIP' for '2,5 DMBCL STAGE-I'
    stock_summary.loc[
        stock_summary['Item Name'] == item_names['dmbcl_stage_i'], 
        'ADDITIONAL QTY CONSUMED IN OTHER WIP'
    ] = combined_value

    # Recalculate Additional_QTY_mult and NET QTY for the updated rows
    stock_summary = stock_summary.apply(calculate_quantities, axis=1)

    # Fetch 'NET QTY' from stock_summary and update in bom_summaries_df where Highlight is True
    for item_key in item_names:
        net_qty_value = stock_summary.loc[
            stock_summary['Item Name'] == item_names[item_key], 
            'NET QTY'
        ].fillna(0).squeeze()
        
        # Update RM WIP QTY where Highlight is True
        bom_summaries_df.loc[
            (bom_summaries_df['Stage Name'] == item_names[item_key]) & 
            (bom_summaries_df['Highlight'] == True), 
            'RM WIP QTY'
        ] = net_qty_value

        # Update RM WIP QTY where Highlight is False using the formula
        quantity_sum = pd.to_numeric(bom_summaries_df.loc[
            (bom_summaries_df['Stage Name'] == item_names[item_key]),
            'Quantity'
        ], errors='coerce').fillna(0).sum()

        bom_summaries_df.loc[
            (bom_summaries_df['Stage Name'] == item_names[item_key]) & 
            (bom_summaries_df['Highlight'] == False), 
            'RM WIP QTY'
        ] = bom_summaries_df.apply(
            lambda row: (pd.to_numeric(row['BOMQty'], errors='coerce') / quantity_sum) * net_qty_value 
            if (row['Stage Name'] == item_names[item_key] and not row['Highlight']) else row['RM WIP QTY'], 
            axis=1
        )

    # New step: Fetch RM WIP QTY from bom_summaries_df for '2,5 DMBCL STAGE-I' and '2,5 DMBCL (STAGE-I) CRUDE'
    dmbcl_stage_i_crude_wip_qty = bom_summaries_df.loc[
        (bom_summaries_df['Stage Name'] == item_names['dmbcl_stage_i']) & 
        (bom_summaries_df['Name'] == item_names['dmbcl_stage_i_crude']), 
        'RM WIP QTY'
    ].fillna(0).squeeze()

    # Update this value in stock_summary for '2,5 DMBCL (STAGE-I) CRUDE'
    stock_summary.loc[
        stock_summary['Item Name'] == item_names['dmbcl_stage_i_crude'], 
        'ADDITIONAL QTY CONSUMED IN OTHER WIP'
    ] = dmbcl_stage_i_crude_wip_qty

    # Recalculate Additional_QTY_mult and NET QTY for the updated rows after the final update
    stock_summary = stock_summary.apply(calculate_quantities, axis=1)

    # Final step: Update RM WIP QTY in bom_summaries_df for '2,5 DMBCL (STAGE-I) CRUDE' where Highlight is True
    net_qty_value = stock_summary.loc[
        stock_summary['Item Name'] == item_names['dmbcl_stage_i_crude'], 
        'NET QTY'
    ].fillna(0).squeeze()
  

    bom_summaries_df.loc[
        (bom_summaries_df['Stage Name'] == item_names['dmbcl_stage_i_crude']) & 
        (bom_summaries_df['Name'] == item_names['dmbcl_stage_i_crude']) & 
        (bom_summaries_df['Highlight'] == True), 
        'RM WIP QTY'
    ] = net_qty_value
     # New step: Update RM WIP QTY in bom_summaries_df where Highlight is False for '2,5 DMBCL (STAGE-I) CRUDE'
    quantity_sum = pd.to_numeric(bom_summaries_df.loc[
        (bom_summaries_df['Stage Name'] == item_names['dmbcl_stage_i_crude']),
        'Quantity'
    ], errors='coerce').fillna(0).sum()

    bom_summaries_df.loc[
        (bom_summaries_df['Stage Name'] == item_names['dmbcl_stage_i_crude']) & 
        (bom_summaries_df['Highlight'] == False), 
        'RM WIP QTY'
    ] = bom_summaries_df.apply(
        lambda row: (pd.to_numeric(row['BOMQty'], errors='coerce') / quantity_sum) * net_qty_value 
        if (row['Stage Name'] == item_names['dmbcl_stage_i_crude'] and not row['Highlight']) else row['RM WIP QTY'], 
        axis=1
    )

    return stock_summary, bom_summaries_df
