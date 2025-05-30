import pandas as pd
from Utils.utils import multiply_with_percentage

def update_bom_summaries_with_net_qty(stock_summary, bom_summaries_df, stage_name):
    # Sum the NET QTY values for rows in stock_summary where Item Name equals stage_name
    net_qty_value = stock_summary.loc[
        stock_summary['Item Name'] == stage_name, 'NET QTY'
    ].sum()

    # Update the RM WIP QTY in bom_summaries_df for the row where Stage Name and Name match stage_name
    bom_summaries_df.loc[
        (bom_summaries_df['Stage Name'] == stage_name) &
        (bom_summaries_df['Name'] == stage_name),
        'RM WIP QTY'
    ] = net_qty_value

    # Extract and convert the Quantity to a numeric value for the specific item
    Quantity_uniq = pd.to_numeric(
        bom_summaries_df.loc[
            (bom_summaries_df['Stage Name'] == stage_name) &
            (bom_summaries_df['Name'] == stage_name),
            'Quantity'
        ].values[0]
    )

    # For rows where Highlight is False, update RM WIP QTY using proportional calculation
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

def calculate_DCAT(stock_summary, bom_summaries_df):
    # Process the "DICA SFG" stage: sum NET QTY for selected items and update the stock summary.
    stages = [
        ('DICA SFG', ['DICA INTERCUT-1', 'DICA INTERCUT-2', 'DICA SFG OFF SPEC']),
    ]
    
    for stage_name, name_filters in stages:
        if stage_name == 'DICA SFG':
            # Sum NET QTY for the provided list of items
            additional_net_qty = stock_summary.loc[
                stock_summary['Item Name'].isin(name_filters), 'NET QTY'
            ].sum()
            # Update the "ADDITIONAL QTY CONSUMED IN OTHER WIP" for the DICA SFG row
            stock_summary.loc[
                stock_summary['Item Name'] == stage_name, 'ADDITIONAL QTY CONSUMED IN OTHER WIP'
            ] = additional_net_qty
        else:
            # Fallback logic for any other stage if needed (not used here)
            additional_qty_consumed = bom_summaries_df.loc[
                (bom_summaries_df['Stage Name'].isin(name_filters)) &
                (bom_summaries_df['Name'] == stage_name),
                'RM WIP QTY'
            ].sum()
            stock_summary.loc[
                stock_summary['Item Name'] == stage_name, 'ADDITIONAL QTY CONSUMED IN OTHER WIP'
            ] = additional_qty_consumed

        # Recalculate Additional_QTY_mult for every row using the updated additional quantity value
        stock_summary['Additional_QTY_mult'] = stock_summary.apply(
            lambda row: round(
                multiply_with_percentage(
                    row['ADDITIONAL QTY CONSUMED IN OTHER WIP'],
                    row['ADDITIONAL QTY CONSUMED IN OTHER WIP_activation']
                ),
                2
            ),
            axis=1
        )
        
        # Recalculate NET QTY for each row in stock_summary
        net_qtys = []
        for _, row in stock_summary.iterrows():
            net_qty = multiply_with_percentage(row['Closing'], row['Closing_activation'])
            net_qty += multiply_with_percentage(
                row['Quantities Consumed from Opening Stock'],
                row['Quantities Consumed from Opening Stock_activation']
            )
            net_qty += multiply_with_percentage(
                row['ADDITIONAL QTY CONSUMED IN OTHER WIP'],
                row['ADDITIONAL QTY CONSUMED IN OTHER WIP_activation']
            )
            net_qtys.append(round(net_qty, 2))
        stock_summary['NET QTY'] = net_qtys

        # Update the BOM summary for the current stage (DICA SFG)
        bom_summaries_df = update_bom_summaries_with_net_qty(stock_summary, bom_summaries_df, stage_name)

    # --- Now process DCAT STAGE II CRUDE ---
    # First, from the BOM summary (for stage "DICA SFG"), locate the row where Name is "DCAT STAGE II CRUDE"
    try:
        additional_qty_for_crude = bom_summaries_df.loc[
            (bom_summaries_df['Stage Name'] == 'DICA SFG') &
            (bom_summaries_df['Name'] == 'DCAT STAGE II CRUDE'),
            'RM WIP QTY'
        ].iloc[0]
    except IndexError:
        additional_qty_for_crude = 0

    # Update the "ADDITIONAL QTY CONSUMED IN OTHER WIP" for the "DCAT STAGE II CRUDE" row in stock_summary
    stock_summary.loc[
        stock_summary['Item Name'] == 'DCAT STAGE II CRUDE',
        'ADDITIONAL QTY CONSUMED IN OTHER WIP'
    ] = additional_qty_for_crude

    # Now, update the NET QTY for the "DCAT STAGE II CRUDE" row using the latest additional quantity
    def recalc_net_qty(row):
        return round(
            multiply_with_percentage(row['Closing'], row['Closing_activation']) +
            multiply_with_percentage(
                row['Quantities Consumed from Opening Stock'],
                row['Quantities Consumed from Opening Stock_activation']
            ) +
            multiply_with_percentage(
                row['ADDITIONAL QTY CONSUMED IN OTHER WIP'],
                row['ADDITIONAL QTY CONSUMED IN OTHER WIP_activation']
            ),
            2
        )
    
    mask = stock_summary['Item Name'] == 'DCAT STAGE II CRUDE'
    stock_summary.loc[mask, 'NET QTY'] = stock_summary[mask].apply(recalc_net_qty, axis=1)

    # Recalculate Additional_QTY_mult for every row using the updated values
    stock_summary['Additional_QTY_mult'] = stock_summary.apply(
        lambda row: round(
            multiply_with_percentage(
                row['ADDITIONAL QTY CONSUMED IN OTHER WIP'],
                row['ADDITIONAL QTY CONSUMED IN OTHER WIP_activation']
            ),
            2
        ),
        axis=1
    )

    # Finally, update the BOM summary for "DCAT STAGE II CRUDE" using its updated NET QTY
    bom_summaries_df = update_bom_summaries_with_net_qty(stock_summary, bom_summaries_df, 'DCAT STAGE II CRUDE')
    
    return stock_summary, bom_summaries_df


def calculate_con_dcat(Con_qty,stock_summary):
    # Filter the stock_summary DataFrame for '2,4,6 RECOVERED MESITYLENE'
    mesitylene_row = stock_summary[stock_summary['Item Name'] == 'DCAT RECOVERED MDC']

    # Extract the 'NET QTY' value and store it in a variable
    mesitylene_net_qty = mesitylene_row['NET QTY'].values[0] if not mesitylene_row.empty else None

    # Filter the Con_qty DataFrame for 'MESITYLENE (M)'
    mesitylene_con_qty_row = Con_qty[Con_qty['Consume_Item_Name'] == 'METHYLENE DICHLORIDE (M)']

    # Add the mesitylene_net_qty value to 'WIP-RM' in the filtered row
    if not mesitylene_con_qty_row.empty:
        Con_qty.loc[mesitylene_con_qty_row.index, 'WIP-RM'] += mesitylene_net_qty
    return Con_qty