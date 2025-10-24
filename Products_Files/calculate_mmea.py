import pandas as pd
from Utils.utils import multiply_with_percentage

def update_bom_summaries_with_net_qty(stock_summary, bom_summaries_df, stage_name):
    # 1) Sum NET QTY from stock_summary for the given stage_name
    net_qty_value = stock_summary.loc[
        stock_summary['Item Name'] == stage_name, 'NET QTY'
    ].sum()

    # 2) Update the RM WIP QTY in bom_summaries_df for the “highlight == True” rows
    main_mask = (
        (bom_summaries_df['Stage Name'] == stage_name) &
        (bom_summaries_df['Name'] == stage_name)
    )
    bom_summaries_df.loc[main_mask, 'RM WIP QTY'] = net_qty_value

    # 3) Check if there is any “Quantity” at all for that exact (Stage Name, Name) pair
    selected_quantities = bom_summaries_df.loc[main_mask, 'Quantity']
    if selected_quantities.empty:
        # Nothing to distribute; simply return as‐is
        return bom_summaries_df

    # 4) Convert the first matching Quantity to a numeric value
    Quantity_uniq = pd.to_numeric(selected_quantities.values[0])

    # 5) For rows where Highlight == False (but Stage Name still equals stage_name),
    #    recalculate RM WIP QTY proportionally:
    def compute_rm_wip(row):
        if (row['Stage Name'] == stage_name) and (row['Highlight'] is False):
            return (row['BOMQty'] / Quantity_uniq) * net_qty_value
        else:
            return row.get('RM WIP QTY', 0)

    # Apply row‐by‐row only when (Stage Name == stage_name) & (Highlight == False)
    mask_non_highlight = (
        (bom_summaries_df['Stage Name'] == stage_name) &
        (bom_summaries_df['Highlight'] == False)
    )
    if not bom_summaries_df.loc[mask_non_highlight].empty:
        bom_summaries_df.loc[mask_non_highlight, 'RM WIP QTY'] = (
            bom_summaries_df.loc[mask_non_highlight]
            .apply(compute_rm_wip, axis=1)
        )

    return bom_summaries_df

def calculate_mmea(stock_summary, bom_summaries_df):
    # Define stages + name_filters (can expand this list as needed)
    stages = [
        ('MMEA N-BUTANOL MLR', ['MMEA RECOVERED N-BUTANOL'])
    ]

    for stage_name, name_filters in stages:
        # 1) Sum existing “RM WIP QTY” from bom_summaries_df for name_filters
        additional_qty_consumed = bom_summaries_df.loc[
            (bom_summaries_df['Stage Name'].isin(name_filters)) &
            (bom_summaries_df['Name'] == stage_name),
            'RM WIP QTY'
        ].sum()

        # 2) Update stock_summary’s “ADDITIONAL QTY CONSUMED IN OTHER WIP”
        stock_summary.loc[
            stock_summary['Item Name'] == stage_name,
            'ADDITIONAL QTY CONSUMED IN OTHER WIP'
        ] = additional_qty_consumed

        # 3) Compute “Additional_QTY_mult” (round to 2 decimals)
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

        # 4) Recompute “NET QTY” for every row in stock_summary
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

        # 5) Finally, update bom_summaries_df based on the newly calculated NET QTY
        bom_summaries_df = update_bom_summaries_with_net_qty(
            stock_summary,
            bom_summaries_df,
            stage_name
        )

    return stock_summary, bom_summaries_df



def calculate_con_mmea(Con_qty,stock_summary):
    # Filter the stock_summary DataFrame for '2,4,6 RECOVERED MESITYLENE'
    mesitylene_row = stock_summary[stock_summary['Item Name'] == 'MMEA N-BUTANOL MLR']

    # Extract the 'NET QTY' value and store it in a variable
    mesitylene_net_qty = mesitylene_row['NET QTY'].values[0] if not mesitylene_row.empty else None

    # Filter the Con_qty DataFrame for 'MESITYLENE (M)'
    mesitylene_con_qty_row = Con_qty[Con_qty['Consume_Item_Name'] == 'N BUTANOL (M)']

    # Add the mesitylene_net_qty value to 'WIP-RM' in the filtered row
    if not mesitylene_con_qty_row.empty:
        Con_qty['WIP-RM'] = Con_qty['WIP-RM'].fillna(0.0)
        Con_qty.loc[mesitylene_con_qty_row.index, 'WIP-RM'] += mesitylene_net_qty
    return Con_qty
