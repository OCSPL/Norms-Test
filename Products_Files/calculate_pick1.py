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

    # For rows where Highlight is False, update RM WIP QTY using a proportional calculation
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

def calculate_pick1(stock_summary, bom_summaries_df):
    bom_summaries_df.to_csv('bom_summaries_df.csv')
    # --- Process "PICK-I-A (STAGE-I)" ---
    # From BOM summary, locate the row where both Stage Name and Name equal "PICK-I-A (STAGE-I)"
    # and extract its RM WIP QTY value.
    try:
        additional_qty_for_pick1 = bom_summaries_df.loc[
            (bom_summaries_df['Stage Name'] == 'PICK-I-B (STAGE-II)') &
            (bom_summaries_df['Name'] == 'PICK-I-A (STAGE-I)'),
            'RM WIP QTY'
        ].iloc[0]
    except IndexError:
        additional_qty_for_pick1 = 0

    # Update the "ADDITIONAL QTY CONSUMED IN OTHER WIP" for the "PICK-I-A (STAGE-I)" row in stock_summary
    stock_summary.loc[
        stock_summary['Item Name'] == 'PICK-I-A (STAGE-I)',
        'ADDITIONAL QTY CONSUMED IN OTHER WIP'
    ] = additional_qty_for_pick1

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

    # Finally, update the BOM summary for "PICK-I-A (STAGE-I)" using the updated NET QTY values
    bom_summaries_df = update_bom_summaries_with_net_qty(stock_summary, bom_summaries_df, 'PICK-I-A (STAGE-I)')
    
    return stock_summary, bom_summaries_df
