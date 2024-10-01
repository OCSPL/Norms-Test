import pandas as pd
from Utils.utils import multiply_with_percentage

def calculate_nndi_iso_propyl_ethyl_amine(stock_summary):
    # Take DIPEA RESIDUE 'NET QTY' value and place under DIPEA INTERCUT-2 (IIND CUT) in Additional_QTY_mult
    net_qty_value_residue = stock_summary.loc[stock_summary['Item Name'] == 'DIPEA RESIDUE', 'NET QTY'].values[0]
    stock_summary.loc[stock_summary['Item Name'] == 'DIPEA INTERCUT-2 (IIND CUT)', 'ADDITIONAL QTY CONSUMED IN OTHER WIP'] = net_qty_value_residue
    stock_summary.loc[stock_summary['Item Name'] == 'DIPEA INTERCUT-2 (IIND CUT)', 'Additional_QTY_mult'] = stock_summary.apply(
        lambda row: round(multiply_with_percentage(row['ADDITIONAL QTY CONSUMED IN OTHER WIP'], row['ADDITIONAL QTY CONSUMED IN OTHER WIP_activation']), 2)
        if row['Item Name'] == 'DIPEA INTERCUT-2 (IIND CUT)' else 0, axis=1
    )
    # Update TOTAL NET QTY for Stage II
    stock_summary.loc[stock_summary['Item Name'] == 'DIPEA INTERCUT-2 (IIND CUT)', 'TOTAL NET QTY'] = stock_summary.loc[stock_summary['Item Name'] == 'DIPEA INTERCUT-2 (IIND CUT)', 'Additional_QTY_mult']
    # Update NET QTY for Stage II based on intermediate values
    stock_summary.loc[stock_summary['Item Name'] == 'DIPEA INTERCUT-2 (IIND CUT)', 'NET QTY'] = (
        stock_summary.loc[stock_summary['Item Name'] == 'DIPEA INTERCUT-2 (IIND CUT)', 'Closing_mult'] +
        stock_summary.loc[stock_summary['Item Name'] == 'DIPEA INTERCUT-2 (IIND CUT)', 'Quantities_Consumed_mult'] +
        stock_summary.loc[stock_summary['Item Name'] == 'DIPEA INTERCUT-2 (IIND CUT)', 'Additional_QTY_mult']
    ).values[0]
    return stock_summary


def calculate_dpi(Con_qty,stock_summary):
    # Ensure 'WIP-RM' column exists in Con_qty and is numeric
    if 'WIP-RM' not in Con_qty.columns:
        Con_qty['WIP-RM'] = 0.0
    else:
        Con_qty['WIP-RM'] = pd.to_numeric(Con_qty['WIP-RM'], errors='coerce').fillna(0.0)

    # Ensure 'NET QTY' is numeric in stock_summary
    stock_summary['NET QTY'] = pd.to_numeric(stock_summary['NET QTY'], errors='coerce').fillna(0.0)

    # Get total 'NET QTY' for both items
    items_to_sum = ['DIPA WIP (IST CUT)', 'DIPEA INTERCUT-2 (IIND CUT)']
    total_net_qty = stock_summary.loc[
        stock_summary['Item Name'].isin(items_to_sum), 'NET QTY'
    ].sum()

    # Add total_net_qty to 'WIP-RM' for 'DIISOPROPYLAMINE (M)' in Con_qty
    Con_qty.loc[
        Con_qty['Consume_Item_Name'] == 'DIISOPROPYLAMINE (M)', 'WIP-RM'
    ] += total_net_qty

    return Con_qty   
