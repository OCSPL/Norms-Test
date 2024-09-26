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
