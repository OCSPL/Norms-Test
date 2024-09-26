import pandas as pd
from Utils.utils import multiply_with_percentage

# Function to perform specific calculations for '2,3 DI CHLORO PYRIDINE'
def calculate_2_3_dichloro_pyridine(stock_summary):
    # Calculate sum of Closing, Quantities Consumed from Opening Stock, and ADDITIONAL QTY CONSUMED IN OTHER WIP for '2,3 DCP IPA ML'
    stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP IPA ML', 'Values'] = (
        stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP IPA ML', 'Closing'] +
        stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP IPA ML', 'Quantities Consumed from Opening Stock'] +
        stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP IPA ML', 'ADDITIONAL QTY CONSUMED IN OTHER WIP']
    )

    # Get the NET QTY value from '2,3 DCP IPA ML'
    net_qty_value = stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP IPA ML', 'Values'].values[0]
    # Place NET QTY from Stage I to ADDITIONAL QTY CONSUMED IN OTHER WIP in Stage II
    stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP RECOVERED IPA', 'ADDITIONAL QTY CONSUMED IN OTHER WIP'] = stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP IPA ML', 'NET QTY'].values[0]
    # Multiply by 7% and place into Stage III 'ADDITIONAL QTY CONSUMED IN OTHER WIP'
    stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP IIND CROP', 'ADDITIONAL QTY CONSUMED IN OTHER WIP'] = round(net_qty_value * 0.07, 2)

    # Update the 'Additional_QTY_mult' for Stage II
    stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP RECOVERED IPA', 'Additional_QTY_mult'] = stock_summary.apply(
        lambda row: round(multiply_with_percentage(row['ADDITIONAL QTY CONSUMED IN OTHER WIP'], row['ADDITIONAL QTY CONSUMED IN OTHER WIP_activation']), 2)
        if row['Item Name'] == '2,3 DCP RECOVERED IPA' else 0, axis=1
    )

    # Update the 'Additional_QTY_mult' for Stage III
    stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP IIND CROP', 'Additional_QTY_mult'] = stock_summary.apply(
        lambda row: round(multiply_with_percentage(row['ADDITIONAL QTY CONSUMED IN OTHER WIP'], row['ADDITIONAL QTY CONSUMED IN OTHER WIP_activation']), 2)
        if row['Item Name'] == '2,3 DCP IIND CROP' else 0, axis=1
    )

    # Update TOTAL NET QTY for Stage II and Stage III 'ADDITIONAL QTY CONSUMED IN OTHER WIP'
    stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP RECOVERED IPA', 'TOTAL NET QTY'] = stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP RECOVERED IPA', 'Additional_QTY_mult']
    stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP IIND CROP', 'TOTAL NET QTY'] = stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP IIND CROP', 'Additional_QTY_mult']

    # Update NET QTY for Stage II based on intermediate values
    stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP RECOVERED IPA', 'NET QTY'] = (
        stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP RECOVERED IPA', 'Closing_mult'] +
        stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP RECOVERED IPA', 'Quantities_Consumed_mult'] +
        stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP RECOVERED IPA', 'Additional_QTY_mult']
    ).values[0]

    # Update NET QTY for Stage III based on intermediate values
    stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP IIND CROP', 'NET QTY'] = (
        stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP IIND CROP', 'Closing_mult'] +
        stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP IIND CROP', 'Quantities_Consumed_mult'] +
        stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP IIND CROP', 'Additional_QTY_mult']
    ).values[0]
    # Add NET QTY from Stage III and Stage IV to final_output_quantity
    stage_3_net_qty = stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP IIND CROP', 'NET QTY'].values[0]
    stage_4_net_qty = stock_summary.loc[stock_summary['Item Name'] == '2,3 DCP SFG', 'NET QTY'].values[0]
    return round(stage_3_net_qty,2), round(stage_4_net_qty,2)
