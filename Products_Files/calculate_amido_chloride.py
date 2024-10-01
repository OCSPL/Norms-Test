import pandas as pd
from Utils.utils import multiply_with_percentage

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

def calculate_amido_chloride(stock_summary,job_work_df):
    
     
    net_qty_amido_recovered_isopropyl_acetate = stock_summary.loc[
        stock_summary['Item Name'] == 'AMIDO IPA ML', 'NET QTY'].values[0]
    
    
    # Update the 'ADDITIONAL QTY CONSUMED IN OTHER WIP' column for 'AMIDO RECOVERED IPAC INTERCUT-1'
    stock_summary.loc[
        stock_summary['Item Name'] == 'AMIDO RECOVERED ISO PROPYL ACETATE', 'ADDITIONAL QTY CONSUMED IN OTHER WIP'] = net_qty_amido_recovered_isopropyl_acetate
    
    stock_summary = stock_summary.apply(calculate_quantities, axis=1)


    # Get the 'NET QTY' value of 'AMIDO RECOVERED ISO PROPYL ACETATE'
    net_qty_amido_recovered_isopropyl_acetate = stock_summary.loc[
        stock_summary['Item Name'] == 'AMIDO RECOVERED ISO PROPYL ACETATE', 'NET QTY'].values[0]
    
    # Update the 'ADDITIONAL QTY CONSUMED IN OTHER WIP' column for 'AMIDO RECOVERED IPAC INTERCUT-1'
    stock_summary.loc[
        stock_summary['Item Name'] == 'AMIDO RECOVERED IPAC INTERCUT-1', 'ADDITIONAL QTY CONSUMED IN OTHER WIP'] = net_qty_amido_recovered_isopropyl_acetate
    
    stock_summary = stock_summary.apply(calculate_quantities, axis=1)

    return stock_summary