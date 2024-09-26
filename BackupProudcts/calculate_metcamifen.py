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

def calculate_metcamifen(stock_summary,bom_summaries_df):
    
    stock_summary.to_csv('stock_summary.csv')
    bom_summaries_df.to_csv('bom_summaries_df.csv')
    return stock_summary,bom_summaries_df