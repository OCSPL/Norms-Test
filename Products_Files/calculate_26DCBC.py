import pandas as pd

def calculate_26DCBC(Con_qty,job_work_df):
     # Group by 'Consume_Item_Name' and sum 'Consume_Quantity' and 'Consume_Value'
    grouped = job_work_df.groupby('Consume_Item_Name').agg({
        'Consume_Quantity': 'sum',
        'Consume_Value': 'sum'
    }).reset_index()

    # Calculate Rate = Consume_Value / Consume_Quantity
    grouped['Rate'] = grouped['Consume_Value'] / grouped['Consume_Quantity']

    # Prepare the DataFrame to append
    grouped['Net_Qty'] = grouped['Consume_Quantity']
    grouped['Value2'] = grouped['Consume_Value']
    grouped['WIP-RM'] = 0  # Assuming WIP-RM is 0 as in your example

    # Select and reorder the columns to match Con_qty
    grouped = grouped[['Consume_Item_Name', 'Net_Qty', 'Rate', 'Value2', 'WIP-RM']]

    # Append the grouped data to Con_qty
    Con_qty = pd.concat([Con_qty, grouped], ignore_index=True)

    return Con_qty 