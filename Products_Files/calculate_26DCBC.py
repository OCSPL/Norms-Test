import pandas as pd

def calculate_26DCBC(Con_qty,job_work_df,fg_name):
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
    Con_qty = Con_qty.merge(grouped, how='outer', on='Consume_Item_Name')
    
    
    # List of fields to combine
    fields_to_combine = ['Net_Qty', 'Rate', 'Value2']  # Specify the fields to combine

    # Combine values from '_x' and '_y' columns for each field
    for field in fields_to_combine:
        if f"{field}_x" in Con_qty.columns and f"{field}_y" in Con_qty.columns:
            Con_qty[field] = Con_qty[f"{field}_x"].fillna(0) + Con_qty[f"{field}_y"].fillna(0)
        elif f"{field}_x" in Con_qty.columns:
            Con_qty[field] = Con_qty[f"{field}_x"].fillna(0)  # Only _x exists
        elif f"{field}_y" in Con_qty.columns:
            Con_qty[field] = Con_qty[f"{field}_y"].fillna(0)  # Only _y exists

    # Drop intermediate columns (_x and _y)
    columns_to_drop = [f"{field}_x" for field in fields_to_combine if f"{field}_x" in Con_qty.columns] + \
                    [f"{field}_y" for field in fields_to_combine if f"{field}_y" in Con_qty.columns]
    Con_qty = Con_qty.drop(columns=columns_to_drop, errors='ignore')
    Con_qty['Rate']=Con_qty['Value2'] / Con_qty['Net_Qty']

    return Con_qty 