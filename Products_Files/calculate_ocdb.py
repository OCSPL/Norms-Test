import pandas as pd

def calculate_ocdb(Con_qty, stock_summary):

        # Filter the stock_summary DataFrame for '2,4,6 RECOVERED MESITYLENE'
        DMA_row_1 = stock_summary[stock_summary['Item Name'] == 'OCDB RECOVERED TOLUENE']
        DMA_row_2 = stock_summary[stock_summary['Item Name'] == 'OCDB TOLUENE MLR']

        # Extract the 'NET QTY' value and store it in a variable
        mesitylene_net_qty = (DMA_row_1['NET QTY'].values[0] if not DMA_row_1.empty else None)+(DMA_row_2['NET QTY'].values[0] if not DMA_row_2.empty else None)
       
        # Filter the Con_qty DataFrame for 'MESITYLENE (M)'
        mesitylene_con_qty_row = Con_qty[Con_qty['Consume_Item_Name'] == 'TOLUENE (M)']

        # Add the mesitylene_net_qty value to 'WIP-RM' in the filtered row
        if not mesitylene_con_qty_row.empty:
            Con_qty.loc[mesitylene_con_qty_row.index, 'WIP-RM'] += mesitylene_net_qty

        return Con_qty