import pandas as pd
from Utils.sql_queries import get_bom_query

def fetch_bom_details(stage_name, stock_summary, engine_eres, fg_name):
    # Dynamically fetch BOM details for the given stage
    bom_query = get_bom_query()
    with engine_eres.connect() as connection:
        bom_df = pd.read_sql_query(bom_query, connection)
        bom_df = bom_df[bom_df['BOMQty'] != 0]
        bom_wip = bom_df.copy()
        
    bom_names = {
        'DIPEA CRUDE': ['DIPEA CRUDE-MAX'],
        '2,4,6 TMBCL (STAGE-I) ORGANIC LAYER': ['2,4,6 TMBCL ST-I ORG.LAYER BR-102'],
        '2,4,6 TMBCL STAGE-I': ['2,4,6 TMBCL ST-I AR-106'],
        '2,4,6 TMBCN STAGE-II':['2,4,6 TMBCN ST-II AR-102'],
        '2,4,6 TMBCN (STAGE-II) WET CAKE':['2,4,6 TMBCN ST-II WET CAKE AR-101'],
        '2,4,6 TMPACL (STAGE-III) DRY POWDER':['2,4,6 TMPACL ST-III AR-105'],
        '2,4,6 TMPACL (STAGE-IV) CRUDE':['2,4,6 TMPACL ST-IV AR-102'],
        '2,4,6 TMPACL (STAGE-III) DRY POWDER':['2,4,6 TMPACL ST-III AR-105'],
        '2,5 DMBCL (STAGE-I) CRUDE':['2,5 DMBCL ST-I CRUDE RE-2511'],
        '2,5 DMBCL STAGE-I':['2,5 DMBCL ST-I RE-1508'],
        '2,5 DMBCN (STAGE-II) CRUDE':['2,5 DMBCL ST-I  CRUDE BR-101'],
        '2,5 DMBCN STAGE-II':['2,5 DMBCN ST-II CR-101 CRUDE'],
        '2,5 DMPAA (STAGE-III) DRY POWDER':['2,5 DMPAA ST-III RE-3507'],
        '2,5 DMPAC (STAGE-IV) CRUDE':['2,5 DMPAC ST-IV BR-109'],
        'METCAMIFEN SAM-I WET CAKE':['26000235 - ST-I - WET CAKE BR-107'],
        'METCAMIFEN SAM-II DRY POWDER':['26000236 - ST-II - DRY POWDER'],
        'C-5 HYDROXY ESTER CRUDE':['26000097-C-5 EASTER CRUDE BR-111'],
        'SPIR STAGE-I':['SPIR (ST-I) DRE-1301'],
        'SPIR (STAGE-II) WET CAKE':['26000210-SPIR (ST-II) WET CAKE DRE-1302'],
        'OCDB ORGANIC LAYER':['OCDB ORGANIC LAYER (RE-2503)'],
        '2,4,6 TMPACL (STAGE-III) WET POWDER':['2,4,6 TMPACL ST-III WP AR-108'],
        '2,4 DCBC CRUDE':['2,4 DCBC CRUDE-MAX'],
        '2-MBA WET CAKE':['2-MBA WET CAKE BR-102'],
        '2-CHLORO PROPIONIC ACID-M2CP STAGE-I':['26000123 - DRE-1302 - M2CP STAGE-I'],
        'M2CP CRUDE':['26000119 - DRE-1302 - M2CP CRUDE']
    }

    # Apply filtering based on the stage name
    if stage_name in bom_names:
        bom_name = bom_names[stage_name][0]  # Get the corresponding BOM name

        if stage_name == 'SPIR STAGE-I':
             # For 'SPIR STAGE-I', filter bom_df to include only 'Semi Finished Good' type
            bom_df = bom_df[
                (bom_df['ItemName'] == stage_name) & 
                (bom_df['BOMName'] == bom_name) & 
                (bom_df['Type'] == 'Semi Finished Good')
            ]
        elif stage_name == '2-CHLORO PROPIONIC ACID-M2CP STAGE-I':
            # For 'SPIR STAGE-I', filter bom_df to include only 'Semi Finished Good' type
            bom_df = bom_df[
                (bom_df['ItemName'] == stage_name) & 
                (bom_df['BOMName'] == bom_name) & 
                (bom_df['Type'].isin(['Key Raw Material', 'Raw Material','Semi Finished Good']))
            ]
        else:           
            # Filter bom_df to include only 'Key Raw Material' and 'Raw Material' types
            bom_df = bom_df[
                (bom_df['ItemName'] == stage_name) & 
                (bom_df['BOMName'] == bom_name) & 
                (bom_df['Type'].isin(['Key Raw Material', 'Raw Material']))
            ]
            
            # Define the filter condition for bom_wip based on ItemName and BOMName
            filter_condition = (
                (bom_wip['ItemName'] == stage_name) &
                (bom_wip['BOMName'] == bom_name)
            )
            
            # Apply the same filtering condition to bom_wip and include only 'Work in Progress' types
            bom_wip = bom_wip[
                filter_condition & bom_wip['Type'].isin(['Work in Progress'])
            ]
            
            # Apply specific filtering logic to bom_wip based on the stage_name
            if stage_name == '2,4,6 TMBCL STAGE-I':
                bom_wip = bom_wip[bom_wip['Name'] == '2,4,6 TMBCL (STAGE-I) ORGANIC LAYER']
            elif stage_name == '2,4,6 TMBCN STAGE-II':
                bom_wip = bom_wip[bom_wip['Name'] == '2,4,6 TMBCL STAGE-I']
            elif stage_name == '2,4,6 TMBCN (STAGE-II) WET CAKE':
                bom_wip = bom_wip[bom_wip['Name'] == '2,4,6 TMBCL STAGE-I']
            elif stage_name == '2,4,6 TMPACL (STAGE-III) DRY POWDER':
                bom_wip = bom_wip[bom_wip['Name'] == '2,4,6 TMBCN STAGE-II']
            elif stage_name == '2,4,6 TMPACL (STAGE-IV) CRUDE':
                bom_wip = bom_wip[bom_wip['Name'] == '2,4,6 TMPACL (STAGE-III) DRY POWDER']
            
            # Concatenate the filtered bom_wip into bom_df
            bom_df = pd.concat([bom_df, bom_wip], ignore_index=True)    
    
    # Continue with the existing calculations
    bom_df['Standard'] = (bom_df['BOMQty'] / bom_df['Quantity']).round(4)
    bom_df['Standardm'] = (bom_df['BOMQty'] / bom_df['Quantity'])

    # Extract the unique values from 'Quantity' column
    unique_quantity = bom_df['Quantity'].unique()
    unique_quantity_str = ', '.join(map(str, unique_quantity))

    # Calculate 'RM WIP QTY' for each row by multiplying 'Standard' with the NET QTY
    net_qty = stock_summary.loc[stock_summary['Item Name'] == stage_name, 'NET QTY'].values[0]
    
    bom_df['RM WIP QTY'] = (bom_df['Standardm'] * net_qty).round(2)

    bom_summary = bom_df[['Name', 'Quantity', 'BOMQty', 'RM WIP QTY']]
    bom_summary.loc[:, 'Quantity'] = ''

    new_row = pd.DataFrame([{
                'Name': stage_name,
                'Quantity': unique_quantity_str,
                'BOMQty': '',
                'RM WIP QTY': net_qty,
                'Highlight': True
            }])

    # Append the new row to the BOM summary
    bom_summary = pd.concat([bom_summary, new_row], ignore_index=True)
    bom_summary['Highlight'] = bom_summary['Highlight'].fillna(False)
    
    return bom_summary
