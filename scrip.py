import os
import re
import sys
from datetime import datetime

import pandas as pd
from Config import engine_norms, engine_eres
from Main import process_data

# Define the fg_names list
fg_names = [    
    '2,3 DI CHLORO PYRIDINE',
    'N,N DI ISO PROPYL ETHYL AMINE',   
    '2,5 DIMETHYL PHENYL ACETYL CHLORIDE',   
    'AMIDO CHLORIDE',
    '2-METHOXY BENZOIC ACID',  
    '2,4 DICHLORO BENZOYL CHLORIDE',
    'C-5 HYDROXY ESTER',
    '4-HYDROXY-3-(2,4,6-TRIMETHYLPHENYL)-1-OXASPIRO[4.4]NON-3-EN-2-ONE',   
    'DMA-CHLORIDE LAN',   
    '2,6 DIMETHOXY BENZOIC ACID',
    'METHYL-2-CHLORO PROPIONATE',
    '2,4,6 TRIMETHYL PHENYL ACETYL CHLORIDE',
    # '4-FLUORO-3-TRIFLUOROMETHYL PHENOL',
    '2,4 DICHLORO BENZALDEHYDE',
    '2,6 DICHLORO BENZOYL CHLORIDE',
    'METCAMIFEN TECH.',
    'DICHLORO ACETIC ACID',
]

def sanitize_filename(name):
    """
    Sanitize the fg_name to create a safe filename.
    Removes or replaces characters that are invalid in filenames.
    """
    # Replace spaces and commas with underscores
    name = name.replace(' ', '_').replace(',', '_')
    # Remove any characters that are not alphanumeric, underscores, or hyphens
    name = re.sub(r'[^\w\-]', '', name)
    return name

def main():
    # Define date ranges
    # You can modify these dates or make them dynamic as needed
    # For example, using the last month
    # end_date = datetime.now()
    # start_date = end_date.replace(month=end_date.month - 1) if end_date.month > 1 else end_date.replace(year=end_date.year -1, month=12)
    
    # Alternatively, set specific dates
    start_date = datetime(2024, 4, 1)
    end_date = datetime(2024, 12, 31)
    
    # Format dates as strings
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    # Create Output directory if it doesn't exist
    output_dir = 'Output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Iterate through each fg_name
    for fg_name in fg_names:
        try:
            print(f"Processing FG Name: {fg_name}")
            
            # Call process_data with the specified dates and fg_name
            data = process_data(start_date_str, end_date_str, fg_name)
            
            # Extract Con_qty DataFrame
            Con_qty = data.get('Con_qty')
            if Con_qty is None:
                print(f"Warning: 'Con_qty' not found for FG Name: {fg_name}. Skipping.")
                continue
            
            # Optionally, you can perform any data cleaning here
            # For example, ensuring Con_qty is a DataFrame
            if not isinstance(Con_qty, pd.DataFrame):
                print(f"Warning: 'Con_qty' is not a DataFrame for FG Name: {fg_name}. Skipping.")
                continue
            
            # Sanitize fg_name for filename
            safe_fg_name = sanitize_filename(fg_name)
            excel_filename = f"{safe_fg_name}.xlsx"
            excel_path = os.path.join(output_dir, excel_filename)
            
            # Save Con_qty to Excel
            Con_qty.to_excel(excel_path, index=False)
            print(f"Saved Con_qty to {excel_path}")
        
        except Exception as e:
            print(f"Error processing FG Name '{fg_name}': {e}")
    
    print("Processing complete.")

if __name__ == '__main__':
    main()
