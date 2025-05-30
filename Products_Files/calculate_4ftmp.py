import pandas as pd
import numpy as np
from Utils.utils import multiply_with_percentage

def calculate_4ftmp(Con_qty, job_work_df_filtered):
    # 1) filter to only Key Raw Material & Raw Material
    df_filtered = job_work_df_filtered[
        job_work_df_filtered['Consume_Item_Type'].isin([
            'Key Raw Material',
            'Raw Material'
        ])
    ].copy()

    # 2) group by item name, summing quantity & value
    grouped = (
        df_filtered
        .groupby('Consume_Item_Name', as_index=False)
        .agg(
            Consume_Quantity=('Consume_Quantity', 'sum'),
            Consume_Value   =('Consume_Value',    'sum')
        )
    )

    # 3) full outer join with Con_qty
    merged = pd.merge(
        Con_qty,
        grouped,
        on='Consume_Item_Name',
        how='outer'
    )

    # 4) coerce any stringy numbers into floats
    numeric_cols = ['Net_Qty', 'Value2', 'Consume_Quantity', 'Consume_Value', 'Rate']
    for col in numeric_cols:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors='coerce').astype(float)

    # 5) fill NaNs for arithmetic
    merged['Net_Qty'] = merged['Net_Qty'].fillna(0) + merged['Consume_Quantity'].fillna(0)
    merged['Value2']  = merged['Value2'].fillna(0)  + merged['Consume_Value'].fillna(0)

    # 6) fill any blank WIP-RM as zero
    merged['WIP-RM'] = merged.get('WIP-RM', 0)

    # 7) recompute Rate
    merged['Rate'] = merged['Value2'] / merged['Net_Qty'].replace({0: np.nan})

    # 8) drop helper cols and return
    result = merged.drop(columns=['Consume_Quantity', 'Consume_Value'])
    return result
