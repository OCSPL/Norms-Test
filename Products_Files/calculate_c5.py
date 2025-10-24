import pandas as pd
import numpy as np

def calculate_c5(con_qty: pd.DataFrame, job_work_df_filtered: pd.DataFrame) -> pd.DataFrame:

    # 1) Group job work by item
    grouped = (
        job_work_df_filtered
        .groupby('Consume_Item_Name', as_index=False)
        .agg(Consume_Quantity=('Consume_Quantity', 'sum'),
             Consume_Value=('Consume_Value', 'sum'))
    )

    # 2) Merge onto the incoming con_qty (left join keeps all base rows)
    out = con_qty.merge(grouped, on='Consume_Item_Name', how='left')

    # 3) Fill missing consumption values (cases where no job work rows exist for an item)
    out['Consume_Quantity'] = out['Consume_Quantity'].fillna(0)
    out['Consume_Value']    = out['Consume_Value'].fillna(0)

    # 4) Ensure base columns exist and are numeric
    out['Net_Qty'] = pd.to_numeric(out['Net_Qty'], errors='coerce').fillna(0)
    out['Value2']  = pd.to_numeric(out['Value2'], errors='coerce').fillna(0)

    # 5) Add grouped consumption to the base columns
    out['Net_Qty'] = out['Net_Qty'] + out['Consume_Quantity']
    out['Value2']  = out['Value2']  + out['Consume_Value']

    # 6) Recompute Rate, avoiding divide-by-zero
    out['Rate'] = np.divide(out['Value2'], out['Net_Qty'],
                            out=np.zeros(len(out), dtype=float),
                            where=out['Net_Qty'] != 0)

    return out
