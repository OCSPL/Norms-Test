import pandas as pd
from Utils.utils import multiply_with_percentage
import numpy as np

def update_bom_summaries_with_net_qty(stock_summary, bom_summaries_df, stage_name):
    # Sum the NET QTY values for rows in stock_summary where Item Name equals stage_name
    net_qty_value = stock_summary.loc[
        stock_summary['Item Name'] == stage_name, 'NET QTY'
    ].sum()

    # Update the RM WIP QTY in bom_summaries_df for the row where Stage Name and Name match stage_name
    bom_summaries_df.loc[
        (bom_summaries_df['Stage Name'] == stage_name) &
        (bom_summaries_df['Name'] == stage_name),
        'RM WIP QTY'
    ] = net_qty_value

    # Extract and convert the Quantity to a numeric value for the specific item
    Quantity_uniq = pd.to_numeric(
        bom_summaries_df.loc[
            (bom_summaries_df['Stage Name'] == stage_name) &
            (bom_summaries_df['Name'] == stage_name),
            'Quantity'
        ].values[0]
    )

    # For rows where Highlight is False, update RM WIP QTY using a proportional calculation
    bom_summaries_df.loc[
        (bom_summaries_df['Stage Name'] == stage_name) &
        (bom_summaries_df['Highlight'] == False),
        'RM WIP QTY'
    ] = bom_summaries_df.apply(
        lambda row: (row['BOMQty'] / Quantity_uniq) * net_qty_value 
                    if row['Highlight'] == False else row['RM WIP QTY'],
        axis=1
    )
    
    return bom_summaries_df

def calculate_dmpm(stock_summary, bom_summaries_df):
    
    # --- Process "PICK-I-A (STAGE-I)" ---
    # From BOM summary, locate the row where both Stage Name and Name equal "PICK-I-A (STAGE-I)"
    # and extract its RM WIP QTY value.
    try:
        additional_qty_for_pick1 = bom_summaries_df.loc[
            (bom_summaries_df['Stage Name'] == 'DMPM CRUDE') &
            (bom_summaries_df['Name'] == 'CP-AMINE'),
            'RM WIP QTY'
        ].iloc[0]
    except IndexError:
        additional_qty_for_pick1 = 0

    # Update the "ADDITIONAL QTY CONSUMED IN OTHER WIP" for the "PICK-I-A (STAGE-I)" row in stock_summary
    stock_summary.loc[
        stock_summary['Item Name'] == 'CP-AMINE',
        'ADDITIONAL QTY CONSUMED IN OTHER WIP'
    ] = additional_qty_for_pick1

    # Recalculate NET QTY for each row in stock_summary
    net_qtys = []
    for _, row in stock_summary.iterrows():
        net_qty = multiply_with_percentage(row['Closing'], row['Closing_activation'])
        net_qty += multiply_with_percentage(
            row['Quantities Consumed from Opening Stock'],
            row['Quantities Consumed from Opening Stock_activation']
        )
        net_qty += multiply_with_percentage(
            row['ADDITIONAL QTY CONSUMED IN OTHER WIP'],
            row['ADDITIONAL QTY CONSUMED IN OTHER WIP_activation']
        )
        net_qtys.append(round(net_qty, 2))
    stock_summary['NET QTY'] = net_qtys

    # Recalculate Additional_QTY_mult for every row using the updated additional quantity value
    stock_summary['Additional_QTY_mult'] = stock_summary.apply(
        lambda row: round(
            multiply_with_percentage(
                row['ADDITIONAL QTY CONSUMED IN OTHER WIP'],
                row['ADDITIONAL QTY CONSUMED IN OTHER WIP_activation']
            ),
            2
        ),
        axis=1
    )

    # Finally, update the BOM summary for "PICK-I-A (STAGE-I)" using the updated NET QTY values
    bom_summaries_df = update_bom_summaries_with_net_qty(stock_summary, bom_summaries_df, 'CP-AMINE')
    
    return stock_summary, bom_summaries_df


def calculate_dmpm_con(Con_qty, job_work_df_filtered):
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





