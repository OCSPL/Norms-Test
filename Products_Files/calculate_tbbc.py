import pandas as pd
from Utils.utils import multiply_with_percentage

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

import pandas as pd

import pandas as pd

def calculate_tbbc(stock_summary: pd.DataFrame, bom_summaries_df: pd.DataFrame):
    df = stock_summary.copy()

    # Normalize names for robust matching
    item_norm = df['Item Name'].astype(str).str.strip().str.upper()

    # Ensure required columns exist
    for col in [
        'Additional_QTY_mult',
        'ADDITIONAL QTY CONSUMED IN OTHER WIP',
        'ADDITIONAL QTY CONSUMED IN OTHER WIP_activation',
        'Closing_mult',
        'Quantities_Consumed_mult',
        'NET QTY',
    ]:
        if col not in df.columns:
            df[col] = 0.0

    # Helper: percent -> ratio (2-decimal precision on percent values)
    def _pct_to_ratio(x):
        if pd.isna(x): return 0.0
        if isinstance(x, str):
            s = x.strip()
            has_pct = '%' in s
            s = s.replace('%', '')
            val = pd.to_numeric(s, errors='coerce')
        else:
            has_pct = False
            val = pd.to_numeric(x, errors='coerce')
        if pd.isna(val): return 0.0
        val = float(val)
        return (round(val, 2) / 100.0) if (has_pct or val > 1) else val

    # -----------------------
    # Stage-II: INTERCUT -> CRUDE
    # -----------------------
    stage2_intercuts = {'TBBC STAGE-II INTERCUT-1', 'TBBC STAGE-II INTERCUT-2'}
    stage2_sum = (
        pd.to_numeric(
            df.loc[item_norm.isin({t.upper() for t in stage2_intercuts}), 'NET QTY'],
            errors='coerce'
        ).fillna(0).sum()
    )
    crude_mask = item_norm.eq('TBBC STAGE-II CRUDE')

    # Raw sum into base column
    df.loc[crude_mask, 'ADDITIONAL QTY CONSUMED IN OTHER WIP'] = float(stage2_sum)
    # Multiplied amount using activation %
    stage2_ratio = df.loc[crude_mask, 'ADDITIONAL QTY CONSUMED IN OTHER WIP_activation'].apply(_pct_to_ratio)
    df.loc[crude_mask, 'Additional_QTY_mult'] = (float(stage2_sum) * stage2_ratio).round(2).values
    # NET QTY = Closing_mult + Quantities_Consumed_mult + Additional_QTY_mult
    comp_cols = ['Closing_mult', 'Quantities_Consumed_mult', 'Additional_QTY_mult']
    df[comp_cols] = df[comp_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
    df.loc[crude_mask, 'NET QTY'] = df.loc[crude_mask, comp_cols].sum(axis=1).round(2).values

    # -----------------------
    # Stage-I: INTERCUT -> STAGE-I
    # -----------------------
    stage1_intercuts = {'TBBC STAGE-I INTERCUT-1', 'TBBC STAGE-I INTERCUT-2'}
    stage1_sum = (
        pd.to_numeric(
            df.loc[item_norm.isin({t.upper() for t in stage1_intercuts}), 'NET QTY'],
            errors='coerce'
        ).fillna(0).sum()
    )
    stage1_mask = item_norm.eq('TBBC STAGE-I')

    # Raw sum into base column
    df.loc[stage1_mask, 'ADDITIONAL QTY CONSUMED IN OTHER WIP'] = float(stage1_sum)
    # Multiplied amount using activation %
    stage1_ratio = df.loc[stage1_mask, 'ADDITIONAL QTY CONSUMED IN OTHER WIP_activation'].apply(_pct_to_ratio)
    df.loc[stage1_mask, 'Additional_QTY_mult'] = (float(stage1_sum) * stage1_ratio).round(2).values
    # NET QTY = Closing_mult + Quantities_Consumed_mult + Additional_QTY_mult
    df.loc[stage1_mask, 'NET QTY'] = df.loc[stage1_mask, comp_cols].sum(axis=1).round(2).values

    return df