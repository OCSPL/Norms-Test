import pandas as pd

# Function to convert percentage to decimal
def percentage_to_decimal(perc):
    if pd.isna(perc) or not isinstance(perc, str):
        return None
    return float(perc.rstrip('%')) / 100

# Function to multiply value with percentage
def multiply_with_percentage(value, perc):
    if value == '':
        value = 0
    perc = percentage_to_decimal(perc)
    if perc is not None:
        return float(value) * perc
    return 0
