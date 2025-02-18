import pandas as pd

def get_df_from_tuple_list(tl):
    """Convert a list of tuples into a pandas DataFrame."""
    return pd.DataFrame(tl, columns=["date", "symbol", "close", "aclose"])


def split_adj_vectorized(df):

    # Calculate split adjustment factors
    split_adjustments = (df['aclose'] / df['close']).cumprod()

    # Add a column for the split adjustment factor
    df['split_adjustment'] = split_adjustments
    print(df['split_adjustment'])
    # Calculate adjusted close prices using split adjustment factors
    df['adj_close'] = df['close'] * split_adjustments


    return df[['date', 'symbol', 'adj_close']]

# Example usage remains the same


def split_adj_vectorized_multiple_symbols(df):
    def adjust_group(group):
        group = group.sort_values(by='date', ascending=False)
        group = group.dropna(subset=['close', 'aclose'])
        split_adjustments = (group['aclose'] / group['close']).cumprod()
        group['split_adjustment'] = split_adjustments
        group['adj_close'] = group['close'] * split_adjustments
        return group

    df = df.groupby('symbol', group_keys=False).apply(adjust_group)
    return df[['date', 'symbol', 'adj_close']]
