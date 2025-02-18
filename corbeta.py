import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import split_div_adjustment as sda
from database_utils import get_db_connection, fetch_all_symbols_that_traded_last_number_of_days, fetch_selected_symbols_data

def fetch_and_prepare_data():
    conn = get_db_connection()
    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=365)
    last_week_traded_symbols = [row[0] for row in fetch_all_symbols_that_traded_last_number_of_days(conn, end_date, 7)]
    symbols_data = fetch_selected_symbols_data(conn, last_week_traded_symbols, start_date, end_date)
    conn.close()
    df = sda.get_df_from_tuple_list(symbols_data)
    return df

def clean_data(df):
    return df[(df['close'] != 0) & df['aclose'].notna() & df['close'].notna()]

def adjust_for_splits_and_dividends(df):
    return sda.split_adj_vectorized_multiple_symbols(df)

def create_time_series_pivot(adjusted_df):
    return adjusted_df.pivot_table(index='date', columns='symbol', values='adj_close')


def calculate_spurious_correlations(pvt, etf_symbols):
    all_correlations = pd.DataFrame(index=pvt.columns)

    # Use raw prices instead of returns
    cleaned_prices = pvt.fillna(0).replace([np.inf, -np.inf], 0)

    # Drop stocks where all price values are constant (std dev = 0)
    valid_columns = cleaned_prices.std() != 0
    cleaned_prices = cleaned_prices.loc[:, valid_columns]

    # Compute Pearson correlation matrix
    correlation_matrix = cleaned_prices.corr(method="pearson")

    # Extract correlations only for the given ETF symbols
    all_correlations = correlation_matrix[etf_symbols].round(2)

    return all_correlations


def calculate_correlations_1(pvt, etf_symbols):
    all_correlations = pd.DataFrame(index=pvt.columns)

    raw_returns = pvt.fillna(0)
    cleaned_returns = raw_returns.fillna(0).replace([np.inf, -np.inf], 0)

    # Drop columns where standard deviation is zero
    valid_columns = cleaned_returns.std() != 0
    cleaned_returns = cleaned_returns.loc[:, valid_columns]

    etf_returns = cleaned_returns[etf_symbols]

    for etf_symbol in etf_symbols:
        etf_series = etf_returns[etf_symbol]
        all_correlations[etf_symbol] = cleaned_returns.corrwith(etf_series, axis=0).round(2)

    return all_correlations

def calculate_correlations(pvt, etf_symbols):
    all_correlations = pd.DataFrame(index=pvt.columns)

    raw_returns = pvt.pct_change(fill_method=None)
    cleaned_returns = raw_returns.fillna(0).replace([np.inf, -np.inf], 0)

    # Drop columns where standard deviation is zero
    valid_columns = cleaned_returns.std() != 0
    cleaned_returns = cleaned_returns.loc[:, valid_columns]

    etf_returns = cleaned_returns[etf_symbols]

    for etf_symbol in etf_symbols:
        etf_series = etf_returns[etf_symbol]
        all_correlations[etf_symbol] = cleaned_returns.corrwith(etf_series, axis=0).round(2)

    return all_correlations



def calculate_beta(pvt, etf_symbols):
    all_betas = pd.DataFrame(index=pvt.columns)

    raw_returns = pvt.pct_change(fill_method=None)

    cleaned_returns = raw_returns.fillna(0)
    etf_returns = cleaned_returns[etf_symbols]

    cleaned_returns = cleaned_returns.replace([np.inf, -np.inf], 0)

    for etf_symbol in etf_symbols:
        start_time = time.time()
        etf_series = etf_returns[etf_symbol]
        beta_values = cleaned_returns.apply(lambda stock_series: stock_series.cov(etf_series) / etf_series.var(), axis=0)
        all_betas[etf_symbol] = beta_values.round(2)

    return all_betas



def save_correlation_matrix_to_tag_file(correlation_matrix, output_filename, prefix='COR_'):
    """
    Converts a correlation matrix into a long-format tab-delimited file with additional date columns.

    Parameters:
        correlation_matrix (pd.DataFrame): DataFrame where rows are stocks and columns are ETFs.
        output_filename (str): Path to save the file.
    """
    # Ensure the index has a unique name
    correlation_matrix.index.name = "Stock"

    # Convert matrix to long format (Stock, ETF, Correlation)
    correlation_long = correlation_matrix.stack().reset_index()
    correlation_long.columns = ["Stock", "ETF", "Correlation"]

    # Get today's date and next week's date
    today = datetime.today().strftime("%d-%m-%Y")
    next_week = (datetime.today() + timedelta(days=7)).strftime("%d-%m-%Y")

    # Add date columns and format ETF column
    correlation_long.insert(0, "Today", today)
    correlation_long.insert(1, "Next_Week", next_week)
    correlation_long["ETF"] = prefix + correlation_long["ETF"]

    # Save as tab-delimited file
    correlation_long.to_csv(output_filename, index=False, sep='\t')

    print(f"Saved correlation data to {output_filename}")


def main():
    df = fetch_and_prepare_data()
    cleaned_df = clean_data(df)
    adjusted_df = adjust_for_splits_and_dividends(cleaned_df)
    pvt = create_time_series_pivot(adjusted_df)
    etfs_df = pd.read_csv('etfs.csv')
    etf_symbols = etfs_df['Symbol'].tolist()
    cor1s = calculate_correlations_1(pvt.copy(), etf_symbols)
    cor2s = calculate_correlations(pvt.copy(), etf_symbols)
    betas = calculate_beta(pvt.copy(), etf_symbols)

    save_correlation_matrix_to_tag_file(cor1s, 'cor1.txt')
    save_correlation_matrix_to_tag_file(cor2s, 'cor2.txt', 'COR2_')
    save_correlation_matrix_to_tag_file(betas, 'beta.txt', 'BETA_')


if __name__ == "__main__":
    main()
