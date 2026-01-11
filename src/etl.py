import yfinance as yf

def extract_stock_data(ticker, start_date, end_date):
    """
    Extract historical stock data for a given ticker symbol from Yahoo Finance.

    Parameters:
    ticker (str): The stock ticker symbol.
    start_date (str): The start date for the data extraction in 'YYYY-MM-DD' format.
    end_date (str): The end date for the data extraction in 'YYYY-MM-DD' format.

    Returns:
    DataFrame: A pandas DataFrame containing the historical stock data.
    """
    stock_data = yf.download(ticker, start=start_date, end=end_date)
    return stock_data

# Example usage
if __name__ == "__main__":
    ticker = "ITUB4.SA"
    start_date = "2024-01-01"
    end_date = "2024-12-31"
    data = extract_stock_data(ticker, start_date, end_date)
    print(data)
