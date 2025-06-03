# Import necessary libraries
import pandas as pd
import os


# Define an extract function
def extract(store_sales, extra_data):
    """
    Extracts data from the store_sales and extra_data files.

    Parameters:
    store_sales (str): Path to the store sales CSV file.
    extra_data (str): Path to the extra data parquet file.

    Returns:
    pd.DataFrame: DataFrame containing the extracted data.
    """
    # Read the store sales data
    store_sales_df = pd.read_csv(store_sales)

    # Read the extra data
    extra_data_df = pd.read_parquet(extra_data)

    # Merge the two DataFrames on 'index'
    merged_df = store_sales_df.merge(extra_data_df, on="index")
    return merged_df

# Define paths 
store_sales = "grocery_sales.csv"
extra_data = "extra_data.parquet"

# Call the extract function with file paths
merged_data = extract(store_sales, extra_data)

# Check for missing values
missing_values = merged_data.isnull().sum()

# Create the transform() function with one parameter: "raw_data"
def transform(raw_data): 
    # fill missing numerical values with mean
    raw_data.fillna(
        {
            'CPI': raw_data['CPI'].mean(), 
            'Weekly_Sales': raw_data['Weekly_Sales'].mean(), 
            'Unemployment': raw_data['Unemployment'].mean()
        }, inplace=True
    ) 

    # Convert Date column to date_time_type
    raw_data["Date"] = pd.to_datetime(raw_data["Date"], format = "%Y-%m-%d") 
    
    # Extract Month value from date
    raw_data['Month'] = raw_data['Date'].dt.month 

    # Filter rows where weekly_sales > 10,000
    raw_data = raw_data.loc[raw_data['Weekly_Sales'] > 10000, :] 

    # Filter for required columns 
    raw_data = raw_data.drop(["index", "Temperature", "Fuel_Price", "MarkDown1", "MarkDown2", "MarkDown3", "MarkDown4", "MarkDown5", "Type", "Size", "Date"], axis=1)

    return raw_data

# Call the transform function with the merged data
clean_data = transform(merged_data)

# Create the avg_weekly_sales_per_month function that takes in the cleaned data from the last step
def avg_weekly_sales_per_month(clean_data):
    holiday_sales = clean_data[['Month', 'Weekly_Sales']]

    holiday_sales = holiday_sales.groupby('Month').agg(Avg_Sales = ('Weekly_Sales', 'mean')).reset_index().round(2)
    
    return holiday_sales

# Call the avg_weekly_sales_per_month() function and pass the cleaned DataFrame
agg_data = avg_weekly_sales_per_month(clean_data)

# Create the load() function that takes in the cleaned DataFrame and the aggregated one with the paths where they are going to be stored
def load(full_data, full_data_file_path, agg_data, agg_data_file_path):
    full_data.to_csv(full_data_file_path, index=False)
    agg_data.to_csv(agg_data_file_path, index=False) 

# Call the load() function and pass the cleaned and aggregated DataFrames with their paths    
load(clean_data, 'clean_data.csv', agg_data, 'agg_data.csv')

# Create the validation() function with one parameter: file_path - to check whether the previous function was correctly executed
def validation(file_path):
    file_exists = os.path.exists(file_path) 

    if not file_exists:
        raise Exception (f'There is no file at the path {file_path}')
    
# Call the validation() function and pass first, the cleaned DataFrame path, and then the aggregated DataFrame path
validation('clean_data.csv')
validation('agg_data.csv')



