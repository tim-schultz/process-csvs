import pandas as pd
from eth_utils.address import to_checksum_address

def apply_checksum(df):
    """
    Apply to_checksum_address to the first column of the DataFrame.
    
    Args:
    df (pd.DataFrame): Input DataFrame
    
    Returns:
    pd.DataFrame: DataFrame with checksum addresses in the first column
    """
    try:
        first_col = df.columns[0]
        df[first_col] = df[first_col].apply(lambda x: to_checksum_address(x) if pd.notna(x) else x)
        return df
    except Exception as e:
        raise ValueError(f"Error applying checksum: {e}")

def compare_csv_files(file1_path, file2_path, output_path):
    """
    Compare two CSV files and return non-overlapping values from the first column.
    
    Args:
    file1_path (str): Path to the first CSV file
    file2_path (str): Path to the second CSV file
    output_path (str): Path to save the output CSV file
    
    Returns:
    None
    """
    try:
        # Read CSV files into DataFrames
        df1 = pd.read_csv(file1_path)
        df2 = pd.read_csv(file2_path)
        
        
        # Check if DataFrames are empty
        if df1.empty or df2.empty:
            raise ValueError("One or both input CSV files are empty")
        
        # Apply checksum to the first column of both DataFrames
        df1 = apply_checksum(df1)
        df2 = apply_checksum(df2)
        
        # Extract the first column from both DataFrames
        col1 = df1.iloc[:, 0]
        col2 = df2.iloc[:, 0]
        
        # Find non-overlapping values
        non_overlapping = pd.concat([col1, col2]).drop_duplicates(keep=False)
        
        # Create a new DataFrame with non-overlapping values
        result_df = pd.DataFrame(non_overlapping)
        
        # Save the result to a new CSV file
        result_df.to_csv(output_path, index=False)
        
        print(f"Non-overlapping values saved to {output_path}")
    
    except FileNotFoundError as e:
        print(f"Error: File not found - {e}")
    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Example usage with placeholder file paths
    input_file1 = "carv-55-percent-processed.csv"
    input_file2 = "CARV-batch_400k.csv"
    output_file = "unprocessed.csv"
    
    compare_csv_files(input_file1, input_file2, output_file)