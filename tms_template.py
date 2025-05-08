import polars as pl
import pandas as pd
from collections import defaultdict
import re

# Read the Excel file
file_path = r"/home/kingchow/PythonProjects/TMS-Project/TMS-Daily/shifts-Global-2025-05 (1).xlsx"

# Read with headers in row 9 (after skipping 8 rows)
df = pl.read_excel(
    file_path,
    read_options={"skip_rows": 8, "header_row": 0}
)

# Clean data
df = df.drop_nulls()  # Drop any rows with null values
df = df.drop(df.columns[0]).drop(df.columns[2])  # Drop first and third columns
print("Original structure:")
print(df.head(5))

def process_column_data(col_data):
    """Process individual cell data:
    1. Remove 'Europe/Gera'
    2. Split at first number"""
    if col_data is None:
        return None
    
    # Convert to string if not already
    text = str(col_data)
    
    # Remove 'Europe/Gera' if present
    processed = text.replace("Europe/Gera", "")
    
    # Find first number in string
    # match = re.search(r"\d+", processed)
    # if match:
    #     # Split at first number (keep number and everything after)
    #     processed = processed[match.start():]
    return processed.strip()

def create_processed_column_sheets(df, output_path):
    first_col = df.columns[0]
    name_counts = defaultdict(int)
    
    with pd.ExcelWriter(output_path) as writer:
        for col in df.columns[1:]:
            # Select first column and current column
            sheet_df = df.select([first_col, col])
            
            # Filter out rows where second column is "r", "rw", or "u"
            sheet_df = sheet_df.filter(
                ~pl.col(col).is_in(["r", "rw", "u"])
            )
            
            # Process each cell in the second column with explicit return type
            sheet_df = sheet_df.with_columns(
                pl.col(col).map_elements(
                    process_column_data,
                    return_dtype=pl.Utf8,  # Explicitly set return type as string
                    skip_nulls=False       # Process nulls explicitly
                ).alias(col)
            )
            
            # Remove any resulting empty strings or nulls
            sheet_df = sheet_df.filter(
                pl.col(col).is_not_null() & (pl.col(col) != "")
            )
            # Handle duplicate sheet names
            name_counts[col] += 1
            sheet_name = f"{col}_{name_counts[col]}" if name_counts[col] > 1 else col
            
            # Write to Excel sheet
            sheet_df.to_pandas().to_excel(
                writer,
                sheet_name=sheet_name[:31],
                index=False
            )
            
            print(f"Processed sheet '{sheet_name}': {len(sheet_df)} rows")

# Create processed output
output_path = "processed_columns_shifts.xlsx"
create_processed_column_sheets(df, output_path)

print(f"\nFinal processed file saved: {output_path}")
