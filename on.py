import pandas as pd

# Read the text file
text_file_path = 'C:\\Users\\SSLTP11406\\Downloads\\sample_comma_separated.txt'  # Replace with your text file path
delimiter = ','  # Replace with the delimiter used in your text file (e.g., ',', '\t', etc.)

# Convert text file to a DataFrame
df = pd.read_csv(text_file_path, delimiter=delimiter)
print(df)

# Save DataFrame to Excel
excel_file_path = 'C:\\Users\\SSLTP11406\\Downloads\\output.xlsx'  # Replace with your desired output Excel file path
# df.to_excel(excel_file_path, index=False)

print(f"Text file converted to Excel and saved as {excel_file_path}")
