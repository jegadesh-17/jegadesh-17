import os
import logging
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
import json
import xml.etree.ElementTree as ET
import csv

app = Flask(__name__)
app.secret_key = 'your_secret_key'

# Define and create the upload directory
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Configure logging
logging.basicConfig(
    filename='C:\\onetoone\\test.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def connect_snowflake():
    """Establish a connection to Snowflake."""
    conn_params = {
        'user': 'JEGADESH',
        'password': 'Welcome$123',
        'account': 'dv67647.ap-south-1',
        'warehouse': 'COMPUTE_WH',
        'role': 'ACCOUNTADMIN',
        'database': 'DEMO444',
        'schema': 'PUBLIC'
    }
    return connect(**conn_params)

def fetch_table_columns(table_name):
    """Fetch column names from a specified table in Snowflake."""
    conn = connect_snowflake()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DESCRIBE TABLE {table_name}")
            columns = cur.fetchall()
            return [col[0] for col in columns]
    finally:
        conn.close()

def list_tables_in_database():
    """List tables in the default Snowflake database and schema."""
    conn = connect_snowflake()
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW TABLES")
            tables = cur.fetchall()
            return [table[1] for table in tables]
    finally:
        conn.close()

def write_to_snowflake(df, table_name, save_mode='append'):
    """Write DataFrame to Snowflake table."""
    conn = connect_snowflake()
    
    try:
        logging.info(df)

        # Write DataFrame to Snowflake
        write_pandas(conn, df, table_name, auto_create_table=True, mode=save_mode)
        logging.info(f"Data successfully written to Snowflake table: {table_name} with mode: {save_mode}")
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        raise
    finally:
        conn.close()

@app.route('/', methods=['GET'])
def index():
    tables = list_tables_in_database()
    return render_template('index.html', tables=tables)

@app.route('/process', methods=['POST'])
def process_file():
    create_or_select = request.form['create_or_select']
    file = request.files['file']
    file_type = request.form['file_type']

    # Save file to the secure location
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(file_path)

    try:
        if file_type == 'csv':
            df = pd.read_csv(file_path)
        elif file_type == 'json':
            df = pd.read_json(file_path)
        elif file_type == 'xml':
            # Convert XML to CSV
            csv_file_path = os.path.join(app.config['UPLOAD_FOLDER'], 'output.csv')
            tree = ET.parse(file_path)
            root = tree.getroot()

            # Extract headers
            headers = set()
            for elem in root.iter():
                for child in elem:
                    if child.text and child.text.strip():
                        headers.add(child.tag)
            headers = sorted(headers)

            # Write to CSV
            with open(csv_file_path, 'w', newline='') as csvfile:
                csvwriter = csv.DictWriter(csvfile, fieldnames=headers)
                csvwriter.writeheader()
                for elem in root.findall('./*'):
                    row_data = {}
                    for header in headers:
                        child = elem.find(header)
                        if child is not None and child.text and child.text.strip():
                            row_data[header] = child.text.strip()
                        else:
                            row_data[header] = ''
                    if any(row_data.values()):
                        csvwriter.writerow(row_data)
            logging.info("XML data successfully converted to CSV.")

            # Load the CSV into a DataFrame
            df = pd.read_csv(csv_file_path)
        elif file_type == 'txt':
            # Existing text file processing logic...
            pass
        else:
            flash('Unsupported file type.')
            return redirect(url_for('index'))

        logging.info(f"DataFrame from file:\n{df.head()}")
        df.fillna("", inplace=True)

        if create_or_select == 'new':
            table_name = request.form['new_table_name']
            conn = connect_snowflake()
            try:
                with conn.cursor() as cur:
                    create_table_query = f"CREATE TABLE {table_name.upper()} ("
                    for column in df.columns:
                        create_table_query += f'"{column.upper()}" STRING, '
                    create_table_query = create_table_query.rstrip(', ') + ')'
                    cur.execute(create_table_query)
                    logging.info(f"Table {table_name.upper()} created successfully.")
                flash('Table created successfully!')
                return redirect(url_for('index'))
            except Exception as e:
                logging.error(f"Error creating table: {e}")
                flash('Error creating new table.')
                return redirect(url_for('index'))
            finally:
                conn.close()
        else:
            table_name = request.form['table_name']
            table_columns = fetch_table_columns(table_name)

            return render_template('map_columns.html', 
                                   df_columns=df.columns, 
                                   table_columns=table_columns,
                                   df_data=json.dumps(df.to_dict(orient='records')), 
                                   table_name=table_name)

    except Exception as e:
        logging.error(f"Error loading file: {e}")
        flash('Error loading file.')
        return redirect(url_for('index'))

@app.route('/map_columns', methods=['POST'])
def map_columns():
    table_name = request.form['table_name']
    df_data = request.form['df_data']
    
    # Get selected columns
    selected_table_columns = request.form.getlist('table_columns')
    selected_input_columns = request.form.getlist('input_columns')

    # Convert df_data back to DataFrame using json.loads
    df = pd.DataFrame(json.loads(df_data))

    # Basic validation: Check if the number of selected columns matches
    if len(selected_table_columns) != len(selected_input_columns):
        flash('Please ensure you map all selected columns correctly.', 'error')
        return render_template('map_columns.html', 
                               df_columns=df.columns, 
                               table_columns=fetch_table_columns(table_name),
                               df_data=df_data, 
                               table_name=table_name)

    # Create a mapping of input columns to table columns
    column_mapping = dict(zip(selected_input_columns, selected_table_columns))

    try:
        logging.info('running')
        logging.info(df)
        # Write the mapped data to Snowflake
        write_to_snowflake(df, table_name, save_mode='append')
        logging.info('completed')
    except Exception as e:
        flash(f'Error occurred while writing data: {e}', 'error')
        return render_template('map_columns.html', 
                               df_columns=df.columns, 
                               table_columns=fetch_table_columns(table_name),
                               df_data=df_data, 
                               table_name=table_name)

    return render_template('show_mapped_data.html', 
                           table_columns=selected_table_columns, 
                           data=df[selected_input_columns].values.tolist())

@app.route('/view_all_data', methods=['POST', 'GET'])
def view_all_data():
    if request.method == 'POST':
        table_name = request.form.get('table_name')
        
        if not table_name:
            flash('No table selected. Please select a table.')
            return redirect(url_for('view_all_data'))

        # Fetch all data from the selected table
        conn = connect_snowflake()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {table_name}")
                data = cur.fetchall()
                columns = [desc[0] for desc in cur.description]  # Get column names
        except Exception as e:
            logging.error(f"Error fetching data from table {table_name}: {e}")
            flash(f"Error fetching data from table {table_name}.")
            return redirect(url_for('view_all_data'))
        finally:
            conn.close()

        return render_template('view_table_data.html', table_columns=columns, data=data, table_name=table_name)

    # Handle GET request to display the dropdown of table names
    tables = list_tables_in_database()
    return render_template('select_table.html', tables=tables)

@app.route('/view_table_data', methods=['POST'])
def view_table_data():
    table_name = request.form.get('table_name')
    
    if not table_name:
        flash('No table selected. Please select a table.')
        return redirect(url_for('view_all_data'))

    # Fetch all data from the selected table
    conn = connect_snowflake()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table_name}")
            data = cur.fetchall()
            columns = [desc[0] for desc in cur.description]  # Get column names
    except Exception as e:
        logging.error(f"Error fetching data from table {table_name}: {e}")
        flash(f"Error fetching data from table {table_name}.")
        return redirect(url_for('view_all_data'))
    finally:
        conn.close()

    return render_template('view_table_data.html', table_columns=columns, data=data, table_name=table_name)

if __name__ == '__main__':
    app.run(debug=True)
