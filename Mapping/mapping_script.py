# Databricks notebook source
# MAGIC %md
# MAGIC # # Excel to SQL Field Mapping Notebook
# MAGIC # 
# MAGIC ## This notebook provides a complete workflow for:
# MAGIC ## 1. Reading field mappings from Excel files
# MAGIC ## 2. Creating fuzzy mappings for similar field names
# MAGIC ## 3. Processing SQL files to convert SAP field names to DBX field names
# MAGIC ## 4. Creating Databricks-compatible notebooks
# MAGIC # 
# MAGIC # ## Usage Instructions
# MAGIC ### 1. Configure the settings in the "Configuration" section below
# MAGIC ### 2. Run cells sequentially or use the "Run All"
# MAGIC ### 3. Check the output files in your project directory
# MAGIC

# COMMAND ----------

import json
import pandas as pd
import os
import re
import shutil
from typing import Dict, List, Tuple, Optional

print("All required libraries imported successfully!")


# COMMAND ----------

# DBTITLE 1,Configs
# Configuration Settings
CONFIG = {
    # File paths
    'excel_file': 'table.xlsx',
    'sql_input_file': 'converted_databricks_sql.txt',
    'sql_output_file': 'converted_databricks_sql_processed.txt',
    'notebook_output_file': 'converted_databricks_notebook_complete.py',
    'field_mapping_json': 'field_mapping_from_excel.json',
    'fuzzy_mapping_json': 'fuzzy_mapping.json',

    # Processing parameters
    'fuzzy_similarity_threshold': 0.8,
    'max_fuzzy_examples': 5,

    # Output settings
    'save_json_files': True,
    'verbose_output': True,
    'auto_replace_original': False  # Set to True to automatically replace original SQL file
}

print("Configuration loaded successfully!")
print(f"Excel file: {CONFIG['excel_file']}")
print(f"SQL input file: {CONFIG['sql_input_file']}")
print(f"Fuzzy similarity threshold: {CONFIG['fuzzy_similarity_threshold']}")


# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

def validate_file_exists(filepath: str, description: str = "File") -> bool:
    """Validate that a file exists and provide user feedback."""
    if os.path.exists(filepath):
        if CONFIG['verbose_output']:
            print(f"✓ {description} found: {filepath}")
        return True
    else:
        print(f"✗ {description} not found: {filepath}")
        return False

def safe_file_operation(operation_func, *args, **kwargs):
    """Safely execute file operations with error handling."""
    try:
        return operation_func(*args, **kwargs)
    except Exception as e:
        print(f"Error during file operation: {str(e)}")
        return None

# Validate configuration files
print("=== File Validation ===")
excel_exists = validate_file_exists(CONFIG['excel_file'], "Excel file")
sql_exists = validate_file_exists(CONFIG['sql_input_file'], "SQL input file")

if not excel_exists:
    print("⚠️  Warning: Excel file not found. Excel processing will be skipped.")
if not sql_exists:
    print("⚠️  Warning: SQL input file not found. SQL processing will be skipped.")


# COMMAND ----------

def read_mapping_from_excel(excel_file: str) -> Dict:
    """
    Read mapping data from Excel file with improved error handling.

    Parameters:
    excel_file (str): Path to the Excel file

    Returns:
    dict: Dictionary containing the field mapping data
    """
    if not validate_file_exists(excel_file, "Excel file"):
        return {}

    try:
        # Read all sheet names
        xl = pd.ExcelFile(excel_file)
        if CONFIG['verbose_output']:
            print(f"Excel file contains sheets: {xl.sheet_names}")

        # Find sheets with 'Field' in their name
        field_sheets = [sheet for sheet in xl.sheet_names if 'Field' in sheet]
        if CONFIG['verbose_output']:
            print(f"Processing field sheets: {field_sheets}")

        if not field_sheets:
            print("⚠️  No sheets with 'Field' in name found.")
            return {}

        # Initialize data storage
        data_storage = {
            'used_by_values': {},
            'base_sap_cv_values': {},
            'sap_field_name_values': {},
            'dbx_field_name_values': {},
            'dbx_table_values': {},
            'comments_values': {}
        }

        unique_used_by_sets = {}

        # Process each field sheet
        for sheet_name in field_sheets:
            if CONFIG['verbose_output']:
                print(f"\nProcessing sheet: {sheet_name}")

            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            next_num = 1

            # Find the appropriate 'Used_by' column
            used_by_column = None
            for col_name in ['ADSO GCM', 'Used_by', 'Used by']:
                if col_name in df.columns:
                    used_by_column = col_name
                    break

            if not used_by_column:
                print(f"⚠️  No 'Used_by' column found in sheet '{sheet_name}'. Skipping.")
                continue

            # Process each row
            for _, row in df.iterrows():
                if pd.isna(row[used_by_column]):
                    continue

                used_by_value = str(row[used_by_column]).strip()
                sheet_used_by_key = (sheet_name, used_by_value)

                # Assign number if new
                if sheet_used_by_key not in unique_used_by_sets:
                    unique_used_by_sets[sheet_used_by_key] = next_num
                    next_num += 1

                num = unique_used_by_sets[sheet_used_by_key]
                composite_key = f"{sheet_name}_{num}"

                # Initialize lists if needed
                for storage_key in data_storage:
                    if composite_key not in data_storage[storage_key]:
                        data_storage[storage_key][composite_key] = []

                # Store data
                _store_field_data(row, df, composite_key, data_storage, used_by_value)

        # Create field mapping
        view_keys = sorted(list(data_storage['used_by_values'].keys()))
        return create_field_mapping(view_keys, **data_storage)

    except Exception as e:
        print(f"Error reading Excel file: {str(e)}")
        return {}

def _store_field_data(row, df, composite_key: str, data_storage: Dict, used_by_value: str):
    """Helper function to store field data from Excel row."""
    # Store Used_by value
    if used_by_value not in data_storage['used_by_values'][composite_key]:
        data_storage['used_by_values'][composite_key].append(used_by_value)

    # Store other field data
    field_mappings = [
        ('Base SAP CV - equivalent of DBX Table', 'base_sap_cv_values'),
        ('SAP Field Name', 'sap_field_name_values'),
        ('DBX Field name', 'dbx_field_name_values'),
        ('DBX Table', 'dbx_table_values'),
        ('Comments', 'comments_values')
    ]

    for col_name, storage_key in field_mappings:
        if col_name in df.columns and not pd.isna(row[col_name]):
            value = row[col_name]
            if value not in data_storage[storage_key][composite_key]:
                data_storage[storage_key][composite_key].append(value)

def create_field_mapping(view_keys: List[str], used_by_values: Dict = None, 
                        base_sap_cv_values: Dict = None, sap_field_name_values: Dict = None,
                        dbx_field_name_values: Dict = None, dbx_table_values: Dict = None,
                        comments_values: Dict = None) -> Dict:
    """
    Create a field mapping dictionary for calculation views.

    Parameters:
    view_keys (list): List of composite keys to include in the mapping
    *_values (dict): Dictionaries mapping composite keys to lists of values

    Returns:
    dict: The field mapping dictionary
    """
    # Initialize empty dictionaries for None parameters
    data_dicts = {
        'used_by_values': used_by_values or {},
        'base_sap_cv_values': base_sap_cv_values or {},
        'sap_field_name_values': sap_field_name_values or {},
        'dbx_field_name_values': dbx_field_name_values or {},
        'dbx_table_values': dbx_table_values or {},
        'comments_values': comments_values or {}
    }

    field_mapping = {}

    for key in view_keys:
        # Get maximum number of records for this view
        max_records = max(
            len(data_dict.get(key, [])) for data_dict in data_dicts.values()
        )
        max_records = max(max_records, 1)  # At least one record

        # Create records
        records = []
        table_name = data_dicts['dbx_table_values'].get(key, [""])[0] if data_dicts['dbx_table_values'].get(key, []) else ""
        base_sap_cv = data_dicts['base_sap_cv_values'].get(key, [""])[0] if data_dicts['base_sap_cv_values'].get(key, []) else ""

        for i in range(max_records):
            record = {
                'Used_by': _get_value_at_index(data_dicts['used_by_values'], key, i),
                'Base SAP CV - equivalent of DBX Table': _get_value_at_index(data_dicts['base_sap_cv_values'], key, i, base_sap_cv),
                'SAP Field Name': _get_value_at_index(data_dicts['sap_field_name_values'], key, i),
                'DBX Field name': _get_value_at_index(data_dicts['dbx_field_name_values'], key, i),
                'DBX Table': _get_value_at_index(data_dicts['dbx_table_values'], key, i, table_name),
                'Comments': _get_value_at_index(data_dicts['comments_values'], key, i)
            }
            records.append(record)

        field_mapping[f'calculation_view_{key}'] = records

    return field_mapping

def _get_value_at_index(data_dict: Dict, key: str, index: int, default: str = "") -> str:
    """Helper function to safely get value at index from data dictionary."""
    values = data_dict.get(key, [])
    return values[index] if index < len(values) else default

print("Excel processing functions loaded successfully!")


# COMMAND ----------

def calculate_similarity(str1: str, str2: str) -> float:
    """
    Calculate similarity between two strings using multiple methods.
    Returns a score between 0 and 1, where 1 is identical.
    """
    if not str1 or not str2:
        return 0.0

    # Convert to lowercase for comparison
    s1 = str1.lower().strip()
    s2 = str2.lower().strip()

    # Exact match
    if s1 == s2:
        return 1.0

    # Remove common suffixes/prefixes for better matching
    suffixes_to_remove = ['_p', '_r', '_cd', '_dat', '_code']
    s1_clean = s1
    s2_clean = s2

    for suffix in suffixes_to_remove:
        s1_clean = s1_clean.replace(suffix, '')
        s2_clean = s2_clean.replace(suffix, '')

    if s1_clean == s2_clean:
        return 0.9

    # Check if one is contained in the other
    if s1_clean in s2_clean or s2_clean in s1_clean:
        return 0.8

    # Character overlap ratio
    set1 = set(s1_clean)
    set2 = set(s2_clean)
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))

    if union == 0:
        return 0.0

    char_similarity = intersection / union

    # Length similarity factor
    len_diff = abs(len(s1_clean) - len(s2_clean))
    max_len = max(len(s1_clean), len(s2_clean))
    len_similarity = 1 - (len_diff / max_len) if max_len > 0 else 0

    # Combined score
    return (char_similarity * 0.7) + (len_similarity * 0.3)

def create_fuzzy_mapping(field_mapping: Dict, similarity_threshold: float = 0.6) -> Dict:
    """
    Create a fuzzy mapping containing only records where SAP Field Name
    and DBX Field name have similarity above the threshold.

    Parameters:
    field_mapping (dict): The complete field mapping
    similarity_threshold (float): Minimum similarity score (0-1)

    Returns:
    dict: Fuzzy mapping containing only similar field name pairs
    """
    fuzzy_mapping = {}

    for calc_view, records in field_mapping.items():
        fuzzy_records = []

        for record in records:
            sap_field = record.get('SAP Field Name', '')
            dbx_field = record.get('DBX Field name', '')

            # Skip empty fields
            if not sap_field or not dbx_field:
                continue

            # Calculate similarity
            similarity = calculate_similarity(sap_field, dbx_field)

            # Only include if similarity is above threshold but not exact match
            if similarity >= similarity_threshold and similarity < 1.0:
                fuzzy_record = record.copy()
                fuzzy_record['Similarity_Score'] = round(similarity, 3)
                fuzzy_record['Match_Type'] = 'Fuzzy'
                fuzzy_records.append(fuzzy_record)

        # Only add calculation view if it has fuzzy matches
        if fuzzy_records:
            fuzzy_mapping[calc_view] = fuzzy_records

    return fuzzy_mapping

print("Fuzzy matching functions loaded successfully!")


# COMMAND ----------

def load_field_mappings(json_file: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Load field mappings from JSON file.

    Returns:
    tuple: (field_mappings, table_mappings)
    """
    if not validate_file_exists(json_file, "JSON mapping file"):
        return {}, {}

    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        field_mappings = {}
        table_mappings = {}

        # Extract mappings from all calculation views
        for calc_view, records in data.items():
            for record in records:
                sap_field = str(record.get('SAP Field Name', '')).strip()
                dbx_field = str(record.get('DBX Field name', '')).strip()
                sap_table = str(record.get('Base SAP CV - equivalent of DBX Table', '')).strip()
                dbx_table = str(record.get('DBX Table', '')).strip()

                if sap_field and dbx_field:
                    field_mappings[sap_field.upper()] = dbx_field

                if sap_table and dbx_table:
                    table_mappings[sap_table] = dbx_table

        if CONFIG['verbose_output']:
            print(f"Loaded {len(field_mappings)} field mappings")
            print(f"Loaded {len(table_mappings)} table mappings")

        return field_mappings, table_mappings

    except Exception as e:
        print(f"Error loading field mappings: {str(e)}")
        return {}, {}

def process_sql_file(input_file: str, output_file: str, 
                    field_mappings: Dict[str, str], table_mappings: Dict[str, str]) -> int:
    """
    Process SQL file to replace field names and table names.

    Returns:
    int: Number of SQL statements processed
    """
    if not validate_file_exists(input_file, "SQL input file"):
        return 0

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()

        # Replace field names (case-insensitive)
        for sap_field, dbx_field in field_mappings.items():
            pattern = r'\b' + re.escape(sap_field) + r'\b'
            content = re.sub(pattern, dbx_field, content, flags=re.IGNORECASE)

        # Replace table names
        for sap_table, dbx_table in table_mappings.items():
            content = content.replace(sap_table, dbx_table)

        # Split content into SQL statements
        sql_statements = re.split(r';\s*\n\s*\n', content.strip())

        # Format as Databricks notebook with spark.sql() wrapper
        formatted_statements = []
        for statement in sql_statements:
            statement = statement.strip()
            if statement:
                if not statement.endswith(';'):
                    statement += ';'
                formatted_statement = f'spark.sql("""\n{statement}\n""")'
                formatted_statements.append(formatted_statement)

        # Join all statements
        final_content = '\n\n'.join(formatted_statements)

        # Write to output file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(final_content)

        if CONFIG['verbose_output']:
            print(f"✓ Processed {len(formatted_statements)} SQL statements")
            print(f"✓ Output saved to: {output_file}")

        return len(formatted_statements)

    except Exception as e:
        print(f"Error processing SQL file: {str(e)}")
        return 0

def create_databricks_notebook(input_file: str, output_file: str) -> int:
    """
    Convert processed SQL file to Databricks notebook format.

    Returns:
    int: Number of SQL statements in the notebook
    """
    if not validate_file_exists(input_file, "Processed SQL file"):
        return 0

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()

        # Split content by spark.sql statements
        statements = content.split('spark.sql("""')

        # Start with notebook header
        notebook_content = [
            "# Databricks notebook source",
            "# MAGIC %md",
            "# MAGIC # Converted SQL Statements for Databricks",
            "# MAGIC This notebook contains SQL statements converted from SAP field names to DBX field names",
            ""
        ]

        # Process each SQL statement
        for i, statement in enumerate(statements):
            if statement.strip() and i > 0:  # Skip first empty part
                notebook_content.extend([
                    "# COMMAND ----------",
                    "",
                    'spark.sql("""' + statement.strip(),
                    ""
                ])

        # Write to output file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(notebook_content))

        num_statements = len(statements) - 1
        if CONFIG['verbose_output']:
            print(f"✓ Created Databricks notebook with {num_statements} SQL statements")
            print(f"✓ Output saved to: {output_file}")

        return num_statements

    except Exception as e:
        print(f"Error creating Databricks notebook: {str(e)}")
        return 0

def replace_original_file(notebook_file: str, target_file: str) -> bool:
    """
    Replace the original file with the properly formatted notebook.

    Returns:
    bool: True if successful, False otherwise
    """
    try:
        shutil.copy2(notebook_file, target_file)
        if CONFIG['verbose_output']:
            print(f"✓ Successfully replaced {target_file} with formatted notebook")
            print("  - SAP field names converted to DBX field names")
            print("  - SAP table names converted to DBX table names")
            print("  - All SQL statements wrapped with spark.sql() syntax")
            print("  - Proper Databricks notebook formatting")
        return True
    except Exception as e:
        print(f"Error replacing original file: {str(e)}")
        return False

print("SQL processing functions loaded successfully!")


# COMMAND ----------

# Initialize variables
field_mapping_from_excel = {}
fuzzy_mapping = {}

# Process Excel file if it exists
if validate_file_exists(CONFIG['excel_file']):
    print("=== Step 1: Processing Excel File ===")

    # Read mapping data from Excel
    field_mapping_from_excel = read_mapping_from_excel(CONFIG['excel_file'])

    if field_mapping_from_excel:
        print(f"✓ Successfully processed Excel file")
        print(f"✓ Created {len(field_mapping_from_excel)} calculation views")

        # Save to JSON file if configured
        if CONFIG['save_json_files']:
            try:
                with open(CONFIG['field_mapping_json'], 'w') as f:
                    json.dump(field_mapping_from_excel, f, indent=4)
                print(f"✓ Field mapping saved to: {CONFIG['field_mapping_json']}")
            except Exception as e:
                print(f"Error saving field mapping: {str(e)}")

        # Display sample data if verbose
        if CONFIG['verbose_output']:
            print("\nSample field mapping data:")
            for i, (calc_view, records) in enumerate(field_mapping_from_excel.items()):
                if i >= 2:  # Show only first 2 calculation views
                    break
                print(f"  {calc_view}: {len(records)} records")
                if records:
                    sample_record = records[0]
                    print(f"    Sample: {sample_record.get('SAP Field Name', 'N/A')} → {sample_record.get('DBX Field name', 'N/A')}")
    else:
        print("⚠️  No field mapping data extracted from Excel file")
else:
    print("⚠️  Skipping Excel processing - file not found")


# COMMAND ----------

# Initialize variables for SQL processing
field_mappings = {}
table_mappings = {}
num_sql_statements = 0

if validate_file_exists(CONFIG['field_mapping_json']) and validate_file_exists(CONFIG['sql_input_file']):
    print("=== Step 3: Processing SQL File ===")

    # Load field mappings
    field_mappings, table_mappings = load_field_mappings(CONFIG['field_mapping_json'])

    if field_mappings:
        # Process SQL file
        num_sql_statements = process_sql_file(
            CONFIG['sql_input_file'],
            CONFIG['sql_output_file'],
            field_mappings,
            table_mappings
        )

        if num_sql_statements > 0:
            print(f"✓ Successfully processed SQL file with {num_sql_statements} statements")
        else:
            print("⚠️  No SQL statements were processed")
    else:
        print("⚠️  No field mappings available for SQL processing")
else:
    print("⚠️  Skipping SQL processing - required files not found")


# COMMAND ----------

num_notebook_statements = 0

if validate_file_exists(CONFIG['sql_output_file']):
    print("=== Step 4: Creating Databricks Notebook ===")

    # Create Databricks notebook
    num_notebook_statements = create_databricks_notebook(
        CONFIG['sql_output_file'],
        CONFIG['notebook_output_file']
    )

    if num_notebook_statements > 0:
        print(f"✓ Successfully created Databricks notebook with {num_notebook_statements} statements")

        # Replace original file if configured
        if CONFIG['auto_replace_original']:
            success = replace_original_file(CONFIG['notebook_output_file'], CONFIG['sql_input_file'])
            if success:
                print("✓ Original SQL file replaced with formatted notebook")
        else:
            print(f"ℹ️  To replace the original file, run:")
            print(f"   replace_original_file('{CONFIG['notebook_output_file']}', '{CONFIG['sql_input_file']}')")
    else:
        print("⚠️  No statements were added to the notebook")
else:
    print("⚠️  Skipping notebook creation - processed SQL file not found")


# COMMAND ----------

num_notebook_statements = 0

if validate_file_exists(CONFIG['sql_output_file']):
    print("=== Step 4: Creating Databricks Notebook ===")

    # Create Databricks notebook
    num_notebook_statements = create_databricks_notebook(
        CONFIG['sql_output_file'],
        CONFIG['notebook_output_file']
    )

    if num_notebook_statements > 0:
        print(f"✓ Successfully created Databricks notebook with {num_notebook_statements} statements")

        # Replace original file if configured
        if CONFIG['auto_replace_original']:
            success = replace_original_file(CONFIG['notebook_output_file'], CONFIG['sql_input_file'])
            if success:
                print("✓ Original SQL file replaced with formatted notebook")
        else:
            print(f"ℹ️  To replace the original file, run:")
            print(f"   replace_original_file('{CONFIG['notebook_output_file']}', '{CONFIG['sql_input_file']}')")
    else:
        print("⚠️  No statements were added to the notebook")
else:
    print("⚠️  Skipping notebook creation - processed SQL file not found")


# COMMAND ----------

print("=" * 60)
print("PROCESSING SUMMARY")
print("=" * 60)

# Excel processing summary
if field_mapping_from_excel:
    print(f"✓ Excel Processing: {len(field_mapping_from_excel)} calculation views created")
    total_records = sum(len(records) for records in field_mapping_from_excel.values())
    print(f"  - Total field mapping records: {total_records}")
else:
    print("✗ Excel Processing: Failed or skipped")

# Fuzzy mapping summary
if fuzzy_mapping:
    total_fuzzy = sum(len(records) for records in fuzzy_mapping.values())
    print(f"✓ Fuzzy Mapping: {total_fuzzy} fuzzy matches found")
else:
    print("✗ Fuzzy Mapping: No matches found or skipped")

# SQL processing summary
if num_sql_statements > 0:
    print(f"✓ SQL Processing: {num_sql_statements} statements processed")
    print(f"  - Field mappings applied: {len(field_mappings)}")
    print(f"  - Table mappings applied: {len(table_mappings)}")
else:
    print("✗ SQL Processing: Failed or skipped")

# Notebook creation summary
if num_notebook_statements > 0:
    print(f"✓ Notebook Creation: {num_notebook_statements} statements in notebook")
else:
    print("✗ Notebook Creation: Failed or skipped")

print("\nOutput Files:")
for file_key, file_path in CONFIG.items():
    if file_key.endswith('_file') or file_key.endswith('_json'):
        if os.path.exists(file_path):
            print(f"  ✓ {file_path}")
        else:
            print(f"  ✗ {file_path} (not created)")

print("=" * 60)


# COMMAND ----------

def run_complete_workflow():
    """Run the complete SQL conversion workflow in one function."""
    print("=== Starting Complete Workflow ===")

    # Step 1: Process Excel
    if validate_file_exists(CONFIG['excel_file']):
        field_mapping = read_mapping_from_excel(CONFIG['excel_file'])
        if CONFIG['save_json_files'] and field_mapping:
            with open(CONFIG['field_mapping_json'], 'w') as f:
                json.dump(field_mapping, f, indent=4)

    # Step 2: Create fuzzy mapping
    if 'field_mapping' in locals() and field_mapping:
        fuzzy_map = create_fuzzy_mapping(field_mapping, CONFIG['fuzzy_similarity_threshold'])
        if CONFIG['save_json_files'] and fuzzy_map:
            with open(CONFIG['fuzzy_mapping_json'], 'w') as f:
                json.dump(fuzzy_map, f, indent=4)

    # Step 3: Process SQL
    if validate_file_exists(CONFIG['field_mapping_json']) and validate_file_exists(CONFIG['sql_input_file']):
        field_maps, table_maps = load_field_mappings(CONFIG['field_mapping_json'])
        if field_maps:
            process_sql_file(CONFIG['sql_input_file'], CONFIG['sql_output_file'], field_maps, table_maps)

    # Step 4: Create notebook
    if validate_file_exists(CONFIG['sql_output_file']):
        create_databricks_notebook(CONFIG['sql_output_file'], CONFIG['notebook_output_file'])

        if CONFIG['auto_replace_original']:
            replace_original_file(CONFIG['notebook_output_file'], CONFIG['sql_input_file'])

    print("=== Workflow Complete ===")

run_complete_workflow()



# COMMAND ----------


