# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End SQL Comment Converter
# MAGIC
# MAGIC This notebook provides a complete end-to-end solution that:
# MAGIC 1. Reads Excel mapping data
# MAGIC 2. Creates field mappings automatically
# MAGIC 3. Converts SQL by adding SAP field description comments
# MAGIC 4. Handles errors gracefully with warnings

# COMMAND ----------

import pandas as pd
import json
import re
import warnings
from typing import Dict, Optional, Tuple

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Excel Data Processing

# COMMAND ----------

def read_excel_mapping(file_path: str = "mapping.xlsx") -> Optional[Dict]:
    """
    Read Excel file and create nested mapping structure.
    
    Args:
        file_path: Path to the Excel file
        
    Returns:
        Nested mapping dictionary or None if failed
    """
    try:
        print(f"Reading Excel file: {file_path}")
        
        # Try to read the specific sheet
        try:
            df = pd.read_excel(file_path, sheet_name='EWD field mapping_NN')
            print(f"Successfully loaded sheet 'EWD field mapping_NN'")
        except Exception as e:
            warnings.warn(f"Could not read sheet 'EWD field mapping_NN': {e}")
            # Try to find any sheet with '_NN' in the name
            excel_file = pd.ExcelFile(file_path)
            nn_sheets = [sheet for sheet in excel_file.sheet_names if "_NN" in sheet]
            if nn_sheets:
                df = pd.read_excel(file_path, sheet_name=nn_sheets[0])
                print(f"Using alternative sheet: {nn_sheets[0]}")
            else:
                warnings.warn("No sheets with '_NN' found. Using first sheet.")
                df = pd.read_excel(file_path, sheet_name=0)
        
        print(f"Original shape: {df.shape}")
        
        # Check for required columns
        required_columns = ['ADSO GCM', 'SAP Field Name', 'SAP Field description', 'DBX Table', 'DBX Field name']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            warnings.warn(f"Missing required columns: {missing_columns}")
            print("Available columns:")
            for col in df.columns:
                print(f"  - '{col}'")
            return None
        
        # Remove rows where all required columns are NaN
        df_clean = df.dropna(subset=required_columns, how='all')
        print(f"Shape after removing empty rows: {df_clean.shape}")
        
        # Create nested mapping
        nested_mapping = {}
        processed_rows = 0
        
        for index, row in df_clean.iterrows():
            adso_gcm = row['ADSO GCM']
            
            # Skip rows where ADSO GCM is NaN
            if pd.isna(adso_gcm):
                continue
                
            # Initialize the ADSO GCM entry if it doesn't exist
            if adso_gcm not in nested_mapping:
                nested_mapping[adso_gcm] = []
            
            # Create the mapping entry
            mapping_entry = {
                'SAP_Field_Name': row['SAP Field Name'] if not pd.isna(row['SAP Field Name']) else None,
                'SAP_Field_Description': row['SAP Field description'] if not pd.isna(row['SAP Field description']) else None,
                'DBX_Table': row['DBX Table'] if not pd.isna(row['DBX Table']) else None,
                'DBX_Field_Name': row['DBX Field name'] if not pd.isna(row['DBX Field name']) else None
            }
            
            nested_mapping[adso_gcm].append(mapping_entry)
            processed_rows += 1
        
        print(f"Successfully processed {processed_rows} rows")
        print(f"Created nested mapping with {len(nested_mapping)} ADSO GCM entries")
        
        return nested_mapping
        
    except Exception as e:
        warnings.warn(f"Error reading Excel file: {e}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Mapping Processing Functions

# COMMAND ----------

def create_field_lookup(nested_mapping: Dict) -> Dict[str, str]:
    """
    Create a lookup dictionary from DBX_Field_Name to SAP_Field_Description.
    
    Args:
        nested_mapping: The nested mapping dictionary
        
    Returns:
        Dictionary mapping DBX field names to SAP field descriptions
    """
    field_lookup = {}
    
    if not nested_mapping:
        warnings.warn("No mapping data provided")
        return field_lookup
    
    for adso_gcm, entries in nested_mapping.items():
        for entry in entries:
            dbx_field = entry.get('DBX_Field_Name')
            sap_description = entry.get('SAP_Field_Description')
            
            if dbx_field and sap_description:
                # Convert to lowercase for case-insensitive matching
                field_lookup[dbx_field.lower()] = sap_description
    
    print(f"Created field lookup with {len(field_lookup)} mappings")
    return field_lookup

def find_field_description(field_name: str, field_lookup: Dict[str, str]) -> Optional[str]:
    """
    Find the SAP field description for a given field name.
    
    Args:
        field_name: The field name to look up
        field_lookup: Dictionary mapping field names to descriptions
        
    Returns:
        SAP field description if found, None otherwise
    """
    if not field_lookup:
        return None
        
    # Try exact match first (case-insensitive)
    field_lower = field_name.lower().strip()
    
    if field_lower in field_lookup:
        return field_lookup[field_lower]
    
    # Try without underscores
    field_no_underscore = field_lower.replace('_', '')
    for key, value in field_lookup.items():
        if key.replace('_', '') == field_no_underscore:
            return value
    
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: SQL Processing Functions

# COMMAND ----------

def process_sql_string(sql_content: str, field_lookup: Dict[str, str]) -> Tuple[str, int, int]:
    """
    Process the SQL string and add comments with SAP field descriptions.
    
    Args:
        sql_content: The SQL content as a string
        field_lookup: Dictionary mapping field names to descriptions
        
    Returns:
        Tuple of (processed SQL string, comments added count, fields processed count)
    """
    if not sql_content.strip():
        warnings.warn("Empty SQL content provided")
        return sql_content, 0, 0
    
    lines = sql_content.split('\n')
    output_lines = []
    comments_added = 0
    fields_processed = 0
    
    for line in lines:
        original_line = line.rstrip()
        
        # Check if this line contains a field definition
        # Pattern: field_name followed by data type, optionally ending with comma
        field_match = re.match(r'\s*(\w+)\s+(.+?)(?:,\s*)?$', line.strip())
        
        if field_match:
            field_name = field_match.group(1)
            data_type = field_match.group(2).rstrip(',').strip()
            fields_processed += 1
            
            description = find_field_description(field_name, field_lookup)
            
            if description:
                # Add COMMENT() syntax with SAP field description
                # Get the original indentation
                indent = len(original_line) - len(original_line.lstrip())
                indent_str = ' ' * indent
                
                if original_line.endswith(','):
                    # Format: field_name DATA_TYPE COMMENT(description),
                    commented_line = f"{indent_str}{field_name:<18} {data_type} COMMENT('{description}'),"
                else:
                    # Format: field_name DATA_TYPE COMMENT(description)
                    commented_line = f"{indent_str}{field_name:<18} {data_type} COMMENT('{description}')"
                
                output_lines.append(commented_line)
                comments_added += 1
                print(f"✓ Added comment for {field_name}: {description}")
            else:
                # No description found, keep original line
                output_lines.append(original_line)
                print(f"⚠ No description found for field: {field_name}")
        else:
            # Not a field definition line, keep as is
            output_lines.append(original_line)
    
    return '\n'.join(output_lines), comments_added, fields_processed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Main End-to-End Function

# COMMAND ----------

def convert_sql_end_to_end(sql_input: str, excel_file: str = "mapping.xlsx", output_filename: str = "sql_commented.sql") -> str:
    """
    Complete end-to-end SQL conversion process.
    
    Args:
        sql_input: SQL content as a string
        excel_file: Path to Excel mapping file
        output_filename: Name of the output file
        
    Returns:
        Processed SQL string with comments
    """
    print("="*60)
    print("STARTING END-TO-END SQL COMMENT CONVERSION")
    print("="*60)
    
    try:
        # Step 1: Read Excel and create mapping
        print("\n📊 STEP 1: Reading Excel mapping data...")
        nested_mapping = read_excel_mapping(excel_file)
        
        if not nested_mapping:
            warnings.warn("Failed to create mapping from Excel. Using empty mapping.")
            nested_mapping = {}
        
        # Step 2: Create field lookup
        print("\n🔍 STEP 2: Creating field lookup...")
        field_lookup = create_field_lookup(nested_mapping)
        
        if not field_lookup:
            warnings.warn("No field mappings available. SQL will be returned unchanged.")
        
        # Step 3: Process SQL
        print("\n⚙️ STEP 3: Processing SQL content...")
        processed_sql, comments_added, fields_processed = process_sql_string(sql_input, field_lookup)
        
        # Step 4: Save output
        print("\n💾 STEP 4: Saving output...")
        try:
            # Try to save to /dbfs/FileStore/ (Databricks path)
            output_path = f"/dbfs/FileStore/{output_filename}"
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(processed_sql)
            print(f"✓ Output saved to: {output_path}")
        except:
            # Fallback to local path
            try:
                with open(output_filename, 'w', encoding='utf-8') as f:
                    f.write(processed_sql)
                print(f"✓ Output saved to: {output_filename}")
            except Exception as e:
                warnings.warn(f"Could not save output file: {e}")
        
        # Summary
        print("\n" + "="*60)
        print("CONVERSION SUMMARY")
        print("="*60)
        print(f"📈 Fields processed: {fields_processed}")
        print(f"💬 Comments added: {comments_added}")
        print(f"📊 Success rate: {(comments_added/fields_processed*100):.1f}%" if fields_processed > 0 else "No fields processed")
        print("✅ CONVERSION COMPLETED SUCCESSFULLY!")
        print("="*60)
        
        return processed_sql
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        warnings.warn(f"End-to-end conversion failed: {e}")
        return sql_input  # Return original SQL if conversion fails

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Usage Example

# COMMAND ----------

# Example SQL for testing
sample_sql = """CREATE TABLE lpdbwlpglm01devdev.gold_lego_edw.md_post_vast
(
    polnr_postalg_ts   TIMESTAMP,
    relnr_r            DECIMAL(5, 0),
    volgnr_p           DECIMAL(7, 0),
    ond_nr_p           DECIMAL(3, 0),
    tep_code_p         DECIMAL(1, 0),
    ltst_mut_dat_p     DATE,
    wap_code_p         DECIMAL(1, 0),
    vorig_polisnr_p    DECIMAL(11, 0),
    aanvangs_dat_p     DATE,
    byz_1_cd_red_hp    DECIMAL(1, 0),
    aanvangs_dat_cd    DECIMAL(1, 0),
    ct_polis_p         DECIMAL(3, 0),
    djr_v_dat_p        DATE,
    opn_dat_post_p     DATE,
    ltst_sal_dat_p     DATE,
    parttime_perc_p    DECIMAL(7, 4),
    ltst_sal_code_p    DECIMAL(3, 0),
    ltst_sal_p         DECIMAL(9, 2),
    parttime_perc_gew  DECIMAL(7, 4),
    parttime_perc_dt_p DECIMAL(7, 4),
    rel_nr_rr_p        DECIMAL(5, 0),
    volg_nr_rr_p       DECIMAL(7, 0),
    rel_nr_uitruil_p   DECIMAL(5, 0),
    volg_nr_uitruil_p  DECIMAL(7, 0),
    afgel_wzp_verz_p   DECIMAL(7, 2),
    zcanceldt          DATE,
    zcancelfg          STRING,
    zvalidfrom         STRING
)"""

# Run the complete end-to-end conversion
result = convert_sql_end_to_end(sample_sql)

# COMMAND ----------

# Display the result
print("FINAL CONVERTED SQL:")
print("="*60)
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Standalone Function for Custom Usage

# COMMAND ----------

def quick_convert(sql_input: str, excel_file: str = "mapping.xlsx") -> str:
    """
    Quick conversion function for simple usage.
    
    Args:
        sql_input: SQL content to convert
        excel_file: Excel mapping file path
        
    Returns:
        Converted SQL with comments
    """
    return convert_sql_end_to_end(sql_input, excel_file, "quick_output.sql")


# COMMAND ----------

quick_convert(sample_sql)

# COMMAND ----------


