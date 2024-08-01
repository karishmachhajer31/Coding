To design a data pipeline for Zomato’s data files, you need to build and implement the modules outlined in the problem statement. Here's a detailed plan and Python code for the two modules you need to develop: File Check Module and Data Quality Check Module.
1. File Check Module
This module ensures that files are valid for processing before they are ingested. It checks if the file is new, not empty, and has the correct extension.
Implementation Steps:
Check if the file is new:
Maintain a record of processed files.
Compare the current file with the processed ones to determine if it’s new.
Check if the file is empty:
Verify if the file has content.
Check if the file extension is .csv:
Ensure the file extension is correct.

import os
import pandas as pd

PROCESSED_FILES_PATH = 'processed_files/'
SOURCE_FILES_PATH = 'source_files/'
INVALID_FILES_PATH = 'invalid_files/'

def is_new_file(file_name):
    processed_files = os.listdir(PROCESSED_FILES_PATH)
    return file_name not in processed_files

def is_file_empty(file_path):
    return os.stat(file_path).st_size == 0

def check_file_extension(file_name):
    return file_name.lower().endswith('.csv')

def file_check_module():
    files = os.listdir(SOURCE_FILES_PATH)
    
    for file in files:
        file_path = os.path.join(SOURCE_FILES_PATH, file)
        
        if not check_file_extension(file):
            os.rename(file_path, os.path.join(INVALID_FILES_PATH, file))
            continue
        
        if not is_new_file(file):
            os.rename(file_path, os.path.join(INVALID_FILES_PATH, file))
            continue
        
        if is_file_empty(file_path):
            os.rename(file_path, os.path.join(INVALID_FILES_PATH, file))
            continue
        
        # If file passes all checks, move to processed files directory
        os.rename(file_path, os.path.join(PROCESSED_FILES_PATH, file))

# Execute the File Check Module
file_check_module()

2. Data Quality Check Module
This module performs several data validation checks on the contents of the files, cleans data where possible, and separates good and bad records.
Implementation Steps:
Validate Phone Numbers:
Remove any non-numeric characters and validate phone numbers.
Check for Null Values:
Identify records with null values in non-null fields.
Clean Descriptive Fields:
Remove special characters and unwanted spaces from fields like address and reviews_list.
Split Contact Numbers:
Split and clean contact numbers if they have multiple entries.
python
Copy code
import re

def validate_phone_number(phone):
    # Remove any non-numeric characters
    cleaned_phone = re.sub(r'\D', '', str(phone))
    # Assuming valid phone numbers are 10 digits long
    return cleaned_phone if len(cleaned_phone) == 10 else None

def clean_field(field):
    # Remove special characters and extra spaces
    return re.sub(r'[^\w\s]', '', str(field)).strip()

def data_quality_check_module(file_name):
    input_file_path = os.path.join(PROCESSED_FILES_PATH, file_name)
    df = pd.read_csv(input_file_path)
    
    good_records = []
    bad_records = []
    
    for index, row in df.iterrows():
        is_valid = True
        
        # Check for null values in required fields
        if pd.isnull(row['name']) or pd.isnull(row['phone']) or pd.isnull(row['location']):
            bad_records.append((index, row.to_dict()))
            is_valid = False
        
        # Validate phone number
        if is_valid:
            cleaned_phone = validate_phone_number(row['phone'])
            if cleaned_phone is None:
                bad_records.append((index, row.to_dict()))
                is_valid = False
            else:
                row['phone'] = cleaned_phone
        
        # Clean address and reviews_list fields
        if is_valid:
            row['address'] = clean_field(row['address'])
            row['reviews_list'] = clean_field(row['reviews_list'])
        
        if is_valid:
            good_records.append(row)
        else:
            bad_records.append((index, row.to_dict()))
    
    # Save good records
    good_df = pd.DataFrame(good_records)
    good_df.to_csv(f'clean_{file_name}.out', index=False)
    
    # Save bad records
    bad_df = pd.DataFrame([record[1] for record in bad_records])
    bad_df.to_csv(f'bad_{file_name}.bad', index=False)
    
    # Optional metadata file
    metadata = pd.DataFrame({
        'Type_of_issue': ['null' if pd.isnull(record[1]['name']) or pd.isnull(record[1]['phone']) or pd.isnull(record[1]['location']) else 'phone_invalid' for record in bad_records],
        'Row_num_list': [record[0] for record in bad_records]
    })
    metadata.to_csv(f'bad_{file_name}_metadata.csv', index=False)

# Execute the Data Quality Check Module
for file in os.listdir(PROCESSED_FILES_PATH):
    if file.endswith('.csv'):
        data_quality_check_module(file)

Summary
File Check Module ensures that files are new, non-empty, and of the correct type. Invalid files are moved to a separate directory.
Data Quality Check Module validates and cleans data fields, separating valid records from invalid ones and saving them to designated files.
You can enhance these modules further by integrating logging, exception handling, and additional validation rules based on specific business requirements.

