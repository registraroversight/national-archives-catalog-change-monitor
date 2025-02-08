import requests
import psycopg2
from datetime import datetime
import wmill
import tempfile
import json
import os
import zipfile

# Fetch PostgreSQL credentials from Windmill resource, change this if running outside of Windmill
postgres_creds = wmill.get_resource("u/user/db_postgresql")

# Handle SSL root certificate if provided as PEM
if "root_certificate_pem" in postgres_creds:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".crt") as temp_cert:
        temp_cert.write(postgres_creds["root_certificate_pem"].encode())
        postgres_creds["sslrootcert"] = temp_cert.name
    del postgres_creds["root_certificate_pem"]

# Build the connection string
connection_str = (
    f"postgresql://{postgres_creds['user']}:{postgres_creds['password']}@"
    f"{postgres_creds['host']}:{postgres_creds['port']}/{postgres_creds['dbname']}"
)

def main():
    url = "https://catalog.archives.gov/api/v2/records/search?recordGroupNumber=612&limit=1000" # RG 612 specific query
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': 'api_key' # request api key
    }

    # Make the API request
    response = requests.get(url, headers=headers)
    data = response.json()

    # Save the returned JSON (comment out lines 38-50 to skip this)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    save_dir = "/tmp/windmill/data/path/"
    os.makedirs(save_dir, exist_ok=True)
    json_file_path = os.path.join(save_dir, f"catalog_response_{timestamp}.json")
    zip_file_path = os.path.join(save_dir, f"catalog_response_{timestamp}.zip")
    
    with open(json_file_path, "w") as json_file:
        json.dump(data, json_file, indent=4)
    
    with zipfile.ZipFile(zip_file_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(json_file_path, os.path.basename(json_file_path))
    
    os.remove(json_file_path)  # Remove the unzipped JSON file after zipping

    # Connect to PostgreSQL database
    conn = psycopg2.connect(connection_str)
    cursor = conn.cursor()

    # Prepare SQL insert statements
    insert_master_sql = """
    INSERT INTO MASTER_TEMP (
        temp_naid, 
        temp_title, 
        temp_level_of_description, 
        temp_parent_series_naid, 
        temp_parent_series_title,
        temp_parent_file_unit_naid, 
        temp_parent_file_unit_title, 
        temp_creator,
        temp_inclusive_start_date, 
        temp_inclusive_end_date, 
        temp_coverage_start_date,
        temp_coverage_end_date, 
        temp_ldr_count, 
        temp_series_extents, 
        temp_access_restriction_status,
        temp_specific_access_restrictions, 
        temp_accession_numbers,
        temp_disposition_authority_numbers,
        temp_crccrca_number,
        temp_scope_and_content_note,
        temp_function_and_use_note,
        temp_general_notes,
        temp_scrape_timestamp
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING temp_naid;
    """

    insert_object_url_temp_sql = """
    INSERT INTO OBJECT_URL_TEMP (temp_naid, temp_digital_object_url, temp_digital_object_id, temp_scrape_timestamp)
    VALUES (%s, %s, %s, %s);
    """

    # Get the current timestamp
    temp_scrape_timestamp = datetime.now()

    # Loop through the response data and build the rows
    for item in data['body']['hits']['hits']:
        record = item['_source']['record']
        temp_ldr_count = ""
        temp_CRCCRCA_number = ""

        # Extract holdings measurement count for "Logical Data Record"
        if 'physicalOccurrences' in record:
            for occurrence in record['physicalOccurrences']:
                if 'holdingsMeasurements' in occurrence:
                    for measurement in occurrence['holdingsMeasurements']:
                        if measurement['type'] == "Logical Data Record": # Change measurement type based on need/record types
                            temp_ldr_count = measurement['count']  # Get the count

        # Extract variantControlNumbers
        if 'variantControlNumbers' in record:
            for occurrence in record['variantControlNumbers']: # Change variant control number based on need/record types
                if 'note' in occurrence and occurrence['note'] == "Civil Rights Cold Case Records Collection Act Request Number.":
                    temp_CRCCRCA_number = occurrence['number']

        # Prepare the row data
        temp_naid = record.get('naId', "")
        temp_title = record.get('title', "")
        digital_objects = record.get('digitalObjects', [])
        temp_level_of_description = record.get('levelOfDescription', "")
        temp_parent_series_naid = record['ancestors'][1]['naId'] if len(record.get('ancestors', [])) > 1 else None
        temp_parent_series_title = record['ancestors'][1]['title'] if len(record.get('ancestors', [])) > 1 else None
        temp_parent_file_unit_naid = record['ancestors'][2]['naId'] if len(record.get('ancestors', [])) > 2 else None
        temp_parent_file_unit_title = record['ancestors'][2]['title'] if len(record.get('ancestors', [])) > 2 else None
        temp_creator = '|'.join([c['heading'] for c in record.get('creators', [])])
        temp_inclusive_start_date = record.get('inclusiveStartDate', {}).get('logicalDate', "")
        temp_inclusive_end_date = record.get('inclusiveEndDate', {}).get('logicalDate', "")
        temp_coverage_start_date = record.get('coverageStartDate', {}).get('logicalDate', "")
        temp_coverage_end_date = record.get('coverageEndDate', {}).get('logicalDate', "")
        temp_series_extents = record.get('physicalOccurrences', [{}])[0].get('extent', "")
        temp_access_restriction_status = record.get('accessRestriction', {}).get('status', "")
        temp_specific_access_restrictions = '|'.join([r['restriction'] for r in record.get('accessRestriction', {}).get('specificAccessRestrictions', [])])
        temp_accession_numbers = '|'.join(record.get('accessionNumbers', []))
        temp_disposition_authority_numbers = '|'.join(record.get('dispositionAuthorityNumbers', []))
        temp_scope_and_content_note = record.get('scopeAndContentNote', "")
        temp_function_and_use_note = record.get('functionAndUse', "")
        temp_general_notes = '|'.join(record.get('generalNotes', []))

        # Insert into MASTER_TEMP table
        try:
            cursor.execute(insert_master_sql, (
                temp_naid, 
                temp_title, 
                temp_level_of_description, 
                temp_parent_series_naid, 
                temp_parent_series_title,
                temp_parent_file_unit_naid, 
                temp_parent_file_unit_title, 
                temp_creator,
                temp_inclusive_start_date, 
                temp_inclusive_end_date, 
                temp_coverage_start_date,
                temp_coverage_end_date, 
                temp_ldr_count, 
                temp_series_extents,
                temp_access_restriction_status, 
                temp_specific_access_restrictions,
                temp_accession_numbers, 
                temp_disposition_authority_numbers,
                temp_CRCCRCA_number,
                temp_scope_and_content_note,
                temp_function_and_use_note,
                temp_general_notes,
                temp_scrape_timestamp
            ))
            # Get the inserted naid for the OBJECT_URL_TEMP table
            inserted_naid = cursor.fetchone()[0]
            print(f"Inserted naid: {inserted_naid}")  # Debug print

            # Insert digital object data into OBJECT_URL_TEMP table, one object per row
            for obj in digital_objects:
                OBJECT_URL_TEMP = obj.get('objectUrl', None)
                object_id = obj.get('objectId', None)
                cursor.execute(insert_object_url_temp_sql, (inserted_naid, OBJECT_URL_TEMP, object_id, temp_scrape_timestamp))

        except psycopg2.Error as e:
            print(f"Error inserting record: {e}")  # Error handling
            conn.rollback()  # Rollback in case of error
            continue  # Skip to the next record

    # Commit the transactions and close the connection
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Data written to database with timestamp: {temp_scrape_timestamp}")

if __name__ == '__main__':
    main()
