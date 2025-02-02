# National Archives Catalog Change Monitor
A python tool to scrape and monitor the metadata in the [U.S. National Archives Catalog](https://catalog.archives.gov). The tool scrapes the [Catalog API](https://www.archives.gov/research/catalog/help/api), parses the returned JSON, writes the metadata to a PostgreSQL DB, and compares the newly scraped data against the previously scraped data for changes. It does not scrape the actual files but it does scrape the S3 object URLs so you could add another step to download them as well.

If you're more interested in bulk data you can get snapshot directly from the [AWS Registry of Open Data](https://registry.opendata.aws/nara-national-archives-catalog) and read more about the snapshot [here](https://www.archives.gov/developer/national-archives-catalog-dataset). You can also directly get the digital objects from the [public S3 bucket](https://us-east-1.console.aws.amazon.com/s3/buckets/NARAprodstorage?region=us-east-1&bucketType=general&prefix=lz%2F&showversions=true).

## Usage
I run this as a flow in a [Windmill](https://github.com/windmill-labs/windmill/tree/main) docker container along with a separate docker container for PostgreSQL 17. Windmill allows you to schedule the python scripts to run in order and stops if there's an error and can send error messages to your chosen notification tool. But you could run the python scripts manually without Windmill.

The catalog_monitor.sql contains the DB schema the python scripts expect to find in the PostgreSQL DB.

Run the python scripts in the following order: 1. catalog_scrape.py 2. catalog_compare.py 3. catalog_url_compare.py 4. clean_up.py

## How It Works
The tool utilizes 6 tables to store the scraped metadata records before comparison (master_temp), the most recent version of the metadata records (master), and the previous versions of the metadata records (master_history). The digital object URLs are stored in separate look up tables (object_url, object_url_history, object_url_temp) because one catalog record could have many digital objects. 

The catalog_scrape.py script scrapes and parses the most common metadata fields and writes the results to the master_temp and object_url_temp tables with a timestamp from when the records were scraped. It is currently configured to scrape and monitor only catalog records within Record Group (RG) 612, the Civil Rights Cold Case Records Collection, but you can modify the API query to return anything. Review the [API documentation](https://catalog.archives.gov/api/v2/api-docs/) for the metadata schema.

The catalog_compare.py script compares the contents of the master_temp table against the master table. The compare script uses the NAID (National Archives Identifier) as a key to compare rows between the master_temp and master tables. If the master table is blank then everything from the master_temp table is moved to master. If the contents of master_temp and master tables have a NAID in common then the row is compared. If there's a difference between the two then the newer version of the row is moved to master and older version is moved to master_history with a timestamp of when the records was moved. If there's no difference between the two rows then no action is taken. It ignores the following columns for the comparison but they are still copied the into the appropriate table: inclusive_start_date, inclusive_end_date, coverage_start_date, coverage_end_date, scrape_timestamp. The date fields are ignored due to an issue with the logical date value in the API changing randomly between 01/01/YYYY and 12/31/YYYY.

The catalog_url_compare.py script compares the object url tables in the same way as the catalog_compare.py script.

The clean_up.py script deletes the contents of the master_temp table after comparison.

### To Do
- Add a script to push the tables from PostgreSQL to Google Sheets
- Add a flag field to the history tables to indicate if a row has been removed entirely from the master tables

### FYI
These scripts were written partially with generative AI and I'm not a python, SQL, or Windmill expert so I probably can't help you troubleshoot. 

[![License: Unlicense](https://img.shields.io/badge/license-Unlicense-blue.svg)]([http://unlicense.org/](http://unlicense.org/))
