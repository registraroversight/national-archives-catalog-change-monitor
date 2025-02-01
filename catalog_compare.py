import psycopg2
from psycopg2 import sql
from datetime import datetime
import wmill
import tempfile

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

# Columns to ignore during comparison
ignore_columns = {"inclusive_start_date", "inclusive_end_date", "coverage_start_date", "coverage_end_date", "scrape_timestamp"}

# All columns in the tables
all_columns = [
    "naid", "title", "level_of_description", "parent_series_naid", "parent_series_title",
    "parent_file_unit_naid", "parent_file_unit_title", "creator", "inclusive_start_date",
    "inclusive_end_date", "coverage_start_date", "coverage_end_date", "ldr_count",
    "series_extents", "access_restriction_status", "specific_access_restrictions", "accession_numbers",
    "disposition_authority_numbers", "crccrca_number", "scope_and_content_note",
    "function_and_use_note", "general_notes", "scrape_timestamp"
]

# Columns to compare (ignoring specific columns)
compare_columns = [col for col in all_columns if col not in ignore_columns]

def execute_query(conn, query, data=None):
    """Execute an SQL query."""
    with conn.cursor() as cursor:
        cursor.execute(query, data)
        conn.commit()

def fetch_results(conn, query, data=None):
    """Fetch results from a query."""
    with conn.cursor() as cursor:
        cursor.execute(query, data)
        return cursor.fetchall()

def main():
    try:
        conn = psycopg2.connect(connection_str)

        # Insert rows from master_temp not in master
        insert_new_rows_query = sql.SQL("""
            INSERT INTO master ({columns})
            SELECT {temp_columns}
            FROM master_temp t
            WHERE NOT EXISTS (
                SELECT 1 FROM master m WHERE m.naid = t.temp_naid
            )
        """).format(
            columns=sql.SQL(", ").join(map(sql.Identifier, all_columns)),
            temp_columns=sql.SQL(", ").join(sql.Identifier(f"temp_{col}") for col in all_columns),
        )
        execute_query(conn, insert_new_rows_query)

        # Insert rows from master not in master_temp into master_history and delete them from master
        current_timestamp = datetime.now()
        insert_deleted_rows_query = sql.SQL("""
            WITH moved_rows AS (
                DELETE FROM master
                WHERE NOT EXISTS (
                    SELECT 1 FROM master_temp t WHERE t.temp_naid = master.naid
                )
                RETURNING *, %s AS h_history_timestamp
            )
            INSERT INTO master_history ({history_columns})
            SELECT {master_columns}, h_history_timestamp
            FROM moved_rows
        """).format(
            history_columns=sql.SQL(", ").join(sql.Identifier(f"h_{col}") for col in all_columns + ["history_timestamp"]),
            master_columns=sql.SQL(", ").join(sql.Identifier(col) for col in all_columns),
        )
        execute_query(conn, insert_deleted_rows_query, (current_timestamp,))

        # Compare rows in both tables
        common_rows_query = sql.SQL("""
            SELECT m.naid, {master_columns}, {temp_columns}
            FROM master m
            JOIN master_temp t ON m.naid = t.temp_naid
        """).format(
            master_columns=sql.SQL(", ").join(sql.SQL(f"m.{col}") for col in compare_columns),
            temp_columns=sql.SQL(", ").join(sql.SQL(f"t.temp_{col}") for col in compare_columns),
        )
        common_rows = fetch_results(conn, common_rows_query)

        for row in common_rows:
            naid = row[0]
            master_values = row[1:len(compare_columns) + 1]
            temp_values = row[len(compare_columns) + 1:]

            # Identify differences
            differences = {
                col: temp_val
                for col, master_val, temp_val in zip(compare_columns, master_values, temp_values)
                if master_val != temp_val
            }

            if differences:
                # Insert old row into master_history
                insert_history_query = sql.SQL("""
                    INSERT INTO master_history ({history_columns})
                    SELECT {master_columns}, %s AS h_history_timestamp
                    FROM master WHERE naid = %s
                """).format(
                    history_columns=sql.SQL(", ").join(sql.Identifier(f"h_{col}") for col in all_columns + ["history_timestamp"]),
                    master_columns=sql.SQL(", ").join(sql.Identifier(col) for col in all_columns),
                )
                execute_query(conn, insert_history_query, (current_timestamp, naid))

                # Remove old row from master
                delete_old_row_query = "DELETE FROM master WHERE naid = %s"
                execute_query(conn, delete_old_row_query, (naid,))

                # Insert new row from master_temp into master
                insert_new_row_query = sql.SQL("""
                    INSERT INTO master ({columns})
                    SELECT {temp_columns}
                    FROM master_temp WHERE temp_naid = %s
                """).format(
                    columns=sql.SQL(", ").join(map(sql.Identifier, all_columns)),
                    temp_columns=sql.SQL(", ").join(sql.Identifier(f"temp_{col}") for col in all_columns),
                )
                execute_query(conn, insert_new_row_query, (naid,))

        print("Synchronization complete.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
