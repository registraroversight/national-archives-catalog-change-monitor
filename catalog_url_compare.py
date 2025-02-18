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
ignore_columns = {"scrape_timestamp"}

# All columns in the tables
all_columns = [
    "naid", "digital_object_url", "digital_object_id", "scrape_timestamp"
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

        # Insert rows from object_url_temp not in object_url
        insert_new_rows_query = sql.SQL("""
            INSERT INTO object_url ({columns})
            SELECT {temp_columns}
            FROM object_url_temp t
            WHERE NOT EXISTS (
                SELECT 1 FROM object_url o WHERE o.digital_object_id = t.temp_digital_object_id
            )
        """).format(
            columns=sql.SQL(", ").join(map(sql.Identifier, all_columns)),
            temp_columns=sql.SQL(", ").join(sql.Identifier(f"temp_{col}") for col in all_columns),
        )
        execute_query(conn, insert_new_rows_query)

        # Insert rows from object_url not in object_url_temp into object_url_history before deleting them
        current_timestamp = datetime.now()

        # Step 1: Move deleted rows to history
        insert_deleted_rows_query = sql.SQL("""
            WITH moved_rows AS (
                DELETE FROM object_url
                WHERE NOT EXISTS (
                    SELECT 1 FROM object_url_temp t WHERE t.temp_digital_object_id = object_url.digital_object_id
                )
                RETURNING *, %s AS h_history_timestamp, TRUE AS h_deleted_from_object_url
            )
            INSERT INTO object_url_history ({history_columns})
            SELECT {master_columns}, h_history_timestamp, h_deleted_from_object_url
            FROM moved_rows
        """).format(
            history_columns=sql.SQL(", ").join(sql.Identifier(f"h_{col}") for col in all_columns + ["history_timestamp", "deleted_from_object_url"]),
            master_columns=sql.SQL(", ").join(sql.Identifier(col) for col in all_columns),
        )
        execute_query(conn, insert_deleted_rows_query, (current_timestamp,))

        # Compare rows in both tables
        common_rows_query = sql.SQL("""
            SELECT o.digital_object_id, {master_columns}, {temp_columns}
            FROM object_url o
            JOIN object_url_temp t ON o.digital_object_id = t.temp_digital_object_id
        """).format(
            master_columns=sql.SQL(", ").join(sql.SQL(f"o.{col}") for col in compare_columns),
            temp_columns=sql.SQL(", ").join(sql.SQL(f"t.temp_{col}") for col in compare_columns),
        )
        common_rows = fetch_results(conn, common_rows_query)

        for row in common_rows:
            digital_object_id = row[0]
            master_values = row[1:len(compare_columns) + 1]
            temp_values = row[len(compare_columns) + 1:]

            # Identify differences
            differences = {
                col: temp_val
                for col, master_val, temp_val in zip(compare_columns, master_values, temp_values)
                if master_val != temp_val
            }

            if differences:
                # Insert old row into object_url_history before updating
                insert_history_query = sql.SQL("""
                    INSERT INTO object_url_history ({history_columns})
                    SELECT {master_columns}, %s AS h_history_timestamp, FALSE AS h_deleted_from_object_url
                    FROM object_url WHERE digital_object_id = %s
                """).format(
                    history_columns=sql.SQL(", ").join(sql.Identifier(f"h_{col}") for col in all_columns + ["history_timestamp", "deleted_from_object_url"]),
                    master_columns=sql.SQL(", ").join(sql.Identifier(col) for col in all_columns),
                )
                execute_query(conn, insert_history_query, (current_timestamp, digital_object_id))

                # Remove old row from object_url
                delete_old_row_query = "DELETE FROM object_url WHERE digital_object_id = %s"
                execute_query(conn, delete_old_row_query, (digital_object_id,))

                # Insert new row from object_url_temp into object_url
                insert_new_row_query = sql.SQL("""
                    INSERT INTO object_url ({columns})
                    SELECT {temp_columns}
                    FROM object_url_temp WHERE temp_digital_object_id = %s
                """).format(
                    columns=sql.SQL(", ").join(map(sql.Identifier, all_columns)),
                    temp_columns=sql.SQL(", ").join(sql.Identifier(f"temp_{col}") for col in all_columns),
                )
                execute_query(conn, insert_new_row_query, (digital_object_id,))

        print("Synchronization complete.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
