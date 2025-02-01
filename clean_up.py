import psycopg2
from psycopg2 import sql
import wmill
import tempfile

def main():
    postgres_creds = wmill.get_resource("u/user/db_postgresql")

    # Handle SSL root certificate if provided as PEM
    if "root_certificate_pem" in postgres_creds:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".crt") as temp_cert:
            temp_cert.write(postgres_creds["root_certificate_pem"].encode())
            postgres_creds["sslrootcert"] = temp_cert.name
        del postgres_creds["root_certificate_pem"]

    # Build the connection string
    connection_str = (
        f"postgresql://{postgres_creds['user']}:{postgres_creds['password']}@{postgres_creds['host']}:{postgres_creds['port']}/{postgres_creds['dbname']}"
    )

    # Tables to clear
    tables = ['master_temp', 'object_url_temp']

    try:
        # Connect to the database
        conn = psycopg2.connect(connection_str)
        conn.autocommit = True  # Automatically commit changes
        cursor = conn.cursor()

        # Clear the contents of each table
        for table in tables:
            query = sql.SQL("DELETE FROM {}").format(sql.Identifier(table))
            cursor.execute(query)
            print(f"Cleared contents of table: {table}")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    main()
