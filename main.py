import sqlite3


# Paths to your database files
source_db1 = "accounts_old.db"  # Path to the first database
source_db2 = "accounts_new.db"  # Path to the second database
target_db = "accounts.db"  # Path to the output merged database


def merge_databases(source_db1, source_db2, target_db):
    """
    Merge two SQLite databases into a single target database.

    :param source_db1: Path to the first source database file.
    :param source_db2: Path to the second source database file.
    :param target_db: Path to the target (merged) database file.
    """
    # Connect to the source and target databases
    conn1 = sqlite3.connect(source_db1)
    conn2 = sqlite3.connect(source_db2)
    conn_target = sqlite3.connect(target_db)

    # Attach source databases to the target database
    conn_target.execute(f"ATTACH DATABASE '{source_db1}' AS db1;")
    conn_target.execute(f"ATTACH DATABASE '{source_db2}' AS db2;")

    # Get the list of tables from the first database
    cursor = conn1.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    # Loop through tables and merge data
    for table in tables:
        print(f"Merging table: {table}")

        # Create the table in the target database if it doesn't exist
        create_table_query = conn1.execute(
            f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}';"
        ).fetchone()[0]
        conn_target.execute(create_table_query)

        # Insert data from db1 into the target database
        conn_target.execute(f"INSERT INTO {table} SELECT * FROM db1.{table};")

        # Insert non-conflicting data from db2 into the target database
        non_conflicting_query = f"""
        INSERT INTO {table}
        SELECT * FROM db2.{table}
        WHERE rowid NOT IN (
            SELECT rowid FROM db1.{table}
        );
        """
        conn_target.execute(non_conflicting_query)

    # Commit and close connections
    conn_target.commit()
    conn1.close()
    conn2.close()
    conn_target.close()
    print("Merge completed successfully!")


# Call the merge function
merge_databases(source_db1, source_db2, target_db)
