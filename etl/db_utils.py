from etl.db import get_connection

def run_sql_scripts(file_paths):
    """Execute a list of SQL scripts to create tables or views."""
    conn = get_connection()
    cur = conn.cursor()
    for path in file_paths:
        with open(path, "r") as f:
            sql = f.read()
            cur.execute(sql)
            print(f"Executed {path}")
    conn.commit()
    cur.close()
    conn.close()
    print("✅ All SQL scripts executed successfully")
