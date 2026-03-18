import snowflake.connector
import pandas as pd

# --- CONNECT TO SNOWFLAKE ---
conn = snowflake.connector.connect(
    user="tim3lrom",
    password="@Eltm1975-snowflake",
    account="cwc48497.us-east-1",   # from your URL
    warehouse="COMPUTE_WH",
    database="VALUFLOW",
    schema="BALANCE"
)

# --- TEST QUERY ---
query = "SELECT CURRENT_VERSION()"

# --- EXECUTE QUERY ---
cur = conn.cursor()
cur.execute(query)

# --- LOAD INTO PANDAS ---
df = pd.DataFrame(cur.fetchall(), columns=[col[0] for col in cur.description])

# --- OUTPUT ---
print("\nConnection successful. Snowflake version:\n")
print(df)

# --- CLEAN UP ---
cur.close()
conn.close()