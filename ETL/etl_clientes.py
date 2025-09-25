import pandas as pd
import pyodbc

# Configuración de conexión
SERVER   = r"DESKTOP-ECEFNK5"   
DATABASE = "ventas__db"
USER     = "sa"                 
PASSWORD = "SAUCEBOYZ"     

CNX_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};Encrypt=no;"
)

# Ruta CSV
CSV_DIR  = r"C:\Users\LENOVO\Desktop\ETL\csv"

# Conexión
conn = pyodbc.connect(CNX_STR)
cursor = conn.cursor()

# Leer clientes.csv
df = pd.read_csv(f"{CSV_DIR}\\customers.csv")

# Renombrar columnas
df = df.rename(columns={
    "CustomerID": "id_cliente",
    "FirstName": "nombre_cliente",
    "LastName": "apellido_cliente",
    "Email": "email",
    "Phone": "telefono",
    "City": "ciudad",
    "Country": "pais"
})[["id_cliente","nombre_cliente","apellido_cliente","email","telefono","ciudad","pais"]]

# Insertar en SQL
cursor.fast_executemany = True
sql = """
INSERT INTO clientes (id_cliente,nombre_cliente,apellido_cliente,email,telefono,ciudad,pais)
VALUES (?,?,?,?,?,?,?)
"""
cursor.executemany(sql, df.values.tolist())
conn.commit()

print(f"Cargados {len(df)} clientes.")
