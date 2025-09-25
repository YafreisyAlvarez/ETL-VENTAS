import pandas as pd
import pyodbc

SERVER   = r"DESKTOP-ECEFNK5"
DATABASE = "ventas__db"
USER     = "sa"
PASSWORD = "SAUCEBOYZ"

CNX_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};Encrypt=no;"
)

CSV_DIR = r"C:\Users\LENOVO\Desktop\ETL\csv"

conn = pyodbc.connect(CNX_STR)
cursor = conn.cursor()

# Leer y preparar orders.csv
df = pd.read_csv(f"{CSV_DIR}\\orders.csv")

df = df.rename(columns={
    "OrderID": "id_factura",
    "CustomerID": "id_cliente",
    "OrderDate": "fecha_factura",
    "Status": "estado"
})[["id_factura","id_cliente","fecha_factura","estado"]]

# Normalizar fecha
df["fecha_factura"] = pd.to_datetime(df["fecha_factura"], errors="coerce").dt.date

# eliminar nulos y duplicados
df = df.dropna(subset=["id_factura","id_cliente","fecha_factura"]).drop_duplicates(subset=["id_factura"])

# Insertar
cursor.fast_executemany = True
sql = """
INSERT INTO facturas (id_factura, id_cliente, fecha_factura, estado)
VALUES (?,?,?,?)
"""
cursor.executemany(sql, df.values.tolist())
conn.commit()

print(f"Cargadas {len(df)} facturas.")
