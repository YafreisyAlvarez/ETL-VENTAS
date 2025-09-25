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

# Carga de productos 
conn = pyodbc.connect(CNX_STR)
cursor = conn.cursor()

df = pd.read_csv(f"{CSV_DIR}\\products.csv")

# Renombrar columnas al esquema de la table
df = df.rename(columns={
    "ProductID": "id_producto",
    "ProductName": "nombre_producto",
    "Category": "categoria",
    "Price": "precio_unitario",
    "Stock": "stock"
})[["id_producto","nombre_producto","categoria","precio_unitario","stock"]]

# Validaciones de tipos
df["precio_unitario"] = pd.to_numeric(df["precio_unitario"], errors="coerce").fillna(0)
df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0).astype(int)

# Insertar 
cursor.fast_executemany = True
sql = """
INSERT INTO productos (id_producto,nombre_producto,categoria,precio_unitario,stock)
VALUES (?,?,?,?,?)
"""
cursor.executemany(sql, df.values.tolist())
conn.commit()

print(f"Cargados {len(df)} productos.")
