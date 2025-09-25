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

# Conexión
conn = pyodbc.connect(CNX_STR)

# 1) Traer claves válidas para respetar FKs
facturas_ids  = set(pd.read_sql("SELECT id_factura FROM facturas", conn)["id_factura"])
productos_ids = set(pd.read_sql("SELECT id_producto FROM productos", conn)["id_producto"])

# 2) Leer CSV y renombrar
df = pd.read_csv(f"{CSV_DIR}\\order_details.csv").rename(columns={
    "OrderID": "id_factura",
    "ProductID": "id_producto",
    "Quantity": "cantidad",
    "TotalPrice": "total_linea"
})[["id_factura","id_producto","cantidad","total_linea"]]

# 3) Tipos + limpieza
df["cantidad"]    = pd.to_numeric(df["cantidad"], errors="coerce").fillna(0).astype(int)
df["total_linea"] = pd.to_numeric(df["total_linea"], errors="coerce").fillna(0)

# Filtrar inválidos
df = df[(df["cantidad"] > 0) & (df["total_linea"] >= 0)]
df = df[df["id_factura"].isin(facturas_ids)]
df = df[df["id_producto"].isin(productos_ids)]

# 4) Agregar por si existe el mismo producto repetido en la misma factura
df = df.groupby(["id_factura","id_producto"], as_index=False).agg(
    cantidad=("cantidad","sum"),
    total_linea=("total_linea","sum")
)

# 5) Insertar
cursor = conn.cursor()
cursor.fast_executemany = True
sql = """
INSERT INTO ventas (id_factura, id_producto, cantidad, total_linea)
VALUES (?,?,?,?)
"""
cursor.executemany(sql, df.values.tolist())
conn.commit()

print(f"Cargadas {len(df)} líneas de venta.")
