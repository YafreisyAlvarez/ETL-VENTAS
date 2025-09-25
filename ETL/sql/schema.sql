CREATE DATABASE ventas__db;
GO
USE ventas__db;
GO

-- CLIENTES
CREATE TABLE clientes (
    id_cliente       INT           NOT NULL PRIMARY KEY,
    nombre_cliente   VARCHAR(120)  NOT NULL,
    apellido_cliente VARCHAR(120)  NOT NULL,
    email            VARCHAR(255)  NULL,
    telefono         VARCHAR(50)   NULL,
    ciudad           VARCHAR(120)  NULL,
    pais             VARCHAR(120)  NULL
);

-- PRODUCTOS
CREATE TABLE productos (
    id_producto      INT           NOT NULL PRIMARY KEY,
    nombre_producto  VARCHAR(255)  NOT NULL,
    categoria        VARCHAR(120)  NULL,
    precio_unitario  DECIMAL(12,2) NOT NULL CHECK (precio_unitario >= 0),
    stock            INT           NULL CHECK (stock >= 0)
);

-- FACTURAS 
CREATE TABLE facturas (
    id_factura     INT          NOT NULL PRIMARY KEY,
    id_cliente     INT          NOT NULL,
    fecha_factura  DATE         NOT NULL,
    estado         VARCHAR(40)  NULL,
    CONSTRAINT FK_facturas_clientes
        FOREIGN KEY (id_cliente) REFERENCES clientes(id_cliente)
);

-- VENTAS (detalle)
CREATE TABLE ventas (
    id_factura            INT           NOT NULL,
    id_producto           INT           NOT NULL,
    cantidad              INT           NOT NULL CHECK (cantidad > 0),
    total_linea           DECIMAL(14,2) NOT NULL CHECK (total_linea >= 0),
    precio_unitario_venta AS (
        CASE WHEN cantidad > 0
             THEN total_linea / CAST(cantidad AS DECIMAL(14,2))
             ELSE 0 END
    ) PERSISTED,
    CONSTRAINT PK_ventas PRIMARY KEY (id_factura, id_producto),
    CONSTRAINT FK_ventas_facturas  FOREIGN KEY (id_factura)  REFERENCES facturas(id_factura),
    CONSTRAINT FK_ventas_productos FOREIGN KEY (id_producto) REFERENCES productos(id_producto)
);

-- Índices 
CREATE INDEX IX_facturas_cliente_fecha ON facturas(id_cliente, fecha_factura);
CREATE INDEX IX_ventas_producto        ON ventas(id_producto);

