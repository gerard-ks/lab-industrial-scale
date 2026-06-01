CREATE TABLE IF NOT EXISTS inventory_schema.stock (
    product_id VARCHAR(50) PRIMARY KEY,
    quantity INT NOT NULL CHECK (quantity >= 0) -- Sécurité SQL contre les stocks négatifs
);

-- On insère un faux stock pour notre test (On a 1 seul PC en stock !)
INSERT INTO inventory_schema.stock (product_id, quantity) VALUES ('PC_GAMER', 1);
