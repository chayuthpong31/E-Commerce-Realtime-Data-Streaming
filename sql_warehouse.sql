-- Create gold_orders_managed table
CREATE OR REPLACE TALBE default.gold_orders_managed
AS SELECT * FROM delta.{gold_path}

-- Test SQL Warehouse by Query all data
SELECT * FROM default.gold_orders_managed;