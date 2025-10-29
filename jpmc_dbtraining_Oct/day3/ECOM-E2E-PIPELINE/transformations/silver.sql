create streaming table dev.silver.sales_cleaned
(
CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
select distinct * from STREAM dev.bronze.sales;

CREATE OR REFRESH STREAMING TABLE dev.silver.products_silver;
CREATE FLOW product_flow
AS AUTO CDC INTO
  silver.products_silver
FROM stream(bronze.products)
  KEYS (product_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY seqNum
  COLUMNS * EXCEPT (operation, seqNum,_rescued_data)
  STORED AS SCD TYPE 1;
  --TRACK HISTORY ON * EXCEPT (city)

  CREATE OR REFRESH STREAMING TABLE silver.customers_silver;
CREATE FLOW customers_flow
AS AUTO CDC INTO
  silver.customers_silver
FROM stream(bronze.customers)
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY sequenceNum
  COLUMNS * EXCEPT (operation, sequenceNum,_rescued_data)
    STORED AS SCD TYPE 2;
