SELECT *
FROM bronze.customer_bronze
WHERE batch_id = (
    SELECT MAX(batch_id)
    FROM bronze.customer_bronze
)

