select customer_id,
        name,
        UPPER(email) AS email,
        trim(phone) AS phone
from bronze.customer_bronze