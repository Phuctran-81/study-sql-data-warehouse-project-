
## 1. gold.dim_customers
- **Purpose:** Stores customer details enriched with demographic and geographic data.
- **Columns:**

| Column Name | Data Type | Description |
| :--- |:--- | :--- |
| customer_key | INT | Surrogate key uniquely identifying each customer record in the dimension table.|
| customer_id | INT | Unique numerical identifier assigned to each customer. |
| customer_number | NVARCHAR | Alphanumeric identifier representing the customer, used for tracking and referencing. |
| first_name | NVARCHAR | The customer's first name, as recorded in the system. |
| last_name | NVARCHAR | The customer's last name or family name. |
| marital_status | NVARCHAR | The marital status of the customer (e.g., 'Married', 'Single'). |
| gender | NVARCHAR | The gender of customer (e.g., 'Male', 'Female') |
| birthdate | DATE | The date of birth of the customer, formatted as YYYY-MM-DD (e.g., 1971-10-06). |
| country | NVARCHAR | The country of residence for the customer (e.g., 'Australia'). |
| create_date | DATE | The date and time when the customer record was created in the system. |

## 2. gold.dim_products
- **Purpose:** Provide information about the products and their attributes.
- **Columns:**
  
|Column Name | Data Type | Description |
| :--- | :--- | :--- |
| product_key | INT | Surrogate key uniquely identifying each product record in the product dimension table. |
| product_id | INT | A structured alphanumeric code representing the product, often used for categorization or inventory. |
| product_number | NVARCHAR | A structured alphanumeric code representing the product, often used for categorization or inventory. |
| product_name | NVARCHAR | Descriptive name of the product, including key details such as type, color, and size. |
| category_id | NVARCHAR | A unique identifier for the product's category, linking to its high-level classification. |
| category_name | NVARCHAR | A detailed classification of the product. |
| subcategory_name | NVARCHAR | A more detailed classification of the product within the category, such as product type. |
| product_cost | INT | The cost or base price of the product, measured in monetary units |
| product_line | NVARCHAR | The specific product line or series to which the product belongs (e.g., Road, Mountain). |
| maintenance | NVARCHAR | Indicates whether the product requires maintenance (e.g., 'Yes', 'No') |
| start_date | DATE | The date when the product became available for sale or use, stored in |

## 3. gold.fact_sales
- **Purpose:** Stores transactional sales datafor analytical purposes.
- **Columns:**
  
| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| order_number | NVARCHAR | A unique alphanumeric identifier for each sales order (e.g., 'SO54496') |
| product_key | INT | Surrogate key linking the order to the product dimension table. |
| customer_key | INT | Surrogate key linking the order to the customer dimension table. |
| order_date | DATE | The date when the order was placed. |
| shipping_date | DATE | the date when the order was shipped to the customer. |
| due_date | DATE | The date when the order payment was due. |
| sales_amount | INT | The total monetary value of the sale for the line item, in whole currency units (e.g., 25). |
| quantity | INT | The number of units of the product ordered for the line item (e.g., 1) |
| price | INT | The price per unit of the proudct for the line item, in whole currency units (e.g., 25) |
