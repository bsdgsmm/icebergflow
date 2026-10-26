# Data dictionary

## Source files (`data/raw/<batch_id>/`)

| File | Format | Columns |
|---|---|---|
| `customers.csv` | CSV with header | `customer_id`, `first_name`, `last_name`, `email`, `country`, `signup_date`, `updated_at` |
| `products.csv` | CSV with header | `product_id`, `product_name`, `category`, `brand`, `unit_price`, `is_active`, `updated_at` |
| `orders.jsonl` | JSON Lines | `order_id`, `customer_id`, `order_ts`, `status`, `currency`, `updated_at`; from v2 also `channel`, `coupon_code` |
| `order_items.jsonl` | JSON Lines | `order_id`, `line_number`, `product_id`, `quantity`, `unit_price`, `discount` |
| `_manifest.json` | JSON | `batch_id`, `schema_version`, `row_counts` per file, `injected_anomalies`, `duplicates`, generator settings |

Timestamps are ISO 8601 in UTC (`2024-01-05T14:03:11Z`). Identifiers use the prefixes `CUS-`,
`PRD-` and `ORD-`.

## Bronze

All four Bronze tables contain the source columns of their file as `STRING`, followed by:

| Column | Type | Description |
|---|---|---|
| `_batch_id` | STRING | Source batch (directory name); identity partition |
| `_source_file` | STRING | Path relative to the raw directory, e.g. `2024-01-05/orders.jsonl` |
| `_ingested_at` | TIMESTAMP | Time of the load |
| `_record_hash` | STRING | SHA-256 of the source columns serialized as JSON |
| `_corrupt_record` | STRING | Raw text of an unparseable JSON line, otherwise NULL |

Every Bronze append carries two snapshot summary properties: `icebergflow.batch-id` and
`icebergflow.source-rows`.

## Silver

### `silver.customers`

| Column | Type | Description |
|---|---|---|
| `customer_id` | STRING | Business key |
| `first_name`, `last_name` | STRING | Customer name |
| `email` | STRING | Lower-cased, validated address |
| `country` | STRING | ISO 3166-1 alpha-2 code |
| `signup_date` | DATE | Registration date |
| `updated_at` | TIMESTAMP | Version timestamp from the source |
| `_batch_id` | STRING | Batch that delivered the current version |
| `_processed_at` | TIMESTAMP | Time of the Silver merge that wrote the row |

### `silver.products`

| Column | Type | Description |
|---|---|---|
| `product_id` | STRING | Business key |
| `product_name`, `category`, `brand` | STRING | Descriptive attributes |
| `unit_price` | DECIMAL(10,2) | Current list price, at least 0 |
| `is_active` | BOOLEAN | Listing flag |
| `updated_at` | TIMESTAMP | Version timestamp |
| `_batch_id`, `_processed_at` | | Lineage, as above |

### `silver.orders` (partitioned by `days(order_ts)`)

| Column | Type | Description |
|---|---|---|
| `order_id` | STRING | Business key |
| `customer_id` | STRING | References `silver.customers` |
| `order_ts` | TIMESTAMP | Time the order was placed |
| `order_date` | DATE | `to_date(order_ts)`, used by Gold |
| `status` | STRING | `placed`, `shipped`, `delivered`, `cancelled` or `returned` (latest) |
| `currency` | STRING | ISO 4217 code |
| `updated_at` | TIMESTAMP | Time of the latest status change |
| `channel` | STRING | `web`, `mobile_app` or `marketplace`; NULL before the v2 feed |
| `coupon_code` | STRING | Upper-cased coupon, NULL if none |
| `_batch_id`, `_processed_at` | | Lineage |

### `silver.order_items`

| Column | Type | Description |
|---|---|---|
| `order_id` | STRING | Key part; references `silver.orders` |
| `line_number` | INT | Key part, at least 1 |
| `product_id` | STRING | References `silver.products` |
| `quantity` | INT | Greater than 0 |
| `unit_price` | DECIMAL(10,2) | Price charged per unit |
| `discount` | DECIMAL(4,3) | Fraction in [0, 1); missing values become 0 |
| `line_amount` | DECIMAL(12,2) | `ROUND(quantity × unit_price × (1 − discount), 2)` |
| `_batch_id`, `_processed_at` | | Lineage |

### `silver.quarantine` (partitioned by `entity`)

| Column | Type | Description |
|---|---|---|
| `entity` | STRING | `customers`, `products`, `orders` or `order_items` |
| `batch_id` | STRING | Source batch of the rejected record |
| `record_hash` | STRING | `_record_hash` from Bronze |
| `reasons` | ARRAY&lt;STRING&gt; | Every rule the record failed |
| `record` | STRING | Raw source record as JSON (or the corrupt line) |
| `quarantined_at` | TIMESTAMP | Time of the merge |

## Gold

### `gold.daily_sales` (partitioned by `months(order_date)`)

| Column | Type | Description |
|---|---|---|
| `order_date` | DATE | Grain |
| `orders` | BIGINT | All orders placed that day |
| `net_orders` | BIGINT | Orders currently placed, shipped or delivered |
| `cancelled_orders`, `returned_orders` | BIGINT | Orders in those final states |
| `unique_customers` | BIGINT | Distinct customers ordering that day |
| `units_sold` | BIGINT | Units in net orders |
| `gross_revenue` | DECIMAL(14,2) | Line amounts of all orders |
| `net_revenue` | DECIMAL(14,2) | Line amounts of net orders |
| `avg_order_value` | DECIMAL(12,2) | `net_revenue / net_orders` |

### `gold.channel_daily` (partitioned by `months(order_date)`)

| Column | Type | Description |
|---|---|---|
| `order_date`, `channel` | DATE, STRING | Grain; `channel` is `unknown` before v2 |
| `net_orders` | BIGINT | Net orders |
| `coupon_orders` | BIGINT | Net orders with a coupon |
| `net_revenue` | DECIMAL(14,2) | Net revenue |
| `avg_order_value` | DECIMAL(12,2) | Mean net order amount |

### `gold.product_performance`

| Column | Type | Description |
|---|---|---|
| `product_id` | STRING | Grain; every Silver product, sold or not |
| `product_name`, `category`, `brand` | STRING | Attributes |
| `current_unit_price` | DECIMAL(10,2) | Latest list price |
| `units_sold`, `orders` | BIGINT | Sales in net orders |
| `net_revenue` | DECIMAL(14,2) | Net revenue |
| `first_sold_date`, `last_sold_date` | DATE | Sales date range |
| `category_revenue_rank` | INT | Rank by net revenue within the category |

### `gold.category_monthly`

| Column | Type | Description |
|---|---|---|
| `order_month`, `category` | DATE, STRING | Grain (month as its first day) |
| `units_sold`, `orders` | BIGINT | Net sales |
| `net_revenue` | DECIMAL(14,2) | Net revenue |
| `revenue_share` | DECIMAL(6,4) | Share of the month's net revenue |

### `gold.customer_summary`

| Column | Type | Description |
|---|---|---|
| `customer_id` | STRING | Grain; every Silver customer |
| `country`, `signup_date` | STRING, DATE | Attributes |
| `first_order_date`, `last_order_date` | DATE | Net order range, NULL if never ordered |
| `order_count` | BIGINT | Net orders |
| `lifetime_revenue` | DECIMAL(14,2) | Net revenue |
| `avg_order_value` | DECIMAL(12,2) | `lifetime_revenue / order_count` |
| `is_repeat_customer` | BOOLEAN | `order_count >= 2` |
| `days_to_second_order` | INT | Days from first to second net order |

### `gold.repeat_purchase_cohorts`

| Column | Type | Description |
|---|---|---|
| `cohort_month` | DATE | Grain: month of the first net order |
| `cohort_customers`, `repeat_customers` | BIGINT | Cohort size and repeat buyers |
| `repeat_rate` | DECIMAL(6,4) | `repeat_customers / cohort_customers` |
| `repeat_within_7_days`, `repeat_within_30_days` | BIGINT | Second order within N days |
| `avg_orders_per_customer` | DECIMAL(8,2) | Mean net orders |
| `avg_days_to_second_order` | DECIMAL(8,2) | Mean, over repeat buyers |

## Operational tables

### `ops.watermarks`

| Column | Type | Description |
|---|---|---|
| `consumer` | STRING | Silver table, e.g. `silver.orders` |
| `source_table` | STRING | Bronze table, e.g. `bronze.orders` |
| `snapshot_id` | BIGINT | Last Bronze snapshot fully processed |
| `updated_at` | TIMESTAMP | Time of the update |

### `ops.dq_results` (partitioned by `days(checked_at)`)

| Column | Type | Description |
|---|---|---|
| `run_id` | STRING | One id per suite execution |
| `suite` | STRING | `silver`, `gold` or `adhoc` |
| `check_name` | STRING | For example `unique:silver.orders(order_id)` |
| `table_name` | STRING | Table under test |
| `severity` | STRING | `error` (fails the run) or `warn` |
| `failures` | BIGINT | Number of violating rows (or 1 for a failed reconciliation) |
| `passed` | BOOLEAN | `failures = 0` |
| `checked_at` | TIMESTAMP | Execution time |
