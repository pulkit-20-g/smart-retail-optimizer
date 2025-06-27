SELECT * FROM public.inventory_log
ORDER BY log_date ASC, store_key ASC, product_key ASC;
-- latest inventory data on the last date
WITH latest_logs AS (
  SELECT *
  FROM inventory_log
  WHERE log_date = (SELECT MAX(log_date) FROM inventory_log)
)
--select * from latest_logs;
SELECT 
  s.store_id,
  p.product_id,
  l.log_date AS last_updated,
  l.inventory,
  l.demand
FROM latest_logs l
JOIN store s ON l.store_key = s.store_key
JOIN product p ON l.product_key = p.product_key
ORDER BY s.store_id, p.product_id;

-- finding understocked rows and the inventory gap between inventory and demand on inventory_log
SELECT 
  l.log_date,
  s.store_id,
  p.product_id,
  l.inventory,
  l.demand,
  (l.inventory - l.demand) AS inventory_gap
FROM inventory_log l
JOIN store s ON l.store_key = s.store_key
JOIN product p ON l.product_key = p.product_key
WHERE l.inventory < l.demand
ORDER BY l.log_date, s.store_id, p.product_id;

-- finding the percentage of understocking of each product in each store
WITH all_days AS (
  SELECT 
    l.log_date,
    s.store_id,
    p.product_id,
    (l.inventory < l.demand)::int AS understock_flag --::int converts boolean result into integral values 
  FROM inventory_log l
  JOIN store s ON l.store_key = s.store_key
  JOIN product p ON l.product_key = p.product_key
)
--select * from all_days;
SELECT 
  store_id,
  product_id,
  COUNT(*) AS total_days,
  SUM(understock_flag) AS understock_days,
  ROUND(100.0 * SUM(understock_flag) / COUNT(*), 2) AS understock_pct
FROM all_days
GROUP BY store_id, product_id
ORDER BY understock_pct DESC;

-- identifying fast or slow moving products
-- let us consider velocity as avg units sold per day
with product_velocity as(
	select
	p.product_id,
	ROUND(AVG(l.units_sold),2) as avg_units_sold_per_day -- 2 represents roundoing upto 2 decimal places 
	from inventory_log l
	join product p on l.product_key = p.product_key
	group by p.product_id
	)
--select * from product_velocity;
SELECT *,
  CASE 
    WHEN avg_units_sold_per_day >= 100 THEN 'Fast-Moving'
    WHEN avg_units_sold_per_day BETWEEN 90 AND 100 THEN 'Medium-Moving'
    ELSE 'Slow-Moving'
  END AS movement_category
FROM product_velocity
ORDER BY avg_units_sold_per_day DESC;

WITH store_product_velocity AS (
  SELECT 
    s.store_id,
    p.product_id,
    ROUND(AVG(l.units_sold), 2) AS avg_units_sold_per_day
  FROM inventory_log l
  JOIN store s ON l.store_key = s.store_key
  JOIN product p ON l.product_key = p.product_key
  GROUP BY s.store_id, p.product_id
)
SELECT *,
  CASE 
    WHEN avg_units_sold_per_day >= 100 THEN 'Fast-Moving'
    WHEN avg_units_sold_per_day BETWEEN 90 AND 100 THEN 'Medium-Moving'
    ELSE 'Slow-Moving'
  END AS movement_category
FROM store_product_velocity
ORDER BY store_id, avg_units_sold_per_day DESC;
--select * from inventory_log;
select
l.log_date,
s.store_id,
p.product_id,
l.units_sold,
l.inventory,
l.units_ord
from inventory_log l
join product p on p.product_key = l.product_key
join store s on s.store_key = l.store_key
order by store_id, product_id,l.log_date Desc;

-- generating Cumilative orders column for the time calculations
SELECT
  il.log_date,
  s.store_id,
  p.product_id,
  il.units_ord,
  SUM(il.units_ord) OVER (
    PARTITION BY s.store_id, p.product_id
    ORDER BY il.log_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_units_ordered
FROM inventory_log il
JOIN store s ON il.store_key = s.store_key
JOIN product p ON il.product_key = p.product_key
ORDER BY s.store_id, p.product_id, il.log_date;

-- Claculating the lag time by incoorporating the cumilative orders 
CREATE TABLE avg_lag_time_per_product_store AS
WITH log_with_cumulative_orders AS (
  SELECT
    il.log_date,
    s.store_id,
    p.product_id,
    il.inventory,
    il.units_ord,
    SUM(il.units_ord) OVER (
      PARTITION BY s.store_id, p.product_id
      ORDER BY il.log_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_ordered
  FROM inventory_log il
  JOIN store s ON il.store_key = s.store_key
  JOIN product p ON il.product_key = p.product_key
),
base AS (
  SELECT
    log_date,
    store_id,
    product_id,
    inventory,
    cumulative_ordered,
    LAG(inventory) OVER (PARTITION BY store_id, product_id ORDER BY log_date) AS prev_inventory,
    LAG(cumulative_ordered) OVER (PARTITION BY store_id, product_id ORDER BY log_date) AS prev_ordered
  FROM log_with_cumulative_orders
),
orders AS (
  SELECT
    store_id,
    product_id,
    log_date AS order_date,
    cumulative_ordered - COALESCE(prev_ordered, 0) AS ordered_delta
  FROM base
  WHERE (cumulative_ordered - COALESCE(prev_ordered, 0)) > 0
),
delivery AS (
  SELECT
    o.store_id,
    o.product_id,
    o.order_date,
    MIN(b.log_date) AS delivery_date,
    MIN(b.log_date) - o.order_date AS lag_days
  FROM orders o
  JOIN base b 
    ON b.store_id = o.store_id 
    AND b.product_id = o.product_id 
    AND b.log_date > o.order_date
    AND (b.inventory - COALESCE(b.prev_inventory, 0)) > 0
  GROUP BY o.store_id, o.product_id, o.order_date
),
avg_lag_by_product_store AS (
  SELECT
    store_id,
    product_id,
    ROUND(AVG(lag_days), 2) AS avg_lag_days,
    COUNT(*) AS deliveries_count
  FROM delivery
  GROUP BY store_id, product_id
);
SELECT *
FROM avg_lag_time_per_product_store;

-- Clculating Reorder Point 
WITH avg_demand AS (
  SELECT
    s.store_id,
    p.product_id,
    ROUND(AVG(il.units_sold), 2) AS avg_daily_demand
  FROM inventory_log il
  JOIN store s ON il.store_key = s.store_key
  JOIN product p ON il.product_key = p.product_key
  GROUP BY s.store_id, p.product_id
)
, reorder_calc AS (
  SELECT
    d.store_id,
    d.product_id,
    d.avg_daily_demand,
    l.avg_lag_days,
   ROUND((avg_daily_demand * avg_lag_days) + (0.1 * avg_daily_demand)) AS reorder_point -- Added 10% safety stock 
  FROM avg_demand d
  JOIN avg_lag_time_per_product_store l
    ON d.store_id = l.store_id AND d.product_id = l.product_id
)
SELECT
  store_id,
  product_id,
  avg_daily_demand,
  avg_lag_days,
  reorder_point
FROM reorder_calc
ORDER BY store_id, product_id;

-- Saving reorder point to the average demand table
ALTER TABLE avg_lag_time_per_product_store
ADD COLUMN reorder_point NUMERIC;

WITH avg_demand AS (
  SELECT
    s.store_id,
    p.product_id,
    ROUND(AVG(il.units_sold), 2) AS avg_daily_demand
  FROM inventory_log il
  JOIN store s ON il.store_key = s.store_key
  JOIN product p ON il.product_key = p.product_key
  GROUP BY s.store_id, p.product_id
)
UPDATE avg_lag_time_per_product_store lag
SET reorder_point = ROUND((d.avg_daily_demand * lag.avg_lag_days) + (0.1 * d.avg_daily_demand))
FROM avg_demand d
WHERE lag.store_id = d.store_id AND lag.product_id = d.product_id;

select * from avg_lag_time_per_product_store;

--Inventory Turnover analysis
select * from inventory_log;
--Monthly inventory turnover based on goods sold 
WITH sales_inventory AS (
  SELECT
    s.store_id,
    p.product_id,
    DATE_TRUNC('month', il.log_date) AS month,
    SUM(il.units_sold) AS total_units_sold,
    ROUND(AVG(il.inventory), 2) AS avg_inventory
  FROM inventory_log il
  JOIN store s ON il.store_key = s.store_key
  JOIN product p ON il.product_key = p.product_key
  GROUP BY s.store_id, p.product_id, DATE_TRUNC('month', il.log_date)
),
turnover AS (
  SELECT
    store_id,
    product_id,
    month,
    total_units_sold,
    avg_inventory,
    ROUND(total_units_sold / NULLIF(avg_inventory, 0), 2) AS turnover_ratio,
    ROUND(365 / NULLIF(total_units_sold / NULLIF(avg_inventory, 0), 0), 2) AS days_on_hand
  FROM sales_inventory
)
SELECT *
FROM turnover
where month = '2022-01-01 00:00:00+05:30'
ORDER BY store_id, product_id, month;
SELECT 
  store_id,
  product_id,
  month,
  days_on_hand,
  turnover_ratio,
  CASE
    WHEN days_on_hand < 18 THEN 'Fast-Moving'
    WHEN days_on_hand BETWEEN 18 AND 20 THEN 'Medium-Moving'
    ELSE 'Slow-Moving'
  END AS movement_category
FROM turnover
ORDER BY month, store_id, product_id;

-- Category wise sales 
Select * from product
order by Category;

SELECT 
    s.store_id,
    p.category,
    EXTRACT(YEAR FROM il.log_date) AS year,
    EXTRACT(QUARTER FROM il.log_date) AS quarter,
    SUM(il.units_sold) AS total_units_sold
FROM inventory_log il
JOIN store s ON il.store_key = s.store_key
JOIN product p ON il.product_key = p.product_key
GROUP BY s.store_id, p.category, EXTRACT(YEAR FROM il.log_date), EXTRACT(QUARTER FROM il.log_date)
ORDER BY s.store_id, p.category, year, quarter;

-- Seasons Vs categories sales 
select * from season;
SELECT 
    s.store_id,
    p.category,
    se.season_name,
    EXTRACT(YEAR FROM il.log_date) AS year,
    SUM(il.units_sold) AS total_units_sold
FROM inventory_log il
JOIN store s ON il.store_key = s.store_key
JOIN product p ON il.product_key = p.product_key
JOIN season se ON il.season_key = se.season_key
GROUP BY s.store_id, p.category, se.season_name, EXTRACT(YEAR FROM il.log_date)
ORDER BY s.store_id, p.category, year, se.season_name;

-- Sales Vs Demand 
SELECT 
    s.store_id,
    p.product_id,
    ROUND(AVG(il.units_sold), 2) AS avg_units_sold,
    ROUND(AVG(il.demand), 2) AS avg_demand
FROM inventory_log il
JOIN store s ON il.store_key = s.store_key
JOIN product p ON il.product_key = p.product_key
GROUP BY s.store_id, p.product_id, p.category
ORDER BY s.store_id, p.product_id;

-- Sales with promotions and without 
SELECT
    s.store_id,
    p.product_id,
    p.category,
    il.holiday_flag AS is_promoted,
    ROUND(SUM(il.units_sold) * 1.0 / COUNT(DISTINCT il.log_date), 2) AS avg_units_sold_per_day
FROM inventory_log il
JOIN store s ON il.store_key = s.store_key
JOIN product p ON il.product_key = p.product_key
GROUP BY s.store_id, p.product_id, p.category, il.holiday_flag
ORDER BY s.store_id, p.product_id, il.holiday_flag;

-- By weather 
select * from weather_condition;
SELECT
    s.store_id,
    p.product_id,
    p.category,
    w.weather_type,
    ROUND(SUM(il.units_sold) * 1.0 / COUNT(DISTINCT il.log_date), 2) AS avg_units_sold_per_day
FROM inventory_log il
JOIN store s ON il.store_key = s.store_key
JOIN product p ON il.product_key = p.product_key
JOIN weather_condition w ON il.weather_key = w.weather_key
GROUP BY s.store_id, p.product_id, p.category, w.weather_type
ORDER BY s.store_id, p.product_id, w.weather_type;

--price advantage disadvantage

SELECT 
  s.store_id,
  p.product_id,
  p.category,
  ROUND(AVG(l.price), 2) AS your_price,
  ROUND(AVG(l.competition), 2) AS competitor_price,
  ROUND(AVG(l.price) - AVG(l.competition), 2) AS price_gap,
  CASE
    WHEN AVG(l.price) < AVG(l.competition) THEN 'You are cheaper'
    WHEN AVG(l.price) > AVG(l.competition) THEN 'Competitor is cheaper'
    ELSE 'Same price'
  END AS price_position
FROM inventory_log l
JOIN store s ON l.store_key = s.store_key
JOIN product p ON l.product_key = p.product_key
GROUP BY s.store_id, p.product_id, p.category
ORDER BY s.store_id, p.product_id;






