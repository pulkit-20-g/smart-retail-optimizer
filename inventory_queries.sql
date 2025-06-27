CREATE TABLE raw_inventory (
    "Date" DATE,
    "Store ID" TEXT,
    "Product ID" TEXT,
    "Category" TEXT,
    "Region" TEXT,
    "Inventory Level" INT,
    "Units Sold" INT,
    "Units Ordered" INT,
    "Demand Forecast" NUMERIC,
    "Price" NUMERIC,
    "Discount" NUMERIC,
    "Weather Condition" TEXT,
    "Holiday/Promotion" BOOLEAN,
    "Competitor Pricing" NUMERIC,
    "Seasonality" TEXT
);

select * from raw_inventory;

--Creating the tables as per new rule 
CREATE TABLE store (
  store_key SERIAL PRIMARY KEY,
  store_id VARCHAR,
  region VARCHAR,
  UNIQUE(store_id, region)
);


INSERT INTO store (store_id, region)
SELECT DISTINCT "Store ID","Region"
FROM raw_inventory;

CREATE TABLE product (
  product_key SERIAL PRIMARY KEY,
  product_id VARCHAR,
  category VARCHAR,
  UNIQUE(product_id, category)
);

INSERT INTO product (product_id, category)
SELECT DISTINCT "Product ID","Category"
FROM raw_inventory;


CREATE TABLE weather_condition (
  weather_key SERIAL PRIMARY KEY,
  weather_type VARCHAR UNIQUE
)

INSERT INTO weather_condition (weather_type)
SELECT DISTINCT "Weather Condition"
FROM raw_inventory;

CREATE TABLE season (
  season_key SERIAL PRIMARY KEY,
  season_name VARCHAR UNIQUE
)

INSERT INTO season (season_name)
SELECT DISTINCT "Seasonality"
FROM raw_inventory;

select * from season;


CREATE TABLE inventory_log (
  log_date DATE,
  store_key INT REFERENCES store(store_key),
  product_key INT REFERENCES product(product_key),
  inventory INT,
  units_sold INT,
  units_ord INT,
  demand NUMERIC,
  price NUMERIC,
  discount NUMERIC,
  weather_key INT REFERENCES weather_condition(weather_key),
  holiday_flag BOOLEAN,
  competition NUMERIC,
  season_key INT REFERENCES season(season_key),
  PRIMARY KEY (log_date, store_key, product_key)
);

INSERT INTO inventory_log (
  log_date, store_key, product_key, inventory, units_sold, units_ord,
  demand, price, discount, weather_key, holiday_flag, competition, season_key
)
SELECT
  r."Date",
  s.store_key,
  p.product_key,
  r."Inventory Level",
  r."Units Sold",
  r."Units Ordered",
  r."Demand Forecast",
  r."Price",
  r."Discount",
  w.weather_key,
  r."Holiday/Promotion",
  r."Competitor Pricing",
  se.season_key
FROM raw_inventory r
JOIN store s ON r."Store ID" = s.store_id AND r."Region" = s.region
JOIN product p ON r."Product ID" = p.product_id AND r."Category" = p.category
JOIN weather_condition w ON r."Weather Condition" = w.weather_type
JOIN season se ON r."Seasonality" = se.season_name;

Select * from inventory_log;

