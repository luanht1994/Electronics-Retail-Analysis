 IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Global_Electronics_Retailer')
BEGIN 
	CREATE DATABASE Global_Electronics_Retailer;
END
USE Global_Electronics_Retailer
CREATE TABLE Products (
	product_key INT PRIMARY KEY,
	product_name VARCHAR(100),
	brand VARCHAR(100),
	color VARCHAR(100),
	unit_cost DECIMAL(10,2),
	unit_price DECIMAL(10,2),
	subcategory_key INT,
	subcategory VARCHAR(100),
	category_key INT,
	category VARCHAR(100)
	)
CREATE TABLE Store (
	store_key INT PRIMARY KEY,
	country VARCHAR(100),
	state VARCHAR(100),
	square_meter INT,
	open_date DATE
	)
CREATE TABLE Customer (
	customer_key INT PRIMARY KEY,
	gender VARCHAR(10),
	name VARCHAR(10),
	city VARCHAR(100),
	state_code VARCHAR(100),
	state VARCHAR(100),
	zip_code INT,
	country VARCHAR(100),
	continent VARCHAR(100),
	birth_date DATE
	)
CREATE TABLE Exchange_rates (
	date DATE,
	currency CHAR(3),
	exchange Float
	)
CREATE TABLE Sales (
	order_no INT,
	line_item INT,
	oder_date DATE,
	delivery_date DATE,
	customer_key INT,
	store_key INT,
	product_key INT,
	quantity INT,
	currency_code CHAR(3),
		CONSTRAINT customer_key_fk FOREIGN KEY (customer_key) REFERENCES dbo.Customer(customer_key),
		CONSTRAINT store_key_fk FOREIGN KEY (store_key) REFERENCES dbo.Store(store_key),
		CONSTRAINT product_key_fk FOREIGN KEY (product_key) REFERENCES dbo.Products(product_key)
	)
EXEC sp_rename 'Sales.oder_date', 'order_date','COLUMN';
-- Upload data from CSV into existing tables
BULK INSERT Products
FROM 'C:\Users\Admin\Downloads\Products.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 1, -- Skip header row
    FIELDTERMINATOR = ',',  -- CSV uses commas to separate columns
    ROWTERMINATOR = '\n',   -- New line for each row
    TABLOCK
);
BULK INSERT Store
FROM 'C:\Users\Admin\Downloads\Stores.csv'
WITH(
	FORMAT='CSV',
	FIRSTROW = 1, -- Skip header row,
	FIELDTERMINATOR = ',', -- CSV uses commas to separate columns
	ROWTERMINATOR = '\n', -- New line for each row
	TABLOCK
	);
ALTER TABLE Customer
	ALTER COLUMN name VARCHAR(100);
ALTER TABLE Customer
	ALTER COLUMN zip_code VARCHAR(100);
BULK INSERT Customer
FROM 'C:\Users\Admin\Downloads\Customers.csv'
WITH(
	FORMAT='CSV',
	FIRSTROW = 1, -- Skip header row,
	FIELDTERMINATOR = ',', -- CSV uses commas to separate columns
	ROWTERMINATOR = '\n', -- New line for each row
	TABLOCK
	);
BULK INSERT Exchange_rates
FROM 'C:\Users\Admin\Downloads\Exchange_Rates.csv'
WITH(
	FORMAT='CSV',
	FIRSTROW = 1, -- Skip header row,
	FIELDTERMINATOR = ',', -- CSV uses commas to separate columns
	ROWTERMINATOR = '\n', -- New line for each row
	TABLOCK
	);
BULK INSERT Sales
FROM 'C:\Users\Admin\Downloads\Sales.csv'
WITH(
	FORMAT='CSV',
	FIRSTROW = 1, -- Skip header row,
	FIELDTERMINATOR = ',', -- CSV uses commas to separate columns
	ROWTERMINATOR = '\n', -- New line for each row
	TABLOCK
	);
-- Sale analysis
use Global_Electronics_Retailer;
-- Join sales with products table and creating a temporary table
SELECT
	s.order_no,
	s.line_item,
	s.order_date,
	s.delivery_date,
	s.quantity,
	p.brand,
	p.category,
	p.subcategory,
	p.color,
	p.unit_cost,
	p.unit_price
INTO #sale_table
FROM Sales AS s
LEFT JOIN Products AS p
ON s.product_key =p.product_key;
-- Total Sales, total cost and total quantity over time
SELECT
	SUM(st.quantity*st.unit_price) AS Total_sale,
	SUM(st.quantity*st.unit_cost) AS Total_cost,
	ROUND((SUM(st.quantity*st.unit_price) - SUM(st.quantity*st.unit_cost))/SUM(st.quantity*st.unit_price) *100,2) AS Profit_margin,
	SUM(st.quantity) AS Total_quantity
FROM #sale_table AS st;
-- Total sales and profit margin over year
DECLARE @month_name nvarchar(Max);
DECLARE @query nvarchar(Max);
/*Generating automatically column name in pivot table*/
--select top 10 * from #sale_table;
SET @month_name = (SELECT
						STRING_AGG(QUOTENAME(month_name,'[]'),',') WITHIN GROUP (ORDER BY month_no)
					FROM (
						SELECT
							DISTINCT 
							FORMAT(st.order_date,'MMM') AS month_name,
							MONTH(st.order_date) AS month_no
						FROM #sale_table AS st) AS shortmonthname);
/*total sales per month per year*/
SET @query =
'SELECT
	year_order,'
	+@month_name+'
FROM (
	SELECT 
		YEAR(st.order_date) AS year_order,
		FORMAT(st.order_date,''MMM'') AS month_order,
		SUM(st.quantity * st.unit_price) AS total_sale
	FROM #sale_table AS st
	GROUP BY YEAR(st.order_date), FORMAT(st.order_date,''MMM'')
) AS grouptable
PIVOT (
SUM(total_sale)
FOR month_order IN ('+@month_name+')
) AS pivottable;';
EXEC sp_executesql @query;
/*total profit month per year*/
DECLARE @profit nvarchar(MAX)
SET @profit =
'SELECT
	year_order,'
	+@month_name+'
FROM (
	SELECT 
		YEAR(st.order_date) AS year_order,
		FORMAT(st.order_date,''MMM'') AS month_order,
		(SUM(st.quantity * st.unit_price) - SUM(st.quantity * st.unit_cost))/SUM(st.quantity * st.unit_price) *100  AS profit_margin
	FROM #sale_table AS st
	GROUP BY YEAR(st.order_date), FORMAT(st.order_date,''MMM'')
) AS grouptable
PIVOT (
SUM(profit_margin)
FOR month_order IN ('+@month_name+')
) AS pivottable;';
EXEC sp_executesql @profit;
--Comparison sale month over month
SELECT
	FORMAT (st.order_date,'MMM yyyy') AS month_order,
	SUM(st.quantity * st.unit_cost) AS total_sale,
	(SUM(st.quantity * st.unit_cost) - LAG(SUM(st.quantity * st.unit_cost),1) OVER(ORDER BY MONTH(st.order_date)))/ LAG(SUM(st.quantity * st.unit_cost),1) OVER(ORDER BY MONTH(st.order_date))*100 AS growth
FROM #sale_table AS st
GROUP BY YEAR(st.order_date), FORMAT (st.order_date,'MMM yyyy'),MONTH(st.order_date)
HAVING YEAR(st.order_date) = 2018;
--Profit margin
DECLARE @monthname nvarchar(max);
DECLARE @querymargin nvarchar(max);
SET @monthname =
(SELECT
	STRING_AGG(QUOTENAME(month_name,'[]'),',') WITHIN GROUP (ORDER BY month_no)
FROM (
		SELECT DISTINCT
			FORMAT(subtable.order_date,'MMM') AS month_name,
			MONTH(subtable.order_date) AS month_no
		FROM #sale_table AS subtable
		)AS monthnametable);
SET @querymargin =
'SELECT
	year_order,'+
	@monthname+'
FROM (
		SELECT
			YEAR(st.order_date) AS year_order,
			FORMAT(st.order_date,''MMM'') AS month_order,
			(SUM(st.quantity*st.unit_price) - SUM(st.quantity*st.unit_cost)) / SUM(st.quantity*st.unit_price) * 100 AS profit_margin
		FROM #sale_table AS st
		GROUP BY YEAR(st.order_date), FORMAT(st.order_date,''MMM'')
		) AS group_table
PIVOT (
	SUM(profit_margin)
	FOR month_order IN ('+@monthname+')
	) AS pivottb;';
EXEC sp_executesql @querymargin;
select top 10 * from Sales;
select top 10 * from Customer;
--Joining Sales table wit customer table
SELECT
	s.order_no,
	s.line_item,
	s.order_date,
	s.delivery_date,
	s.quantity,
	p.brand,
	p.category,
	p.subcategory,
	p.color,
	p.unit_cost,
	p.unit_price,
	c.customer_key,
	c.gender,
	c.name,
	c.continent,
	c.country,
	c.city,
	c.birth_date,
	(SELECT MAX(YEAR(order_date)) FROM Sales) - YEAR(c.birth_date) as age,
	CASE
		WHEN (SELECT MAX(YEAR(order_date)) FROM Sales) - YEAR(c.birth_date) < 25 THEN 'Young_Adults'
		WHEN (SELECT MAX(YEAR(order_date)) FROM Sales) - YEAR(c.birth_date) < 35 THEN 'Early_Career'
		WHEN (SELECT MAX(YEAR(order_date)) FROM Sales) - YEAR(c.birth_date) < 45 THEN 'Mid_Career'
		WHEN (SELECT MAX(YEAR(order_date)) FROM Sales) - YEAR(c.birth_date) < 65 THEN 'Established_Professionals'
		ELSE 'Retirement'
	END AS group_people
INTO #sale_table1
FROM Sales AS s
LEFT JOIN Products AS p
ON s.product_key =p.product_key
LEFT JOIN Customer AS c
ON s.customer_key =c.customer_key;
select top 10 * from #sale_table1;
--DROP TABLE #sale_table1;
--Sale per continential
SELECT
	continent,
	total_sale,
	total_sale/ SUM(total_sale) OVER() * 100 AS sale_proportion
FROM (
	SELECT
		st.continent,
		SUM(st.quantity*st.unit_price) AS total_sale
	FROM #sale_table1 AS st
	GROUP BY st.continent) AS continential;
select top 10 * from #sale_table1;
--Top 5 products by sales
SELECT TOP 5
	product_name,
	total_sale,
	RANK() OVER(ORDER BY total_sale DESC) AS rank_sales
FROM (
	SELECT
		st.subcategory AS product_name,
		SUM(st.quantity*st.unit_price) AS total_sale
	FROM #sale_table1 AS st
	GROUP BY st.subcategory) AS grouptable;
--Top 5 products by sold quantity
SELECT TOP 5
	product_name,
	total_quantity,
	RANK() OVER(ORDER BY total_quantity DESC) AS rank_sold_quantity
FROM (
	SELECT
		st.subcategory AS product_name,
		SUM(st.quantity) AS total_quantity
	FROM #sale_table1 AS st
	GROUP BY st.subcategory) AS grouptable;
--Customer analysis
--Total customer
SELECT
	DISTINCT COUNT(st.customer_key)
FROM #sale_table1 AS st;
DECLARE @column NVARCHAR(MAX);
DECLARE @retentionrate NVARCHAR(MAX);
SET @column =
    (SELECT
        STRING_AGG(QUOTENAME(month_name,'[]'),',') WITHIN GROUP (ORDER BY month_no)
    FROM (
        SELECT DISTINCT
            FORMAT(st3.order_date,'MMM') AS month_name, 
            MONTH(st3.order_date) AS month_no
        FROM #sale_table1 AS st3) AS month_name);
SET @retentionrate ='
WITH first_order AS(
SELECT
    st.customer_key,
    FORMAT(MIN(st.order_date),''MMM yyyy'') AS first_order
FROM #sale_table1 AS st
GROUP BY st.customer_key),
month_order AS(
SELECT
    st1.customer_key,
    FORMAT(st1.order_date,''MMM yyyy'') AS order_month
FROM #sale_table1 AS st1),
new_customer AS(
SELECT
    order_month,
    COUNT(DISTINCT customer_key) AS new_customer
FROM (
SELECT
    om.*,
    fo.first_order
FROM month_order AS om
LEFT JOIN first_order AS fo
ON om.customer_key = fo.customer_key) AS customer_order
WHERE first_order = order_month
GROUP BY customer_order.order_month),
total_customer AS(
SELECT 
    FORMAT(st2.order_date,''MMM yyyy'') AS order_month,
    COUNT(DISTINCT st2.customer_key) AS total_customer
FROM #sale_table1 AS st2
GROUP BY FORMAT(st2.order_date,''MMM yyyy'')),
retentionrate AS(
SELECT 
    ac.*,
    nc.new_customer,
    ROUND(CAST((ac.total_customer - nc.new_customer) AS float)/CAST(ac.total_customer AS float) *100,2) AS retention_rate
FROM total_customer AS ac
LEFT JOIN new_customer AS nc
ON ac.order_month = nc.order_month)
SELECT 
    year_order,'+
    @column +'
FROM (
        SELECT
            CAST(RIGHT(order_month,4) AS int) AS year_order,
            LEFT(order_month,3) AS order_month,
            retention_rate
        FROM retentionrate) AS rt
PIVOT (
    SUM(retention_rate)
    FOR order_month IN ('+ @column +')
    ) AS pivottable;';
EXEC sp_executesql @retentionrate;
select top 10 * from #sale_table1;
-- Sale proportion per group people
SELECT
	group_people,
	SUM(quantity) AS total_quantity,
	SUM(unit_price*quantity) / (SELECT SUM(unit_price*quantity) FROM #sale_table1) *100 AS sale_proportion
FROM #sale_table1 AS st4
GROUP BY st4.group_people
ORDER BY sale_proportion DESC;
