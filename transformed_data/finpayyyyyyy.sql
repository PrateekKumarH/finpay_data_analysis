create database finpay_analytics;

use finpay_analytics;

create table dim_country(
	country_id int primary key,
    country varchar(50)
);

create table dim_device(
	device_id int primary key,
    device_type varchar(50)
);

create table dim_payment_method(
	payment_method_id int primary key,
    payment_method varchar(50)
);

create table dim_user(
	user_id varchar(50) primary key,
    dummy varchar(20)
);

create table dim_merchant(
	merchant_id varchar(50)primary key,
    category varchar(50)
);

create table dim_date(
	date DATE primary key,
    month int,
    year int,
    month_name varchar(20),
    weekday varchar(20),
    hour int
);


create table fact_transactions(
	transaction_id varchar(50) primary key,
    user_id varchar(50),
    merchant_id varchar(50),
    country_id int,
    device_id int,
    payment_method_id int,
    date Date,
    amount decimal(10, 2),
    status varchar(20),
	is_promo_used tinyint,
    
    foreign key (user_id) references dim_user(user_id),
    foreign key (merchant_id) references dim_merchant(merchant_id),
    foreign key (country_id) references dim_country(country_id),
    foreign key (device_id) references dim_device(device_id),
    foreign key (payment_method_id) references dim_payment_method(payment_method_id),
    foreign key (date) references dim_date(date)
);


select count(*) from fact_transactions;
select * from fact_transactions;

SELECT *
FROM fact_transactions
WHERE country_id IS NULL
   OR device_id IS NULL
   OR payment_method_id IS NULL;

SELECT status, COUNT(*)
FROM fact_transactions
GROUP BY status;

#KPI QUERIES (CORE ANALYTICS)
#Total Revenue Calculation
select sum(amount) as total_revenue
from fact_transactions
where status="success";

#Total transactions count
select count(*) as total_transactions
from fact_transactions;

#Average ticket size
select avg(amount) as avg_ticket_amount
from fact_transactions
where status="success";

#Promo Conversion Rate
select sum(is_promo_used)*1.0/count(*) as promo_conversion_rate
from fact_transactions;



select sum(amount)*1.0/count(*) as payment_success_rate
from fact_transactions
where status="success";

#payment_status rate
select status, count(*) *100.0/sum(count(*)) over () as percentage
from fact_transactions
group by status;

#Payment success rate
SELECT
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) * 1.0
    / COUNT(*) AS payment_success_rate
FROM fact_transactions;

#Time-Based Analytics (VERY IMPORTANT)
#Monthly Revenue
select
	d.year,
    d.month,
    d.month_name,
    sum(f.amount) as revenue
from fact_transactions as f
join dim_date as d
	on f.date=d.date
where f.status="success"
group by d.year, d.month, d.month_name
order by d.year, d.month;

#Month-over-Month (MoM %) Revenue
with monthly as(
	select
		d.year,
        d.month,
        sum(f.amount) as revenue
	from fact_transactions as f
    join dim_date as d on f.date=d.date
    where f.status='success'
    group by d.year, d.month
)
select *, (revenue-lag(revenue) over (order by year, month))/
	lag(revenue) over (order by year, month) as mom_percentage
from monthly;

#Create SQL Views (BEST PRACTICE)
#Overview KPI View
create view vw_overview_kpis as
select
	count(*) as total_transactions,
    sum(case when status='success' then amount else 0 end) as total_revenue,
    avg(case when status='success' then amount end) as avg_ticket_size,
    sum(is_promo_used) * 1.0/count(*) as promo_conversion_rate
from fact_transactions;
    
select * from vw_overview_kpis;

CREATE VIEW vw_payment_insights AS
SELECT
    pm.payment_method,
    f.status,
    COUNT(*) AS transaction_count,
    SUM(f.amount) AS total_amount,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY pm.payment_method) 
        AS status_percentage
FROM fact_transactions f
JOIN dim_payment_method pm
    ON f.payment_method_id = pm.payment_method_id
GROUP BY
    pm.payment_method,
    f.status;
    
CREATE VIEW vw_merchant_performance AS
SELECT
    m.merchant_id,
    m.category,
    COUNT(f.transaction_id) AS transaction_count,
    SUM(CASE WHEN f.status = 'SUCCESS' THEN f.amount ELSE 0 END) AS revenue,
    SUM(f.is_promo_used) AS promo_transactions
FROM fact_transactions f
JOIN dim_merchant m
    ON f.merchant_id = m.merchant_id
GROUP BY
    m.merchant_id,
    m.category;

CREATE VIEW vw_user_analytics AS
SELECT
    u.user_id,
    COUNT(f.transaction_id) AS transaction_count,
    SUM(CASE WHEN f.status = 'SUCCESS' THEN f.amount ELSE 0 END) AS total_spent,
    SUM(f.is_promo_used) AS promo_usage_count
FROM fact_transactions f
JOIN dim_user u
    ON f.user_id = u.user_id
GROUP BY
    u.user_id;

CREATE VIEW vw_time_trends AS
SELECT
    d.year,
    d.month,
    d.month_name,
    d.weekday,
    d.hour,
    COUNT(f.transaction_id) AS transaction_count,
    SUM(CASE WHEN f.status = 'SUCCESS' THEN f.amount ELSE 0 END) AS revenue,
    SUM(f.is_promo_used) AS promo_transactions
FROM fact_transactions f
JOIN dim_date d
    ON f.date = d.date
GROUP BY
    d.year,
    d.month,
    d.month_name,
    d.weekday,
    d.hour;

SHOW FULL TABLES WHERE Table_type = 'VIEW';

#MoM View
CREATE VIEW vw_monthly_mom AS
WITH monthly AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        SUM(f.amount) AS revenue,
        COUNT(f.transaction_id) AS transactions
    FROM fact_transactions f
    JOIN dim_date d ON f.date = d.date
    WHERE f.status = 'SUCCESS'
    GROUP BY d.year, d.month, d.month_name
)
SELECT
    *,
    (revenue - LAG(revenue) OVER (ORDER BY year, month))
    / LAG(revenue) OVER (ORDER BY year, month) AS revenue_mom_pct,
    (transactions - LAG(transactions) OVER (ORDER BY year, month))
    / LAG(transactions) OVER (ORDER BY year, month) AS txn_mom_pct
FROM monthly;

select * from vw_monthly_mom;





























