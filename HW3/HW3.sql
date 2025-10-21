

use hw3;

-- CHECKS ON VALUES AND SETTING OF PRIMARY/FOREIGN KEYS
alter table merchants
add constraint mer_pk primary key(mid);

alter table products
add constraint prod_pk primary key(pid),
add constraint prod_name_const check 
	(name in ("Printer", "Ethernet Adapter", "Desktop", "Hard Drive", 
    "Laptop", "Router", "Network Card", "Super Drive", "Monitor")),
add constraint prod_cat_const check (category in 
	("Peripheral", "Networking", "Computer"));
    
alter table sell
add constraint sell_fk_mid foreign key(mid)
	references merchants(mid) 
    on delete cascade 
    on update cascade,
add constraint sell_fk_pid foreign key(pid)
	references products(pid) 
    on delete cascade 
    on update cascade,
add constraint sell_price_const check 
	(price between 0 and 100000),
add constraint sell_qa_const check 
	(quantity_available between 0 and 1000);

alter table orders
add constraint orders_pk primary key(oid),
add constraint orders_sm_const check 
	(shipping_method in ("UPS","FedEx","USPS")),
add constraint orders_sc_const check 
	(shipping_cost between 0 and 500);
    
alter table contain
add constraint con_fk_oid foreign key(oid)
	references orders(oid) 
    on delete cascade 
    on update cascade,
add constraint con_fk_pid foreign key(pid)
	references products(pid) 
    on delete cascade 
    on update cascade;
    
alter table customers
add constraint cust_pk primary key(cid);

alter table place
add constraint place_fk_cid foreign key(cid)
	references customers(cid) 
    on delete cascade 
    on update cascade,
add constraint place_fk_oid foreign key(oid)
	references orders(oid) 
    on delete cascade 
    on update cascade;
-- ---------------------------------------------

-- QUERIES
-- ------------
-- Query 1: List names and sellers of products that are no longer available (quantity=0)
-- Using merchants, products, and sell tables
select m.name as Seller, 
	p.name as ProductName, 
    s.quantity_available as Quantity
from merchants m
inner join sell s on m.mid = s.mid
inner join products p on s.pid = p.pid
where s.quantity_available = 0;

-- Query 2: List names and descriptions of products that are not sold.
-- Left join with products table on left, so all products are included
select p.name as ProductName, p.description as Description
from products p
left outer join sell s
	on s.pid = p.pid
where s.pid is NULL; -- filtering for only the products that are not sold

-- Query 3: How many customers bought SATA drives but not any routers?
select count(cid) from
	( -- part one gets all customers that ordered a Hard Drive
	select distinct cust.cid
	from customers cust
	inner join place on place.cid = cust.cid
	inner join contain con on con.oid = place.oid
	inner join products prod on prod.pid = con.pid
	where prod.name = 'Hard Drive'

	except	-- we are taking away the customers that also bought a Router

	select distinct cust.cid
	from customers cust
	inner join place on place.cid = cust.cid
	inner join contain con on con.oid = place.oid
	inner join products prod on prod.pid = con.pid
	where prod.name = 'Router'
	) t;


-- Query 4: HP has a 20% sale on all its Networking products.
-- Interpreted this as asking to show the old and prices of each product that changed
select m.name as Seller,
	p.name as ProductName,
    p.category as Category,
    s.price as OldPrice,
    (s.price * 0.80) as NewPrice
from merchants m
inner join sell s
	on m.mid = s.mid
inner join products p
	on p.pid = s.pid
where m.name = 'HP' and p.category = 'Networking';


-- Query 5: What did Uriel Whitney order from Acer? (make sure to at least retrieve product names and prices).
-- I did this query before Dr. Forouraghi changed the question, and he said I can leave my previous answer....
	-- This query is a bit difficulty because our schema does not allow us to see which merchant supplied the product that Uriel Whitney purchased.
	-- Each product can be sold by multiple merchants, so we have no way of knowing which of the products that Uriel Whitney bought were from Acer.
-- This query answers the question: What items that Uriel Whitney ordered does Acer sell?
	-- This is as close as we can get with the current schema.
select distinct p.pid as ProductID, p.name as ProductName, s.price as Price, m.name as Seller
from customers cust
inner join place on place.cid = cust.cid
inner join contain c on c.oid = place.oid
inner join products p on p.pid = c.pid
inner join sell s on s.pid = p.pid
inner join merchants m on m.mid = s.mid
where cust.fullname = 'Uriel Whitney' and m.name = 'Acer'
order by p.pid;


-- Query 6: List the annual total sales for each company (sort the results along the company and the year attributes).
-- The current schema does not allow us to answer this query specifically.
	-- This is because the current schema does not allow us to see which merchant supplied each product that was bought.
	-- The orders table tells us which products were purchased in each order, but not which merchant supplied the product.
-- The following query inflates totals when multiple merchants sell the same product, because `contain` does not store `mid`. 
	-- Results should be interpreted as potential revenue, not actual sales.
select m.name as Company,
	year(pl.order_date) as OrderYear,
	sum(s.price*s.quantity_available) as PotentialRev
from sell s
inner join contain c
	on c.pid = s.pid
inner join place pl
	on pl.oid = c.oid
inner join merchants m
	on m.mid = s.mid
group by Company, OrderYear
order by OrderYear, Company;

-- Query 7: Which company had the highest annual revenue and in what year?
-- The current schema does not allow us to answer this query specifically.
	-- This is because the current schema does not allow us to see which merchant supplied each product that was bought.
-- The following query inflates totals when multiple merchants sell the same product, because `contain` does not store `mid`. 
	-- Results should be interpreted as potential revenue, not actual sales.
select Company, OrderYear, PotentialRev as HighestRev
from (
	select m.name as Company, year(pl.order_date) as OrderYear, sum(s.price*s.quantity_available) as PotentialRev
	from sell s
	inner join contain c on c.pid = s.pid
	inner join place pl on pl.oid = c.oid
	inner join merchants m on m.mid = s.mid
	group by Company, OrderYear
	order by OrderYear, Company   
) t
order by PotentialRev
limit 1;


-- Query 8: On average, what was the cheapest shipping method used ever?
-- This query can be answered directly, by calculating average shipping cost, grouped by the method
-- The average shipping costs were sorted from lowest to highest, and only the lowest is shown with a limit of 1 being used.
select shipping_method as ShippingMethod, avg(shipping_cost) as AvgCost
from orders
group by shipping_method
order by AvgCost
limit 1;


-- Query 9: What is the best sold ($) category for each company?
-- The current schema does not allow us to answer this query specifically.
	-- This is because the schema does not record which merchant actually fulfilled each purchased product.
	-- In addition, the schema does not record quantities sold, only quantities available for sale.
-- The following query therefore uses inventory data (price × quantity_available from `sell`) to approximate revenue.
	-- Results should be interpreted as potential revenue, not actual sales.
select t1.Company, t1.Category, t1.PotentialRev from
(
	-- this subquery gets the potential revenue for each category for each company
	select m.name as Company, p.category as Category, sum(s.price*s.quantity_available) as PotentialRev
	from sell s
	inner join merchants m on m.mid = s.mid
	inner join products p on p.pid = s.pid
	group by m.name, p.category
) as t1
where t1.PotentialRev = (select max(t2.PotentialRev) -- we select the highest potential revenue for each company
	from (
		-- this subquery also gets the potential revenue for each category for each company
		select m2.name as Company, p2.category as Category, sum(s2.price*s2.quantity_available) as PotentialRev
		from sell s2
		inner join merchants m2 on m2.mid = s2.mid
		inner join products p2 on p2.pid = s2.pid
		group by m2.name, p2.category
	) as t2
    where t1.Company = t2.Company -- we select the highest potential revenue for each company
) order by t1.Company; -- we put the companies in alphabetical order, for display
			

-- Query 10: For each company, find out which customers have spent the most and the least amounts.
-- The current schema does not allow us to answer this query specifically.
	-- This is because `contain` does not store mid, so we cannot tell which merchant actually fulfilled the product.
-- The query assumes qty = 1 and uses prices from `sell`.
-- Totals are inflated when multiple merchants sell the same product, since each merchant is credited.
-- Results should be read as potential spend, not actual sales.
with customers_per_company as (
	-- this query builds a table of how much each customer spends with each company
	select m.name Company, c.fullname CustomerName, sum(s.price) as Spent
	from customers c
	inner join place p on p.cid = c.cid
	inner join contain co on co.oid = p.oid
	inner join sell s on s.pid = co.pid
	inner join merchants m on m.mid = s.mid
	group by m.name, c.fullname)
-- for each company, select the customers that spent the most
select t1.Company, t1.CustomerName, t1.Spent
from customers_per_company t1
where t1.Spent = (
	select max(t2.Spent) from customers_per_company t2
    where t2.Company = t1.Company)
union all -- union to combine results
-- for each company, select the customers that spent the least
select t1.Company, t1.CustomerName, t1.Spent
from customers_per_company t1
where t1.Spent = (
	select min(t2.Spent) from customers_per_company t2
    where t2.Company = t1.Company)
order by company;