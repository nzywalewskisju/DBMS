use class_examples;

-- NUMBER 1
-- Selecting the restaurant and their average price
select r.name as Restaurant, 
	avg(f.Price) as AvgPrice
from serves s
inner join restaurants r
	on s.restID = r.restID
inner join foods f
	on s.foodID = f.foodID
group by r.name;



-- NUMBER 2
-- Selecting the restaurant and their highest price
select r.name as Restaurant, 		
	max(f.price) as MaxPrice
from serves s
inner join restaurants r
	on s.restID = r.restID
inner join foods f
	on s.foodID = f.foodID
group by r.name;


-- NUMBER 3
-- Selecting the count of different food types at each restaurant
select r.name as Restaurant, 
	count(distinct(f.name)) as NumberItems,
	f.type as FoodType
from serves s
inner join restaurants r
	on s.restID = r.restID
inner join foods f
	on s.foodID = f.foodID
group by r.name, f.type;


-- NUMBER 4
-- Selecting the average food price for each chef
select c.name as Chef,
	avg(f.price) as AvgPrice
from serves s
inner join restaurants r
	on s.restID = r.restID
inner join foods f
	on s.foodID = f.foodID
inner join works w
	on w.restID = r.restID
inner join chefs c
	on c.chefID = w.chefID
group by c.name;



-- NUMBER 5
-- Finding the restaurant with the highest average food price
select r.name as Restaurant,
       avg(f.price) as AvgPrice
from serves s
inner join restaurants r
	on s.restID = r.restID
inner join foods f
	on s.foodID = f.foodID
group by r.name
having avg(f.price) >= all 
	(
		-- Sub Query
        -- returns list of all avg prices grouped by restaurants
        -- used as a refernce for the Having clause in the main query
		select avg(f.price) as AvgPrice
		from serves s
		inner join restaurants r
			on s.restID = r.restID
		inner join foods f
			on s.foodID = f.foodID
		group by r.name
);


-- NUMBER 6-- EXTRA CREDIT
-- Determine which chef has the highest average price of the foods served at the restaurants where they work. 
-- Include the chef’s name, the average food price, and the names of the restaurants where the chef works. 
-- Sort the  results by the average food price in descending order.
-- 
-- This question confused me because it seems to state that there is only one chef with a higher avg food price than all others
-- But there are multiple chefs that are tied for this position
-- I proceeded by returning results for each of these chefs.
select c.name as ChefName,
       avg(f.price) as AvgPrice,
      GROUP_CONCAT(DISTINCT r.name) AS Restaurants -- used online resources to understand this
from serves s
inner join restaurants r on s.restID = r.restID
inner join foods f on s.foodID = f.foodID
inner join works w on w.restID = r.restID
inner join chefs c on c.chefID = w.chefID
group by c.name
having avg(f.price) >= all 
	(
		-- Sub Query
        -- returns list of all avg prices for each chef
        -- used as a refernce for the Having clause in the main query
		select avg(f.price) as AvgPrice
		from serves s
		inner join restaurants r on s.restID = r.restID
		inner join foods f on s.foodID = f.foodID
		inner join works w on w.restID = r.restID
		inner join chefs c on c.chefID = w.chefID
		group by c.name
)
order by avg(f.price) desc;