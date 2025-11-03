use hw4;

-- ADD ALL PRIMARY KEYS
alter table actor add constraint actor_pk primary key (actor_id);
alter table address add constraint address_pk primary key (address_id);
alter table category add constraint category_pk primary key (category_id);
alter table city add constraint city_pk primary key (city_id);
alter table country add constraint country_pk primary key (country_id);
alter table customer add constraint customer_pk primary key (customer_id);
alter table film add constraint film_pk primary key (film_id);
alter table film_actor add constraint film_actor_pk primary key (actor_id, film_id); -- composite pk
alter table film_category add constraint film_category_pk primary key (film_id, category_id); -- composite pk
alter table inventory add constraint inventory_pk primary key (inventory_id); 
alter table language add constraint language_pk primary key (language_id);    
alter table payment add constraint payment_pk primary key (payment_id) ;
alter table rental add constraint rental_pk primary key (rental_id);
alter table staff add constraint staff_pk primary key (staff_id);
alter table store add constraint store_pk primary key (store_id);

-- FOREIGN KEYS AND CONSTRAINTS
-- -----------------------------
-- ADDRESS
alter table address
    add constraint address_fk_city_id foreign key (city_id) references city(city_id)
		on update cascade on delete cascade;   -- fk

-- CATEGORY
alter table category
    add constraint category_name_const check (name in 
		('Animation','Comedy','Family','Foreign','Sci-Fi','Travel',
        'Children','Drama','Horror','Action','Classics','Games',
        'New','Documentary','Sports','Music'));   -- valid categories

-- CITY
alter table city
    add constraint city_fk_country_id foreign key (country_id) references country(country_id)
		on update cascade on delete cascade;   -- fk

-- CUSTOMER
alter table customer
    add constraint customer_fk_address_id foreign key (address_id) references address(address_id)
		on update cascade on delete cascade,   -- fk
	add constraint customer_fk_store_id foreign key (store_id) references store(store_id)
		on update cascade on delete cascade,   -- fk
	add constraint customer_active_const check (active in (0,1));   -- 0: inactive and 1: active

-- FILM
alter table film
    add constraint film_fk_language_id foreign key (language_id) references language(language_id)
		on update cascade on delete cascade,   -- fk
	add constraint film_rental_duration_const check (rental_duration between 2 and 8),   -- valid duration
    add constraint film_rental_rate_const check (rental_rate between 0.99 and 6.99),   -- valid rate
    add constraint film_length_const check (length between 30 and 200),   -- valid length
    add constraint film_rating_const check (rating in ('PG','G','NC-17','PG-13','R')),   -- valid rating
    add constraint film_replacement_cost_const check (replacement_cost between 5.00 and 100.00),   -- valid cost
    add constraint film_special_features_const check (special_features in ('Behind the Scenes', 'Commentaries', 'Deleted Scenes', 'Trailers')), -- valid attributes
    add constraint film_feature2_const check (feature2 in ('Behind the Scenes', 'Commentaries', 'Deleted Scenes', 'Trailers')
		or feature2 = ''), -- valid attributes
	add constraint film_feature3_const check (feature3 in ('Behind the Scenes', 'Commentaries', 'Deleted Scenes', 'Trailers')
		or feature3 = ''), -- valid attributes
	add constraint film_feature4_const check (feature4 in ('Behind the Scenes', 'Commentaries', 'Deleted Scenes', 'Trailers')
		or feature4 = ''); -- valid attributes

-- FILM_ACTOR
alter table film_actor
	add constraint film_actor_fk_actor_id foreign key (actor_id) references actor(actor_id)
		on update cascade on delete cascade,   -- fk
	add constraint film_actor_fk_film_id foreign key (film_id) references film(film_id)
		on update cascade on delete cascade;   -- fk

-- FILM_CATEGORY
alter table film_category
    add constraint film_category_fk_film_id foreign key (film_id) references film(film_id)
		on update cascade on delete cascade,   -- fk
	add constraint film_category_fk_category_id foreign key (category_id) references category(category_id)
		on update cascade on delete cascade;   -- fk

-- INVENTORY
alter table inventory
    add constraint inventory_fk_film_id foreign key (film_id) references film(film_id)
		on update cascade on delete cascade,   -- fk
	add constraint inventory_fk_store_id foreign key (store_id) references store(store_id)
		on update cascade on delete cascade;   -- fk

-- PAYMENT
alter table payment
	modify column payment_date datetime, -- valid date
	add constraint payment_fk_customer_id foreign key (customer_id) references customer(customer_id)
		on update cascade on delete cascade,   -- fk
	add constraint payment_fk_staff_id foreign key (staff_id) references staff(staff_id)
		on update cascade on delete cascade,   -- fk
	add constraint payment_fk_rental_id foreign key (rental_id) references rental(rental_id)
		on update cascade on delete cascade,   -- fk
	add constraint payment_amount_const check (amount >= 0);   -- non-negative amount

-- RENTAL
alter table rental
	modify column rental_date datetime, -- valid date
    modify column return_date datetime, -- valid date
    add constraint rental_fk_inventory_id foreign key (inventory_id) references inventory(inventory_id)
		on update cascade on delete cascade,   -- fk
	add constraint rental_fk_customer_id foreign key (customer_id) references customer(customer_id)
		on update cascade on delete cascade,   -- fk
	add constraint rental_fk_staff_id foreign key (staff_id) references staff(staff_id)
		on update cascade on delete cascade,   -- fk
	add constraint rental_unique unique (rental_date,inventory_id,customer_id); -- unique contraint according to legend

-- STAFF
alter table staff
    add constraint staff_fk_address_id foreign key (address_id) references address(address_id)
		on update cascade on delete cascade,   -- fk
	add constraint staff_fk_store_id foreign key (store_id) references store(store_id)
		on update cascade on delete cascade,   -- fk
    add constraint staff_active_const check (active in (0,1));   -- active flag

-- STORE
alter table store
    add constraint store_fk_address_id foreign key (address_id) references address(address_id)
		on update cascade on delete cascade;   -- fk
        
-- ----------------------
-- QUERIES
-- ----------------------
-- Query 1: What is the average length of films in each category? List the results in alphabetic order of categories.
select c.name, avg(f.length) as AvgLength
from film f
inner join film_category fc
	on f.film_id = fc.film_id
inner join category c
	on fc.category_id = c.category_id
group by c.name
order by c.name;


-- Query 2: Which categories have the longest and shortest average film lengths?
with category_averages as (
	select c.name as Category, avg(f.length) as AvgLength
	from film f
	inner join film_category fc
		on f.film_id = fc.film_id
	inner join category c
		on fc.category_id = c.category_id
	group by c.name
) -- this CTE gets the average length of each category of movie
-- using the CTE, we select the longest and shortest average lengths
select Category, AvgLength
from category_averages
where AvgLength = (select max(AvgLength) from category_averages)
or AvgLength = (select min(AvgLength) from category_averages);


-- Query 3: Which customers have rented action but not comedy or classic movies?
select cust.first_name, cust.last_name
	from customer cust
	inner join rental r on r.customer_id = cust.customer_id
	inner join inventory i on i.inventory_id = r.inventory_id
	inner join film_category fc on fc.film_id = i.film_id
	inner join category cat on cat.category_id = fc.category_id
	where cat.name = 'Action' -- customers that rented Action movies
except -- we remove the customers that rented Comedy or Classics movies
select cust.first_name, cust.last_name
	from customer cust
	inner join rental r on r.customer_id = cust.customer_id
	inner join inventory i on i.inventory_id = r.inventory_id
	inner join film_category fc on fc.film_id = i.film_id
	inner join category cat on cat.category_id = fc.category_id
	where cat.name = 'Comedy' or cat.name = 'Classics'; -- customers that rented Comedy or Classics

-- Query 4: Which actor has appeared in the most English-language movies?
with act_lang_counts as (
	select a.actor_id, a.first_name, a.last_name,
    l.name as Language, count(distinct f.film_id) as MovieCount
	from actor a
	inner join film_actor fa on fa.actor_id = a.actor_id
	inner join film f on f.film_id = fa.film_id
	inner join language l on l.language_id = f.language_id
    where l.name = 'English'
    group by a.actor_id, a.first_name, a.last_name
) -- this CTE gets each actor's count of English-language films
-- uisng the CTE, we select the actor with the max movie count
select first_name, last_name, MovieCount
from act_lang_counts
where MovieCount = (select max(MovieCount) from act_lang_counts);

-- Query 5: How many distinct movies were rented for exactly 10 days from the store where Mike works?
with film_days as (
	select f.film_id, datediff(r.return_date, r.rental_date) as DaysRented
	from film f
	inner join inventory i on i.film_id = f.film_id
	inner join rental r on r.inventory_id = i.inventory_id
    inner join store str on str.store_id = i.store_id
    inner join staff stf on stf.store_id = str.store_id
    where stf.first_name = 'Mike'
) -- this CTE gets the number of days each film was rented from Mike's store
-- uisng this CTE, we count how many distinct films were rented for exactly 10 days
select count(distinct film_id) as NumberOfMovies
from film_days
where DaysRented = 10;

-- Query 6 Alphabetically list actors who appeared in the movie with the largest cast of actors.
with cast_sizes as (
	select f.film_id as FilmID, f.title as MovieTitle,
    count(distinct a.actor_id) as CastSize
	from film f
    inner join film_actor fa on fa.film_id = f.film_id
    inner join actor a on a.actor_id = fa.actor_id
	group by f.film_id
) -- this CTE gets all movies with their casts sizes
-- using this CTE, we get the actors from the movie with the largest cast (using the cte)
select a.first_name as FirstName, a.last_name as LastName, MovieTitle
from cast_sizes cs
inner join film_actor fa on fa.film_id = cs.FilmID
inner join actor a on a.actor_id = fa.actor_id
where CastSize = (select max(CastSize) from cast_sizes)
order by a.last_name;
        