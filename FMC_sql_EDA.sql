

--Author 	: Jojo john Moolayil
--Date 		: 14 June 2015
-- Project	: xxx


--Creating  a table at Item level with all required dimensions
 drop table item_raw

 select a.*,man_desc_guid,asset_name,c.City,c.State
 into item_raw
 from item360  a 
 inner join 
 newAsset_mapping b
 on a.asset_guid = b.cust_desc_guid
 inner join 
 tblCustomerLocations c
 on cast(a.customerlocation_guid as varchar(100)) = concat('{',cast(c.customerlocation_guid as varchar(100)), '}') 
where age <10000  --REmoving Outliers [8 in total]
 --getting the overall picture


 --Overall : Statistics across Assets
select a.asset_name, min_age,q1_age, median_Age, q3_age, max_age, total_items
from
(--Calculating min and max at an asset level
select asset_name, min(age) as min_age,max(age) as max_age,count(*) as total_items
from item_raw
where scrapflag=1
group by asset_name
) a
inner join
(
--Calculating 1 quartile, median and 2 quartile at an asset level
select distinct asset_name, 
PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY age) OVER (PARTITION BY asset_name) AS q1_age,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age) OVER (PARTITION BY asset_name) AS median_age,
PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY age) OVER (PARTITION BY asset_name) AS q3_Age
from item_raw
where scrapflag =1
)b
on a.asset_name = b.asset_name


-- Overall statistics : Asset across Customer_name

select a.asset_name,a.CustomerName, min_age,q1_age, median_Age, q3_age, max_age,total_items
 from
(--Calculating min and max at an asset level
select asset_name,CustomerName, min(age) as min_age,max(age) as max_age, count(*) as total_items
from item_raw
where scrapflag=1
group by asset_name, CustomerName
) a
inner join
(
--Calculating 1 quartile, median and 2 quartile at an asset level
select distinct asset_name, CustomerName,
PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY age) OVER (PARTITION BY asset_name,CustomerName) AS q1_age,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age) OVER (PARTITION BY asset_name,CustomerName) AS median_age,
PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY age) OVER (PARTITION BY asset_name,CustomerName) AS q3_Age
from item_raw
where scrapflag =1
)b
on a.asset_name = b.asset_name and a.CustomerName = b.CustomerName



 --Overall : Statistics across Assets X state
 select a.asset_name,a.state, min_age,q1_age, median_Age, q3_age, max_age,total_items
 from
(--Calculating min and max at an asset level
select asset_name,state, min(age) as min_age,max(age) as max_age, count(*) as total_items
from item_raw
where scrapflag=1  and country = 'USA'
group by asset_name, state
) a
inner join
(
--Calculating 1 quartile, median and 2 quartile at an asset level
select distinct asset_name, state,
PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY age) OVER (PARTITION BY asset_name,state) AS q1_age,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age) OVER (PARTITION BY asset_name,state) AS median_age,
PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY age) OVER (PARTITION BY asset_name,state) AS q3_Age
from item_raw
where scrapflag =1 and country = 'USA'
)b
on a.asset_name = b.asset_name and a.state = b.state
inner join 
(
select top 20 state, count(*) as a from item_raw 
where state is not null and state != 'NULL'  and country = 'USA'
group by state order by count(*) desc 
) c
on a.state =c.state



select a.asset_name,a.CustomerName,a.state, min_age,q1_age, median_Age, q3_age, max_age,total_items
 from
(--Calculating min and max at an asset level
select asset_name,CustomerName,state,min(age) as min_age,max(age) as max_age, count(*) as total_items
from item_raw
where scrapflag=1 and country = 'USA'
group by asset_name, CustomerName,state
) a
inner join
(
--Calculating 1 quartile, median and 2 quartile at an asset level
select distinct asset_name, CustomerName,state,
PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY age) OVER (PARTITION BY asset_name,CustomerName,state) AS q1_age,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age) OVER (PARTITION BY asset_name,CustomerName,state) AS median_age,
PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY age) OVER (PARTITION BY asset_name,CustomerName,state) AS q3_Age
from item_raw
where scrapflag =1 and country = 'USA'
)b
on a.asset_name = b.asset_name and a.CustomerName = b.CustomerName and a.state = b.state
inner join 
(
select top 20 state, count(*) as a from item_raw  where country = 'USA' group by state order by count(*) desc 
) c
on a.state =c.state


-------------------------------------------------------------------------------------------------------------------
