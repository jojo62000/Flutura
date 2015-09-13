--Author 		: Jojo John Moolayil
--Date			: 02 July 2015
-----------------------------------------------------------------
--Description 
--Creating benchmakrs for Weather Parameters at a Postal code (Month 15 day interval level) 	

--Context
--Considers a running historical dataset for 5 years to create a benchmark for q 15 day window at zipcode year,month part level
--End table will have for each postal code
	-- 15 [years] * 12 [Months] * 2 [Parts]
-----------------------------------------------------------------

--Creating the Table skeleton
IF OBJECT_ID('temp_check2', 'U') IS NOT NULL
	drop table temp_check2

create table temp_check2
(
postal_code varchar(50),yr int,mnth int,part varchar(2),
max_temp int,min_temp int,avg_temp int,stdev_temp float,
max_dewpoint int,min_dewpoint int,avg_dewpoint int,
stdev_dewpoint float,max_humidity int,min_humidity int,
mean_humidity int,stdev_humidity float,max_Pressure int,
min_Pressure int,mean_Pressure int,stdev_Pressure float,
max_Visibility int,min_Visibility int,mean_Visibility int,
stdev_Visibility float,max_windspeed int,mean_windspeed int,
stdev_windspeed float,max_gust_speed int,mean_rainfall float,
stdev_rainfall float,mean_windDirDeg int,max_cloudcover int,
min_cloudcover int,avg_cloudcover int,stdev_cloudcover float,
avg_rain float,avg_thunder float,avg_fog float,
avg_hail float,avg_snow float,avg_tornado float
)

----------------------
DECLARE @cnt INT = 2000;
WHILE @cnt < 2015
BEGIN
	insert into temp_check2
	--DECLARE @cnt INT = 2000;
	select 
	postal_code,
	@cnt as yr, month(est) as mnth,case when day(est) between 0 and 15 then 'P1'  else 'P2' end as part,
	
	--Temperature
	max(cast(max_temperatureC as int)) as max_temp,
	min(cast(min_temperatureC as int)) as min_temp,
	avg(cast(mean_temperatureC as int)) as avg_temp,
	stdev(cast(mean_temperatureC as int)) as stdev_temp,
	
	--Dewpoint
	max(cast(Dew_pointc as int)) as max_dewpoint,
	min(cast(Min_Dewpointc as int)) as min_dewpoint,
	avg(cast(MeanDew_pointc as int)) as avg_dewpoint,
	stdev(cast(MeanDew_pointc as int)) as stdev_dewpoint,

	--Humidity
	max(cast(MAx_humidity as int)) as max_humidity,
	min(cast(min_humidity as int)) as min_humidity,
	avg(cast(mean_humidity as int)) as mean_humidity,
	stdev(cast(mean_humidity as int)) as stdev_humidity,
	
	--Sea Level Pressure
	max(cast(Max_Sea_Level_PressurehPa as int)) as max_Pressure,
	min(cast(Min_Sea_Level_PressurehPa as int)) as min_Pressure,
	avg(cast(Mean_Sea_Level_PressurehPa as int)) as mean_Pressure,
	stdev(cast(Mean_Sea_Level_PressurehPa as int)) as stdev_Pressure,

	--Visiblity
	max(cast(Max_VisibilityKm as int)) as max_Visibility,
	min(cast(Min_VisibilityKm as int)) as min_Visibility,
	avg(cast(Mean_VisibilityKm as int)) as mean_Visibility,
	stdev(cast(Mean_VisibilityKm as int)) as stdev_Visibility,

	--Max Wind Speed
	max(cast(max_wind_speed as int)) as max_windspeed,
	avg(cast(mean_wind_speed as int)) as mean_windspeed,
	stdev(cast(mean_wind_speed as int)) as stdev_windspeed,

	--Gust Speed
	max(cast( case when Max_Gust_Speed = '' then '0' else Max_Gust_Speed end as int)) as max_gust_speed,

	--Precipitation
	avg(cast(case when precipitationmm = 'T' then null else precipitationmm end as float)) as mean_rainfall,
	stdev(cast(case when precipitationmm = 'T' then null else precipitationmm end as float)) as stdev_rainfall,
	
	--Wind Direction [Degrees]
	avg(cast(substring(WindDirDegrees,1,charindex('<',WindDirDegrees)-1) as int)) as mean_windDirDeg, 
	 
	--Cloud Cover 
	max(cast(case when cloudcover = ''  then '0' else cloudcover end as int)) as max_cloudcover,
	min(cast(case when cloudcover = ''  then '0' else cloudcover end as int)) as min_cloudcover,
	avg(cast(case when cloudcover = ''  then '0' else cloudcover end as int)) as avg_cloudcover,
	stdev_cloudcoverev(cast(case when cloudcover = ''  then '0' else cloudcover end as int)) as stdev_cloudcover,

	--Events [Avg # of specific events in 15 days
	sum(cast(case when events like '%rain%' then 1 else 0 end as float))/15 as avg_rain,
	sum(cast(case when events like '%thunderstorm%' then 1 else 0 end as float))/15 as avg_thunder,
	sum(cast(case when events like '%fog%' then 1 else 0 end as float))/15 as avg_fog,
	sum(cast(case when events like '%hail%' then 1 else 0 end as float))/15  as avg_hail,
	sum(cast(case when events like '%snow%' then 1 else 0 end as float))/15 as avg_snow,
	sum(cast(case when events like '%tornado%' then 1 else 0 end as float))/15 as avg_tornado 
	--into temp_check
	from Weather
	where Postal_Code in 
	(select top 20 postal_code from Weather group by postal_code order by count(distinct est) desc) 
	and year(est) between (@cnt-5) and (@cnt -1)
	group by 
	postal_code,month(est),case when day(est) between 0 and 15 then 'P1'  else 'P2' end
   
   SET @cnt = @cnt + 1;
END;

