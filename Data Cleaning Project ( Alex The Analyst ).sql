use `project cleaning data`; -- put backtick when dosent used underscore between name of the data base 
select * from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Vlaues or Blank Vlaues 
-- 4. Remove Any Columns 

create table layoffs_staging 
like layoffs;
select * from layoffs_staging;

Insert into layoffs_staging     -- inserting into staging table .. that is the next line select * from layoffs
select * from layoffs;

select * from layoffs_staging;  -- executing this line to see the staging  table 

select * , 
row_number() over ( 
partition by company ,location, industry , total_laid_off, percentage_laid_off, `date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging;

WITH duplicate_cte AS (
select * , 
row_number() over ( 
partition by company ,location, industry , total_laid_off, percentage_laid_off, `date`,stage,country , funds_raised_millions
) as row_num
from layoffs_staging
)

select * from duplicate_cte 
where row_num > 1;

delete from layoffs_staging where row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;
insert into layoffs_staging2
select * , 
row_number() over ( 
partition by company ,location, industry , total_laid_off, percentage_laid_off, `date`,stage,country , funds_raised_millions
) as row_num
from layoffs_staging;

select * from layoffs_staging2;

SET SQL_SAFE_UPDATES = 0;
delete from layoffs_staging2 where row_num > 1;

 -- Standardizing the data

select distinct(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct (industry) from layoffs_staging2 order by 1;

update layoffs_staging2
set industry = "Crypto"
where industry = "Crypto%";

select * from layoffs_staging2 where industry = "Crypto" ;

select distinct(country) from layoffs_staging2 order by 1; 

update layoffs_staging2
set country = "United States"
where country = "United States.";


select `date`,
str_to_date(`date`,"%m/%d/%Y") as Cleaned_Date
from layoffs_staging2;

update layoffs_staging2
set `date`= str_to_date(`date`,"%m/%d/%Y");

select * from layoffs_staging2;

alter table layoffs_staging2
modify column `date` date;


-- important 

select * from layoffs_staging where industry is null;

-- now lets consider company is Airbnb and it has industry as travel in one of its data out of 2 .. one with travel as
-- industry and one blank as industry but with same company Airbnb .. we should not delete it .. we should fill the 
-- empty with travel .. for all this kind of stuffs that is missing data .. below querry for finding all this kind of stuffs 
-- just practicing .. we dont have that kind of data in our dataset 

select t1.industry , t2.industry from layoffs_staging2 t1                   -- t1 is null or blank while t2 is not null ,
join layoffs_staging2 t2                     -- so convert all blank into null so that null comapring with not null .. easy
on t1.company = t2.company                             -- update layoffs_staging2 
where t1.industry is null or t1.industry = ' '   -- set industry = NULL where industry = " " after doing left side query execute
and t2.industry is not null     ;                 -- without t1.industry = " " because all converted to null 


-- upper query will return 1st industry as empty and 2nd with values for same company 
-- down query for updating the 2nd column value in 1st column also 

update layoff_staging2 t1
join layoffs_staging2 t2                     -- so convert all blank into null so that null comapring with not null .. easy
on t1.company = t2.company 
set t1.industry = t2.industry 
where t1.industry is null 
and t2.industry is not null;

select * from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null ;

delete from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null ;

select * from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;

-- Exploratory Data Analysis

select max(total_laid_off)
from layoffs_staging2;

select company , sum(total_laid_off)
from layoffs_staging2
group by company 
order by 2 desc ;  -- i want to order desc based on sum(total_laid_off) which is 2nd column , so order by 2 desc 

select industry , sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc ; 

select country , sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc ; 

select year(`date`) , sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 2 desc ; 

select stage , sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc ; 

select * from layoffs_staging2;
 
select substring(`date`,1,7) as `MONTH` , sum(total_laid_off) 
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `MONTH`
order by 1 asc;

with rolling_total as (
select substring(`date`,1,7) as `MONTH` , sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `MONTH`
order by 1 asc
)

select `MONTH` , total_off , 
sum(total_off) over(order by `MONTH`) as rolling_total   -- This is like MTD calcukation like in Power BI
from rolling_total;


select company , year(`date`) , sum(total_laid_off)
from layoffs_staging2 
group by company , year(`date`)
order by 3 desc;

with Company_Year(Company , `year` , Total_Laid_Off) as (
select company , year(`date`) , sum(total_laid_off)
from layoffs_staging2 
group by company , year(`date`)
) , Company_Year_Rank as (
select * ,
dense_rank() over ( partition by `year` order by Total_Laid_Off desc ) as Ranking
from Company_Year
where `year` is not null
)

select * from Company_Year_Rank
where Ranking <= 5;



