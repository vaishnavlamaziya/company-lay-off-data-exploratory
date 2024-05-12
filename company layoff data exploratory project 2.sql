-- DATA CLEANING -- 

-- EDA

-- Here we are jsut going to explore the data and find trends or patterns or anything interesting like outliers --

-- normally when you start the EDA process you have some idea of what you're looking for --

-- with this info we are just going to look around and see what we find! --

select * from layoffs

create table layoffs_staging
like layoffs ;
insert layoffs_staging
select * from layoffs;

select * from layoffs_staging;


-- 1. REMOVE DUPLICATES --

select *,
row_number() over( partition by company, location, industry, total_laid_off, percentage_laid_off, date,
 stage, country, funds_raised_millions) as row_num
 from layoffs_staging 
 


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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over( partition by company, location, industry, total_laid_off, percentage_laid_off, 'date',
 stage, country, funds_raised_millions) as row_num
 from layoffs_staging
 
 select * from layoffs_staging2
 where row_num  >1;


 delete from layoffs_staging2
 where row_num  >1;
 
  select * from layoffs_staging2

-- 2. STANDARDIZING THE DATA --

select company, trim(company)
from layoffs_staging2 ;

update layoffs_staging2
set company = trim(company);

select * from layoffs_staging2
  
select distinct(industry)
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'crypto'
where industry like 'cryoto%'

select *
from layoffs_staging2
where industry like 'crypto%'

select distinct location
from layoffs_staging2
order by 1;

select distinct country
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where country like 'united states%'
order by 1;

update layoffs_staging2
set country = 'United states'
where country like 'united states%'

-- OR --
select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United states%'

select *
from layoffs_staging2
where country like 'United states%'

-- changing date column dataset text to date -------
select date
from layoffs_staging2;

select date,
STR_TO_DATE(DATE,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set date = STR_TO_DATE(DATE,'%m/%d/%Y');

alter table layoffs_staging2
modify column date date;

select * 
from  layoffs_staging2;


-- 3. ELIMINATING NULL & BLANK VALUES IF ANY --

select * from layoffs_staging2
where company like'airbnb%' ;

select * from layoffs_staging2
where industry is null;

update layoffs_staging2
set industry = null
where industry = ' ';

select * from layoffs_staging2 t1
join layoffs_staging2 t2 on t1.company = t2.company	
where t1.industry is null and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2 on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select * from layoffs_staging2

-- 4. REMOVE UNNECESSARY COLUMNS --

select * from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete  from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select * from layoffs_staging2

alter table layoffs_staging2
drop  column row_num;


-- EDA - exploratory data analysis --

select * from layoffs_staging2

select  max( total_laid_off)
from layoffs_staging2;

select  max(total_laid_off)
from layoffs_staging2;

select  min(total_laid_off)
from layoffs_staging2;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by sum(total_laid_off) desc;

select * 
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

select min(`date`) , max(`date`)
 from layoffs_staging2
 
select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by sum(total_laid_off) desc;

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by sum(total_laid_off) desc;

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 2 desc

select month(`date`), sum(total_laid_off)
from layoffs_staging2
group by month(`date`)
order by 2 desc

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc

select stage, sum(percentage_laid_off)
from layoffs_staging2
group by stage
order by 2 desc

-- rolling sum on month---

select substring( `date`,6,2) as `month`,sum(total_laid_off)
from layoffs_staging2
group by `month`
order by sum(total_laid_off) desc ;


select substring( `date`,1,7) as `month`,sum(total_laid_off)
from layoffs_staging2
where substring( `date`,1,7) is not null
group by `month`
order by 1 

with Rolling_total as (
select substring( `date`,1,7) as `month`,sum(total_laid_off)      
from layoffs_staging2
where substring( `date`,1,7) is not null
group by `month`
order by 1 
)
select * from Rolling_total


WITH Rolling_total AS (
    SELECT SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off) AS total_off
    FROM layoffs_staging2
    WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
    GROUP BY `month`
    ORDER BY `month`
)
SELECT *
FROM Rolling_total;


select `month`, total_off, sum(total_off)over(order by `month`) as rolling_total
from Rolling_total;

WITH Rolling_total AS (
    SELECT SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off) AS total_off
    FROM layoffs_staging2
    WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
    GROUP BY `month`
    ORDER BY 1 asc
)
select `month`, total_off, sum(total_off)over(order by `month`) as rolling_total
from Rolling_total;


select company,year(`date`),SUM(total_laid_off)
from layoffs_staging2
group by company,year(`date`)
order by 1 asc;

select company,year(`date`),SUM(total_laid_off)
from layoffs_staging2
group by company,year(`date`)
order by 3 desc;

with Company_year(copmany,year, total_laid_off) as
(
select company,year(`date`),SUM(total_laid_off)
from layoffs_staging2
group by company,year(`date`)
order by 3 desc
), company_year_rank as
(select *, dense_rank() over(partition by year order by total_laid_off desc) as Ranking
from Company_year
where year is not null
)
    select * from
    company_year_rank
    where ranking <=5;
 -- this will give top 5 companylaid their employees in each year --
 