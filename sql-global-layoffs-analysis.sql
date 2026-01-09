-- DATA CLEANING PROJECT

SELECT *
FROM layoffs;

-- 1. Removing Duplicates

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

SELECT *, 
ROW_NUMBER() OVER (PARTITION BY company, location, total_laid_off, `date`, percentage_laid_off, industry, source, stage, funds_raised, country, date_added) AS row_num
FROM layoffs_staging;

WITH duplicates_cte AS
(
SELECT *, 
ROW_NUMBER() OVER (PARTITION BY company, location, total_laid_off, `date`, percentage_laid_off, industry, source, stage, funds_raised, country, date_added) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicates_cte
WHERE row_num > 1;

-- Since no rows have returned, there aren't any duplicate rows.

-- 2. Standardize the Data

-- The company column has some unwanted spaces hence we will be removing it using TRIM

SELECT *
FROM layoffs_staging
WHERE company LIKE ' %';

SELECT company, TRIM(company)
FROM layoffs_staging;

UPDATE layoffs_staging
SET company = TRIM(company);

-- Changing the format of the date columns from TEXT to DATE

SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging;

UPDATE layoffs_staging
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

SELECT date_added, 
STR_TO_DATE(date_added,'%m/%d/%Y')
FROM layoffs_staging;

UPDATE layoffs_staging
SET date_added = STR_TO_DATE(date_added, '%m/%d/%Y');

ALTER TABLE layoffs_staging
MODIFY COLUMN `date` DATE;

ALTER TABLE layoffs_staging
MODIFY COLUMN date_added DATE;

-- 3. NULL Values or Blank Values

SELECT *
FROM layoffs_staging
WHERE industry IS NULL
OR industry = '';

-- Here, we got to know that the industry is BLANK for company named 'Appsmith'

UPDATE layoffs_staging
SET industry = NULL
WHERE industry = '';

-- We found out that 'Appsmith' belongs to the Software Development Applications industry hence we replaced the NULL

UPDATE layoffs_staging
SET industry = 'Software Development Applications'
WHERE industry IS NULL;

-- Deleting Unwanted Rows

SELECT total_laid_off
FROM layoffs_staging
WHERE total_laid_off = '';

UPDATE layoffs_staging
SET total_laid_off = NULL
WHERE total_laid_off = '';

SELECT percentage_laid_off
FROM layoffs_staging
WHERE percentage_laid_off = '';

UPDATE layoffs_staging
SET percentage_laid_off = NULL
WHERE percentage_laid_off = '';

SELECT total_laid_off, percentage_laid_off
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Deleting Unwanted Columns

ALTER TABLE layoffs_staging
DROP COLUMN source;

-- Populating Empty Rows

SELECT *
FROM layoffs_staging
WHERE stage = '';

-- We got to know that the stage is BLANK here for the company 'Zapp'

SELECT *
FROM layoffs_staging
WHERE company = 'Zapp';

-- We then found that 'Zapp' has another row where the stage is 'Series B' for the same industry (Food)

SELECT *
FROM layoffs_staging T1
JOIN layoffs_staging T2
ON T1.company = T2.company
WHERE T1.stage = ''
AND T2.stage <> '';

UPDATE layoffs_staging T1
JOIN layoffs_staging T2
ON T1.company = T2.company
SET T1.stage = T2.stage
WHERE T1.stage = ''
AND T2.stage <> '';

-- We have successfully populated the stage for both the rows
-- Below is the final cleaned data

SELECT *
FROM layoffs_staging;

-- Exploratory Data Analysis

SELECT *
FROM layoffs_staging;

-- Looking at Percentage of layoffs

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging;

-- The companies that had 1 which is basically 100 percent of their company laid off

SELECT *
FROM layoffs_staging
WHERE percentage_laid_off = 1;

-- these are mostly startups or maybe these companies went out of business during this time

-- then if we order by funds_raised we can see how big some of these companies were

SELECT *
FROM layoffs_staging
WHERE percentage_laid_off = 1
ORDER BY funds_raised DESC;

-- BritishVolt looks like the company which raised most funds

-- companies with the most total layoffs

SELECT company, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY company
ORDER BY 2 DESC;

-- industries with the most total layoffs

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY industry
ORDER BY 2 DESC;

-- countries with the most total layoffs

SELECT country, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY country
ORDER BY 2 DESC;

-- total  layoffs by year to identify yearly trends and peak layoff periods

SELECT YEAR(date) AS Year, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY YEAR(date)
ORDER BY 1 DESC;

-- total  layoffs by company stage

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY stage
ORDER BY 1 DESC;

-- Rolling Total Layoffs by Month

SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY `Month`
ORDER BY 1 ASC;

WITH rolling_total AS
(
SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off) AS total_off
FROM layoffs_staging
GROUP BY `Month`
ORDER BY 1 ASC
)
SELECT `Month`, total_off, SUM(total_off) OVER(ORDER BY `Month`) AS rolling_total
FROM rolling_total;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY company
ORDER BY 2 DESC;

-- Rolling Total Layoffs by Company and Year

WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`) AS Year, SUM(total_laid_off)
FROM layoffs_staging
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
ORDER BY Ranking
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5
ORDER BY years ASC;


