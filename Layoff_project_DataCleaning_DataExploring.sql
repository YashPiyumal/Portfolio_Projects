select*
from portfolioproject_1.layoffs;

-- Data Cleaning
-- 1. Remove Duplicates
-- 2. Standardize the Data 
-- 3. Null Values or Blank Values
-- 4. Remove any columns 

-- Making a copy of table
CREATE TABLE portfolioproject_1.Layoff_Staging AS
SELECT *
FROM portfolioproject_1.layoffs;

select *
from portfolioproject_1.layoff_staging;

-- Checking for duplicates
SELECT *,
ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions
        ORDER BY `date`
    ) AS row_num
FROM 
    portfolioproject_1.layoff_staging;

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions
        ORDER BY `date`
    ) AS row_num
FROM 
    portfolioproject_1.layoff_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM portfolioproject_1.layoff_staging
WHERE company = 'Casper';

-- Delete duplicates
WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions
        ORDER BY `date`
    ) AS row_num
FROM 
    portfolioproject_1.layoff_staging
)
DELETE 
FROM duplicate_cte
WHERE row_num > 1;

-- above query is not working
CREATE TABLE `layoff_staging2` (
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

SELECT *
FROM layoff_staging2;

INSERT INTO layoff_staging2
SELECT *,
ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, `date`, stage, country, funds_raised_millions
        ORDER BY `date`
    ) AS row_num
FROM 
    portfolioproject_1.layoff_staging;

SELECT *
FROM layoff_staging2
WHERE row_num > 1 ;

SET SQL_SAFE_UPDATES = 0;

DELETE 
FROM layoff_staging2
WHERE row_num > 1 ;

SELECT *
FROM layoff_staging2;

-- Standardizing Data 
SELECT company, TRIM(company)
FROM layoff_staging2;

UPDATE layoff_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoff_staging2
ORDER BY 1;

SELECT *
FROM layoff_staging2
WHERE industry LIKE 'Crypto%'
ORDER BY 3 DESC;

UPDATE layoff_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM layoff_staging2
-- WHERE industry LIKE 'Crypto%'
ORDER BY 1;

UPDATE layoff_staging2
SET country = trim(TRAILING '.' FROM Country)
WHERE country LIKE 'United States%';

SELECT 
`date`,
STR_TO_DATE(`date` ,'%m/%d/%Y')
FROM layoff_staging2;

UPDATE layoff_staging2
SET `date` = STR_TO_DATE(`date` ,'%m/%d/%Y');

select `date`
FROM layoff_staging2;

ALTER TABLE layoff_staging2
MODIFY COLUMN `date` DATE;

-- Working with Null Values or Blank Values

SELECT *
FROM layoff_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoff_staging2
WHERE company = 'Airbnb';

SELECT *
FROM layoff_staging2 t1
JOIN layoff_staging2 t2
  ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND (t2.industry IS NOT NULL OR t2.industry <> '');

UPDATE layoff_staging2 t1
JOIN layoff_staging2 t2
  ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND (t2.industry IS NOT NULL OR t2.industry <> '');

UPDATE layoff_staging2
SET industry = null
WHERE industry = '';

SELECT *
FROM layoff_staging2
WHERE company LIKE 'Bally%';

-- Remove any columns that has meaningful data
SELECT * 
FROM layoff_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

DELETE 
FROM layoff_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

SELECT *
FROM layoff_staging2;

AlTER TABLE layoff_staging2
DROP COLUMN row_num;

-- Exploratory Data Analysis

SELECT *
FROM layoff_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoff_staging2;

SELECT *
FROM layoff_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoff_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoff_staging2;

SELECT industry, SUM(total_laid_off)
FROM layoff_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoff_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT country, company, industry, SUM(total_laid_off)
FROM layoff_staging2
GROUP BY country, company, industry
ORDER BY 4 DESC, 1;

SELECT year(`date`), SUM(total_laid_off)
FROM layoff_staging2
GROUP BY year(`date`)
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoff_staging2
GROUP BY stage
ORDER BY 1 DESC;

SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off)
FROM layoff_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1;

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off) AS total_off
FROM layoff_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1
)
SELECT `Month`, total_off, SUM(total_off) OVER(ORDER BY `Month`) AS rolling_total
FROM Rolling_Total; 

SELECT company, YEAR(`date`) ,SUM(total_laid_off)
FROM layoff_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year (company, years, total_laid_off) AS 
(
SELECT company, YEAR(`date`) ,SUM(total_laid_off)
FROM layoff_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;