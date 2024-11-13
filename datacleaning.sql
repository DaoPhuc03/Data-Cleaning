-- Bắt đầu quá trình 
USE world_layoffs;

-- Tạo bảng tạm để làm việc với dữ liệu gốc
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

-- Chèn dữ liệu vào bảng tạm
INSERT INTO world_layoffs.layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

-- Bước 1: Kiểm tra và loại bỏ các bản sao trong dữ liệu

-- Kiểm tra các bản sao
SELECT company, industry, total_laid_off, `date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off, `date`) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

-- Lấy ra các bản ghi có giá trị thực sự
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- Xóa các bản sao
WITH DELETE_CTE AS 
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1
)
DELETE FROM DELETE_CTE;

-- Bước 2: Chuẩn hóa dữ liệu

-- Kiểm tra các giá trị null trong cột industry
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging;

-- Cập nhật các giá trị trống thành NULL
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Cập nhật các giá trị NULL dựa trên các hàng khác cùng công ty
UPDATE world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Chuẩn hóa các giá trị ngành nghề
UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Chuẩn hóa các giá trị quốc gia
UPDATE world_layoffs.layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Chuyển đổi định dạng ngày tháng
UPDATE world_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Thay đổi kiểu dữ liệu của cột ngày tháng
ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Bước 3: Kiểm tra các giá trị NULL

-- Kiểm tra các giá trị NULL trong các cột total_laid_off, percentage_laid_off, và funds_raised_millions
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;

-- Bước 4: Loại bỏ các hàng và cột không cần thiết

-- Xóa các hàng không có giá trị hữu ích
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Xóa cột tạm thời row_num
ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;

-- Kiểm tra lại dữ liệu sau khi dọn dẹp
SELECT * 
FROM world_layoffs.layoffs_staging2;