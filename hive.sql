
-- create hive table

CREATE TABLE books (
             bookID INT,
             title STRING,
             authors STRING,
             average_rating FLOAT,
             isbn  STRING,
             isbn13  STRING,
             language_code  STRING,
             num_pages  INT,
             ratings_count INT,
             text_reviews_count INT,
             publication_date  DATE,
             publisher  STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES('skip.header.line.count'='1');


