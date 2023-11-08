CREATE DATABASE mydatabase;

USE mydatabase;

CREATE TABLE table_gc7 (
    Retailer VARCHAR(50),
    Retailer_ID INT,
    Invoice_Date VARCHAR(50),
    Region VARCHAR(50),
    State VARCHAR(50),
    City VARCHAR(50),
    Product VARCHAR(50),
    Price_per_Unit VARCHAR(50),
    Units_Sold VARCHAR(50),
    Total_Sales VARCHAR(50),
    Operating_Profit VARCHAR(50),
    Operating_Margin VARCHAR(50),
    Sales_Method VARCHAR(50)
);

COPY table_gc7 (
	Retailer,
    Retailer_ID,
    Invoice_Date,
    Region,
    State,
    City,
    Product,
    Price_per_Unit,
    Units_Sold,
    Total_Sales,
    Operating_Profit,
    Operating_Margin,
    Sales_Method
)
FROM 'C:\Users\LENOVO\p2-ftds008-hck-g7-taaratheresia\P2G7_taara_data_raw.csv'
DELIMITER ','
CSV HEADER;

SELECT * FROM table_gc7
