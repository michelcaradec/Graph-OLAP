// Facts-based Queries

// Sum of Sales for Strings in 2016
SELECT SUM(Sales), SUM(Units) FROM Sales WHERE Category = 'Strings' AND Year = 2016

// Sum of Sales for Strings in January 2016
SELECT SUM(Sales), SUM(Units) FROM Sales WHERE Category = 'Strings' AND Year = 2016 AND Month = 1

// Aggregate-based Queries

// By Product.Product (Aggregate_01x0x0)
SELECT Category, SUM(Sales), SUM(Units) FROM Sales GROUP BY Category

// By Product.Product (Aggregate_01x0x0)
SELECT Product, SUM(Sales), SUM(Units) FROM Sales GROUP BY Product

// Two-Levels Aggregates

// Sum of Sales for Violin in France
SELECT SUM(Sales), SUM(Units) FROM Sales WHERE Product = 'Violin' AND Country = 'France'

// Sum of Sales for Violin in 2016
SELECT SUM(Sales), SUM(Units) FROM Sales WHERE Product = 'Violin' AND Year = 2016
