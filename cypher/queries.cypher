// Facts-based Queries

// Sum of Sales for Strings in 2016
MATCH (cat:Category {category: 'Strings'})<-[*]-(meas:Measure)
MATCH (year:Year {year: 2016})<-[*]-(meas:Measure)
RETURN
    SUM(meas.sales) AS Sales,
    SUM(meas.units) AS Units;

// Sum of Sales for Strings in January 2016
MATCH (cat:Category {category: 'Strings'})<-[*]-(meas:Measure)
MATCH (year:Year {year: 2016})<-[]-(month:Month {month: 1})<-[]-(meas:Measure)
RETURN
    SUM(meas.sales) AS Sales,
    SUM(meas.units) AS Units;

// or
MATCH (cat:Category {category: 'Strings'})<-[*]-(meas:Measure)
MATCH (month:Month {year: 2016, month: 1})<-[]-(meas:Measure)
RETURN
    SUM(meas.sales) AS Sales,
    SUM(meas.units) AS Units;

// Detail of Sales for Strings in January 2016
MATCH (cat:Category {category: 'Strings'})<-[]-(prod:Product)<-[]-(meas:Measure)
MATCH (year:Year {year: 2016})<-[]-(month:Month {month: 1})<-[]-(meas:Measure)
RETURN
    year.year AS Year,
    month.month AS Month,
    cat.category AS Category,
    prod.product AS Product,
    meas.sales AS Sales,
    meas.units AS Units
LIMIT 10;

// Aggregate-based Queries

// Sum of Sales for Violin in France
MATCH (prod:Product {product: 'Violin'})<-[]-(a:Aggregate_0101x0)-[]->(ct:Country {country: 'France'})
RETURN
    a.sales AS Sales,
    a.units AS Units;

// Sum of Sales for Violin in 2016
MATCH (prod:Product {product: 'Violin'})<-[]-(a:Aggregate_01x010)-[]->(ct:Year {year: 2016})
RETURN
    a.sales AS Sales,
    a.units AS Units;
