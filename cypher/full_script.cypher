// -------
// Indexes
// -------

// Dimensions
CREATE INDEX ON :Country(country);
CREATE INDEX ON :Product(product);
CREATE INDEX ON :Month(month);
// Measures
CREATE INDEX ON :Measure(mid);

// -----------
// Data Import
// -----------

UNWIND ["sales.2016.tsv", "sales.2017.tsv"] AS sourceFile
//USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS
    FROM "file:///" + sourceFile
    AS row
    FIELDTERMINATOR '\t'
// Place dimension
MERGE (r:Region {region: row.Region})
MERGE (c:Country {country: row.Country})
MERGE (c)-[:IN_REGION]->(r)
// Time dimension
MERGE (y:Year {year: toInteger(row.Year)})
// Year property must be added to Month node in order to differentiate months of different years.
MERGE (m:Month {year: toInteger(row.Year), month: toInteger(row.Month)})
MERGE (m)-[:OF_YEAR]->(y)
// Product dimension
MERGE (cat:Category {category: row.Category})
MERGE (prod:Product {product: row.Product})
MERGE (prod)-[:IN_CATEGORY]->(cat)
WITH
    c, m, prod, row,
    c.country + '_' + toString(m.year) + '_' + toString(m.month) + '_' + prod.product AS MeasureID
// Facts
MERGE (meas:Measure {mid: MeasureID})
ON CREATE
    // Create new fact node
	SET meas.sales = toFloat(row.Sales),
    meas.units = toInteger(row.Units)
ON MATCH
    // Update fact node
	SET meas.sales = meas.sales + toFloat(row.Sales),
    meas.units = meas.units + toInteger(row.Units)
// Foreign keys
MERGE (meas)-[:IN_PLACE]->(c)
MERGE (meas)-[:AT_TIME]->(m)
MERGE (meas)-[:FOR_PRODUCT]->(prod);

// --------------------
// One-Level Aggregates
// --------------------

// Category Aggregates
MATCH (cat:Category)<-[*2]-(meas:Measure)
WITH DISTINCT cat, SUM(meas.sales) AS SumSales, SUM(meas.units) As SumUnits
CREATE (a:Aggregate_10x0x0 {sales: SumSales, units: SumUnits})-[:AGGREGATE_OF]->(cat);

// Product Aggregates
MATCH (prod:Product)<-[]-(meas:Measure)
WITH DISTINCT prod, SUM(meas.sales) AS SumSales, SUM(meas.units) As SumUnits
CREATE (a:Aggregate_01x0x0 {sales: SumSales, units: SumUnits})-[:AGGREGATE_OF]->(prod);

// Region Aggregates
MATCH (r:Region)<-[*2]-(meas:Measure)
WITH DISTINCT r, SUM(meas.sales) AS SumSales, SUM(meas.units) As SumUnits
CREATE (a:Aggregate_x010x0 {sales: SumSales, units: SumUnits})-[:AGGREGATE_OF]->(r);

// Country Aggregates
MATCH (ct:Country)<-[]-(meas:Measure)
WITH DISTINCT ct, SUM(meas.sales) AS SumSales, SUM(meas.units) As SumUnits
CREATE (a:Aggregate_x001x0 {sales: SumSales, units: SumUnits})-[:AGGREGATE_OF]->(ct);

// Year Aggregates
MATCH (y:Year)<-[*2]-(meas:Measure)
WITH DISTINCT y, SUM(meas.sales) AS SumSales, SUM(meas.units) As SumUnits
CREATE (a:Aggregate_x0x010 {sales: SumSales, units: SumUnits})-[:AGGREGATE_OF]->(y);

// Month Aggregates
MATCH (m:Month)<-[]-(meas:Measure)
WITH DISTINCT m, SUM(meas.sales) AS SumSales, SUM(meas.units) As SumUnits
CREATE (a:Aggregate_x0x001 {sales: SumSales, units: SumUnits})-[:AGGREGATE_OF]->(m);

// ---------------------
// Two-Levels Aggregates
// ---------------------

// Product x Country Aggregates
MATCH (prod:Product)<-[]-(meas:Measure)
MATCH (ct:Country)<-[]-(meas:Measure)
WITH DISTINCT prod, ct, SUM(meas.sales) AS SumSales, SUM(meas.units) As SumUnits
CREATE (prod)<-[:AGGREGATE_OF]-(a:Aggregate_0101x0 {sales: SumSales, units: SumUnits})-[:AGGREGATE_OF]->(ct);

// Product (leaf level) x Year (level 1) Aggregates
MATCH (prod:Product)<-[]-(meas:Measure)
MATCH (y:Year)<-[]-(m:Month)<-[]-(meas:Measure)
WITH DISTINCT prod, y, SUM(meas.sales) AS SumSales, SUM(meas.units) As SumUnits
CREATE (prod)<-[:AGGREGATE_OF]-(a:Aggregate_01x010 {sales: SumSales, units: SumUnits})-[:AGGREGATE_OF]->(y);
