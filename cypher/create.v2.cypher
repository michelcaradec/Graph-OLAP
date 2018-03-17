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
