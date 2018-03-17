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
WITH c, m, prod, row
// Facts
OPTIONAL MATCH
	(meas:Measure)-[:IN_PLACE]->(c),
    (meas:Measure)-[:AT_TIME]->(m),
    (meas:Measure)-[:FOR_PRODUCT]->(prod)
// Create new fact node
FOREACH (x IN CASE WHEN meas IS NULL THEN [1] ELSE [] END |
    CREATE (meas:Measure {sales: toFloat(row.Sales), units: toInteger(row.Units)})
    // Foreign keys
    CREATE (meas)-[:IN_PLACE]->(c)
    CREATE (meas)-[:AT_TIME]->(m)
    CREATE (meas)-[:FOR_PRODUCT]->(prod)
)
// Update fact node
FOREACH (x IN CASE WHEN meas IS NULL THEN [] ELSE [1] END |
    SET meas.sales = meas.sales + toFloat(row.Sales)
    SET meas.units = meas.units + toInteger(row.Units)
);

