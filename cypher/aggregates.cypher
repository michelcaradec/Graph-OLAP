// One-Level Aggregates

// Category Aggregates
MATCH (cat:Category)<-[*2]-(meas:Measure)
WITH DISTINCT cat, SUM(meas.sales) AS SumSales, SUM(meas.units) As SumUnits
CREATE (a:Aggregate_10x0x0 {sales: SumSales, units: SumUnits})-[:AGGREGATE_OF]->(cat);
// Added 3 labels, created 3 nodes, set 6 properties, created 3 relationships, completed after 337 ms.

// Product Aggregates
MATCH (prod:Product)<-[]-(meas:Measure)
WITH DISTINCT prod, SUM(meas.sales) AS SumSales, SUM(meas.units) As SumUnits
CREATE (a:Aggregate_01x0x0 {sales: SumSales, units: SumUnits})-[:AGGREGATE_OF]->(prod);
// Added 11 labels, created 11 nodes, set 22 properties, created 11 relationships, completed after 59 ms.

// Region Aggregates
MATCH (r:Region)<-[*2]-(meas:Measure)
WITH DISTINCT r, SUM(meas.sales) AS SumSales, SUM(meas.units) As SumUnits
CREATE (a:Aggregate_x010x0 {sales: SumSales, units: SumUnits})-[:AGGREGATE_OF]->(r);
// Added 5 labels, created 5 nodes, set 10 properties, created 5 relationships, completed after 36 ms.

// Country Aggregates
MATCH (ct:Country)<-[]-(meas:Measure)
WITH DISTINCT ct, SUM(meas.sales) AS SumSales, SUM(meas.units) As SumUnits
CREATE (a:Aggregate_x001x0 {sales: SumSales, units: SumUnits})-[:AGGREGATE_OF]->(ct);
// Added 16 labels, created 16 nodes, set 32 properties, created 16 relationships, completed after 28 ms.

// Year Aggregates
MATCH (y:Year)<-[*2]-(meas:Measure)
WITH DISTINCT y, SUM(meas.sales) AS SumSales, SUM(meas.units) As SumUnits
CREATE (a:Aggregate_x0x010 {sales: SumSales, units: SumUnits})-[:AGGREGATE_OF]->(y);
// Added 2 labels, created 2 nodes, set 4 properties, created 2 relationships, completed after 48 ms.

// Month Aggregates
MATCH (m:Month)<-[]-(meas:Measure)
WITH DISTINCT m, SUM(meas.sales) AS SumSales, SUM(meas.units) As SumUnits
CREATE (a:Aggregate_x0x001 {sales: SumSales, units: SumUnits})-[:AGGREGATE_OF]->(m);
// Added 24 labels, created 24 nodes, set 48 properties, created 24 relationships, completed after 23 ms.

// Two-Levels Aggregates

// Product x Country Aggregates
MATCH (prod:Product)<-[]-(meas:Measure)
MATCH (ct:Country)<-[]-(meas:Measure)
WITH DISTINCT prod, ct, SUM(meas.sales) AS SumSales, SUM(meas.units) As SumUnits
CREATE (prod)<-[:AGGREGATE_OF]-(a:Aggregate_0101x0 {sales: SumSales, units: SumUnits})-[:AGGREGATE_OF]->(ct);
// Added 176 labels, created 176 nodes, set 352 properties, created 352 relationships, completed after 114 ms.

// Product (leaf level) x Year (level 1) Aggregates
MATCH (prod:Product)<-[]-(meas:Measure)
MATCH (y:Year)<-[]-(m:Month)<-[]-(meas:Measure)
WITH DISTINCT prod, y, SUM(meas.sales) AS SumSales, SUM(meas.units) As SumUnits
CREATE (prod)<-[:AGGREGATE_OF]-(a:Aggregate_01x010 {sales: SumSales, units: SumUnits})-[:AGGREGATE_OF]->(y);
// Added 22 labels, created 22 nodes, set 44 properties, created 44 relationships, completed after 73 ms.
