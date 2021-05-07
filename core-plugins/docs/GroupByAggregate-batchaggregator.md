# GroupBy Aggregate


Description
-----------
Groups by one or more fields, then performs one or more aggregate functions on each group.
Supports `Average`, `Count`, `First`, `Last`, `Max`, `Min`,`Sum`,`Collect List`,`Collect Set`, 
`Standard Deviation`, `Variance`, `Count Distinct`, `Longest String`,`Shortest String`,`Count Nulls`,
`Concat`, `Concat Distinct`, `Logical And`, `Logical Or`, `Sum Of Squares`, `Corrected Sum Of Squares`, 
`Any If`, `Average If`, `Count If`, `Max If`, `Min If`, `Sum If`, `Collect List If`, `Collect Set If`,
`Standard Deviation If`, `Variance If`, `Count Distinct If`, `Longest String If`, `Shortest String If`,
`Concat If`, `Logical And If`, `Logical Or If`, `Sum Of Squares If`, `Corrected Sum Of Squares If`
as aggregate functions.

Use Case
--------
The transform is used when you want to calculate some basic aggregations in your data similar
to what you could do with a group-by query in SQL.

Properties
----------
**groupByFields:** Comma-separated list of fields to group by.
Records with the same value for all these fields will be grouped together.
Records output by this aggregator will contain all the group by fields and aggregate fields.
For example, if grouping by the ``user`` field and calculating an aggregate ``numActions:count(*)``,
output records will have a ``user`` field and a ``numActions`` field. (Macro-enabled)

**aggregates:** Aggregates to compute on each group of records.
Supported aggregate functions are `avg`, `count`, `count(*)`, `first`, `last`, `max`, `min`,`sum`,`collectList`,
`collectSet`, `countDistinct`, `longestString`, `shortestString`, `countNulls`, `concat`, `variance` `concatDistinct`,
`stdDev`,`logicalAnd`, `logicalOr`, `sumOfSquares`, `correctedSumOfSquares`, `avgIf`, `countIf`, `maxIf`, `minIf`, 
`sumIf`, `collectListIf`, `collectSetIf`, `countDistinctIf`, `longestStringIf`, `shortestStringIf`, `concatIf`,
`varianceIf`, `anyIf`, `concatDistinctIf`, `stdDevIf` `logicalAndIf`, `logicalOrIf`, `sumOfSquaresIf`, 
`correctedSumOfSquaresIf`.
A function must specify the field it should be applied on, as well as the name it should 
be called. Aggregates are specified using the syntax `name:function(field)[, other aggregates]`.
For example, ``avgPrice:avg(price),cheapest:min(price),countPricesHigherThan:countIf(price):condition(price>500)``
will calculate three aggregates.
The first will create a field called ``avgPrice`` that is the average of all ``price`` fields in the group.
The second will create a field called ``cheapest`` that contains the minimum ``price`` field in the group.
The third will create a field ``countPricesHigherThan`` that contains the number of all ``price`` fields in the group 
that meet the condition bigger than 500.
The count function differs from count(*) in that it contains non-null values of a specific field,
while count(*) will count all records regardless of value. (Macro-enabled)

**numPartitions:** Number of partitions to use when grouping fields. If not specified, the execution
framework will decide on the number to use.

Example
-------
This example groups records by their ``user`` and ``item`` fields.
It then calculates three aggregates for each group. The first is a sum on ``price``,
the second counts the number of records in the group and third one calculates the average price of each item not taking 
into consideration prices lower than 0.50.

```json
    {
        "name": "GroupByAggregate",
        "type": "batchaggregator",
        "properties": {
            "groupByFields": "user,item",
            "aggregates": "totalSpent:sum(price),numPurchased:count(*),avgItemPrice:avgIf(price):condition(price>=0.50)"
        }
    }
```

For example, suppose the aggregator receives input records where each record represents a purchase:

| user  | item   | price |
| ----- | ------ | ----- |
| bob   | donut  | 0.80  |
| bob   | coffee | 2.05  |
| bob   | coffee | 0.35  |
| bob   | donut  | 1.50  |
| bob   | donut  | 0.50  |
| bob   | donut  | 0.45  |
| bob   | coffee | 3.50  |
| alice | tea    | 1.99  |
| alice | cookie | 0.50  |
| alice | cookie | 0.80  |
| alice | tea    | 1.50  |
| alice | tea    | 0.30  |

Output records will contain all group fields in addition to a field for each aggregate:

| user  | item   | totalSpent | numPurchased | avgItemPrice |
| ----- | ------ | ---------- | ------------ | ------------ |
| bob   | donut  | 3.25       | 4            | 0.933        |
| bob   | coffee | 5.90       | 3            | 2.775        |
| alice | tea    | 3.79       | 3            | 1.745        |
| alice | cookie | 1.30       | 2            | 0.65         |
