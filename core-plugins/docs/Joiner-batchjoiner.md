# Joiner

Description
-----------
Joins records from one or more input based on join key equality.
Supports `inner` and `outer` joins, selection and renaming of output fields.
The plugin is used when you want to combine fields from one or more inputs, similar to joins in SQL.

Properties
----------
**Fields:** List of fields from each input that should be included in the output. 
Output field names must be unique. If the same field name exists in more than one input,
each field must be aliased (renamed) to a unique output name.

**Join Type:** Type of join to perform.
A join between two required input is an inner join. A join between a required input and an optional
input is a left outer join. A join between two optional inputs is an outer join.

A join of more than two inputs is logically equivalent to performing inner joins over all the
required inputs, followed by left outer joins on the optional inputs.

**Join Condition:** List of keys to perform the join operation. The list is separated by `&`. 
Join key from each input stage will be prefixed with `<stageName>.` and the relation among join keys from different inputs is represented by `=`. 
For example: customers.customer_id=items.c_id&customers.customer_name=items.c_name means the join key is a composite key
of customer id and customer name from customers and items input stages and join will be performed on equality 
of the join keys. This transform only supports equality for joins.

**Inputs to Load in Memory:** Hint to the underlying execution engine that the specified input data should be
loaded into memory to perform an in-memory join. This is ignored by the MapReduce engine and passed onto the Spark engine.
An in-memory join performs well when one side of the join is small (for example, under 1gb). Be sure to set
Spark executor memory to a number large enough to load all of these datasets into memory. This is most commonly
used when a large input is being joined to a small input and will lead to much better performance in such scenarios.

**Join on Null Keys:** Whether to join rows together if both of their key values are null.
For example, suppose the join is on a 'purchases' input that contains:

| purchase_id | customer_name | item   |
| ----------- | ------------- | ------ |
| 1           | alice         | donut  |
| 2           |               | coffee | 
| 3           | bob           | water  |

and a 'customers' input that contains:

| customer_id | name   |
| ----------- | ------ |
| 1           | alice  |
| 2           |        |
| 3           | bob    |

The join is a left outer join on purchases.customer_name = customers.name.
If this property is set to true, the joined output would be:

| purchase_id | customer_name | item   | customer_id | name  |
| ----------- | ------------- | ------ | ----------- | ----- |
| 1           | alice         | donut  | 1           | alice |
| 2           |               | coffee | 2           |       |
| 3           | bob           | water  | 3           | bob   |

Note that the rows with a null customer name were joined together, with customer_id set to 2 for purchase 2.
If this property is set to false, the joined output would be:

| purchase_id | customer_name | item   | customer_id | name  |
| ----------- | ------------- | ------ | ----------- | ----- |
| 1           | alice         | donut  | 1           | alice |
| 2           |               | coffee |             |       |
| 3           | bob           | water  | 3           | bob   |

In this scenario, the null customer name on the left did not get joined to the null customer name on the right.
Traditional relational database systems do not join on null key values.
In most situations, you will want to do the same and set this to false. 
Setting it to true can cause a large drop in performance if there are a lot of null keys in your input data.

**Number of Partitions:** Number of partitions to use when grouping fields. If not specified, the execution
framework will decide on the number to use.

Example
-------
This example performs an inner join on records from ``customers`` and ``purchases`` inputs
 on customer id. It selects customer_id, name, item and price fields as the output fields.
This is equivalent to a SQL query like:

```
SELECT customes.id as customer_id, customers.first_name as name, purchases.item, purchases.price
FROM customers 
INNER JOIN purchases
ON customers.id = purchases.customer_id
```

For example, suppose the joiner receives input records from customers and purchases as below:


| id | first_name | last_name |  street_address      |   city    | state | zipcode | phone number |
| -- | ---------- | --------- | -------------------- | --------- | ----- | ------- | ------------ |
| 1  | Douglas    | Williams  | 1, Vista Montana     | San Jose  | CA    | 95134   | 408-777-3214 |
| 2  | David      | Johnson   | 3, Baypointe Parkway | Houston   | TX    | 78970   | 804-777-2341 |
| 3  | Hugh       | Jackman   | 5, Cool Way          | Manhattan | NY    | 67263   | 708-234-2168 |
| 4  | Walter     | White     | 3828, Piermont Dr    | Orlando   | FL    | 73498   | 201-734-7315 |
| 5  | Frank      | Underwood | 1609 Far St.         | San Diego | CA    | 29770   | 201-506-8756 |
| 6  | Serena     | Woods     | 123 Far St.          | Las Vegas | Nv    | 45334   | 888-605-3479 |


| customer_id | item   | price |
| ----------- | ------ | ----- |
| 1           | donut  | 0.80  |
| 1           | coffee | 2.05  |
| 2           | donut  | 1.50  |
| 2           | plate  | 0.50  |
| 3           | tea    | 1.99  |
| 5           | cookie | 0.50  |

Output records will contain inner join on customer id:

| customer_id | name    | item   | price |
| ----------- | ------- | ------ | ----- |
| 1           | Douglas | donut  | 0.80  |
| 1           | Douglas | coffee | 2.05  |
| 2           | David   | donut  | 1.50  |
| 2           | David   | plate  | 0.50  |
| 3           | Hugh    | tea    | 1.99  |
| 5           | Frank   | cookie | 0.50  |
