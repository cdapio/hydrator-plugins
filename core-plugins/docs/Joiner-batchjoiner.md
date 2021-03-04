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

**Join Condition Type:** Type of join condition to use. A condition can either be 'Basic' or 'Advanced'.
Advanced join conditions cannot be used in streaming pipelines or with the MapReduce engine.
Advanced join conditions can only be used when joining two inputs.

**Join Condition:** When the condition type is 'Basic', the condition specifies the list of keys to perform the join operation.
The join will be performed based on equality of the join keys.
When the condition type is 'Advanced', the condition can be any SQL expression supported by the engine.
It is important to note that advanced join conditions can be many times more expensive than basic joins performed on equality.
Advanced outer joins must load one of the inputs into memory.
Advanced inner joins do not need to load an input into memory.
However, without an in-memory input, the engine will be forced to calculate a very expensive cartesian product.

**Input Aliases:** When using advanced join conditions, input aliases can be specified to make the SQL expression
more readable. For example, if the join inputs are named 'User Signups 2020' and 'Signup Locations',
they can be aliased to a simpler names like 'users' and 'locations'. This allows you to use a simpler condition like 
'users.loc = locations.id or users.loc_name = locations.name'.

**Inputs to Load in Memory:** Hint to the underlying execution engine that the specified input data should be
loaded into memory to perform an in-memory join. This is ignored by the MapReduce engine and passed onto the Spark engine.
An in-memory join performs well when one side of the join is small (for example, under 1gb). Be sure to set
Spark executor and driver memory to a number large enough to load all of these datasets into memory. This is most commonly
used when a large input is being joined to a small input and will lead to much better performance in such scenarios.
A general rule of thumb is to set executor and driver memory to fives times the dataset size.

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

**Distribution:** Enabling distribution will increase the level of parallelism when joining skewed data. 
A skewed join happens when a significant percentage of input records have the same key. Distribution is
 possible when the following conditions are met:
1. There are exactly two input stages. 
1. Broadcast is not enabled for either stage.
1. The skewed input is marked as `required`.


Distribution requires two parameters:
1. **Distribution Size:** This controls the size of the salt that will be generated for distribution. The **Number of Partitions** 
property should be greater than or equal to this number for optimal results. A larger value will lead to more 
parallelism but it will also grow the size of the non-skewed dataset by this factor.
1. **Skewed Input Stage:**  Name of the skewed input stage. The skewed input stage is the one that contains many rows that join 
to the same row in the non-skewed stage. Ex. If stage A has 10 rows that join on the same row in stage B, then
stage A is the skewed input stage.

For more information about Distribution and data skew, please see the **Skew** section of this documentation.

Skew
----------
### Problem
Data skew is an important characteristic to consider when implementing joins. A skewed join happens
when a significant percentage of input records in one dataset have the same key and therefore join to
the same record in the second dataset. This is problematic due to the way the execution framework handles
joins. At a high level, all records with matching keys are grouped into a partition, these partitions 
are distributed across the nodes in a cluster to perform the join operation. In a skewed join, one 
or more of these partitions will be significantly larger than the rest, which will result in a majority 
of the workers in the cluster remaining idle while a couple workers process the large partitions. This
results in poor performance since the cluster is being under utilized.

### Solution 1: In-Memory Join (Spark only)
*This option is only available if the Spark engine is used, MapReduce 
does not support In-Memory joins.*

The first approach for increasing performance of skewed joins is using an In-Memory join. An in-memory
join is a performance improvement when a large dataset is being joined with a small dataset. In this
approach, the small dataset is loaded into memory and broadcast to workers and loaded into 
workers memory. Once it is in memory, a join is performed by iterating through the elements of the
large dataset. With this approach, data from the large dataset is NEVER shuffled. Data with the 
same key can be joined in parallel across the cluster instead of handled only by a single worker 
providing optimal performance. Data sets that have a small size can be used for in-memory joins. 
Make sure the total size of broadcast data does not exceed **2GB.**
  
### Solution 2: Distribution
Distribution should be used when the smaller dataset cannot fit into memory. This solution solves the 
data skew problem by adding a composite key (salt) on both the datasets and by specifying a join condition 
with a combination of composite keys along with original keys to be joined. The addition of the 
composite key allows the data to be spread across more workers therefore increasing parallelism which 
increases overall performance. The following example illustrates how salting works and how it is used for distribution:

Suppose the skewed side (Stage A) has data like:

| id | country |
|----|---------|
| 0  | us      |
| 1  | us      |
| 2  | us      |
| 3  | gb      |

where most of the data has the same value for the country. The unskewed side (Stage B) has data like:

 | country | code | 
 | ------- | ---- | 
 | us      | 1    | 
 | gb      | 44   | 

The join key is country. With a `Distribution Size` of 2 and `Skewed Stage` of 'Stage A', a new salt
column is added to the skewed data, where the value is a random number between 0 and 1:

| id | country | salt |
|----|---------|------|
| 0  | us      | 0    |
| 1  | us      | 1    |
| 2  | us      | 0    |
| 2  | gb      | 1    |


The unskewed data is exploded, where each row is becomes 2 rows, one for each value between 0 and `Distribution Size`:

 | country | code | salt | 
 | ------- | ---- | ---- | 
 | us      | 1    | 0    | 
 | us      | 1    | 1    | 
 | gb      | 44   | 0    | 
 | gb      | 44   | 1    | 
 
 The salt column is added to the join key and the join can be performed as normal. 
However, now the skewed key can be processed across two workers which increases the performance.

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
