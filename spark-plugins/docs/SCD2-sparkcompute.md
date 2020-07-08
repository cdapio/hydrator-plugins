# SCD2 SparkCompute plugin


Description
-----------
SCD2 SparkComptue plugin. This plugin groups the records by the specified key and sort them based on the start date field
and compute the end date based on that. The end date is computed as the start date of the next value minus 1 day. If
there is no next value, 9999-12-31 is used.

Properties
----------
**key:** The name of the key field. The records will be grouped based on the key. This field must be comparable.

**startDateField:** The name of the start date field. The grouped records are sorted based on this field.

**endDateField:** The name of the end date field. The sorted results are iterated to compute the value of this field based
on the start date.

**deduplicate:** Deduplicate records that have no changes.

**fillInNull:** Fill in null fields from most recent previous record.

**blacklist:** Blacklist for fields to ignore to compare when deduplicating the record.

**hybridSCD2:** Use SCD2 Hybrid Feature.

**placeHolderFields:** PlaceHolder fields when using SCD2 Hybrid feature.

**numPartitions:** Number of partitions to use when grouping fields. If not specified, the execution
framework will decide on the number to use.

Example
-------
For example, suppose the following records:

| id | start_date | end_date |
| -- | ---------- | -------- |
| 1  | 0          | 10       |
| 2  | 10         | 20       |
| 1  | 1000       | 5000     |
| 2  | 21         | 1000     |
| 1  | 100        | null     |
| 2  | 15         | null     |


Output records will be like:

| id | start_date | end_date |
| -- | ---------- | -------- |
| 1  | 0          | 99       |
| 1  | 100        | 999      |
| 1  | 1000       | 2932896  |
| 2  | 10         | 14       |
| 2  | 15         | 20       |
| 2  | 21         | 2932896  |