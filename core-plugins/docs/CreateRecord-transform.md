# Create Hierarchy transform

Description
-----------
Transform plugin generates hierarchical structures from flat schemas.

Use Case
--------
Collapse flat data into structures.


Properties
----------
**Mapping:** Specifies the mapping for generating the hierarchy.
**Include fields missing in mapping:** Specifies whether the fields in the input schema that are not part of the mapping,
should be carried over as-is.

Example
-------
Let's say we have two following data structure:

| field                 | type         |
| ------------          | ------------ |
| customer_id           | Int          |
| customer_name         | String       |
| customer_phone        | String       |
| order_id              | Int          |
| product_id            | Int          |
| amount                | Double       |
| order_description     | String       |

By setting the following mapping:

```
{
	"id": ["customer_id"],
	"customer": {
		"name": ["customer_name"],
		"phone": ["customer_phone"]
	},
	"orders": {
		"id": ["order_id"],
		"product_id": ["product_id"],
		"amount": ["amount"],
		"description": ["order_description"]
	}
}
```

It will create target model will look like this:

| field                | type         |
| -------------------- | ------------ |
| id                   | Int          |
| customer             | Record       |
| -- name              | String       |
| -- phone             | String       |
| orders               | Record       |
| -- id                | Int          |
| -- product_id        | Int          |
| -- amount            | Double       |
| -- description       | String       |
