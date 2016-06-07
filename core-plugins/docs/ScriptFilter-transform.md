# Script Filter Transform


Description
-----------
A transform plugin that filters records using a custom JavaScript provided in the plugin's config.


Use Case
--------
The transform is used when you need to filter records. For example, you may want to filter
out records that have null values for an important field.


Properties
----------
**script:** JavaScript that must implement a function ``'shouldFilter'``, takes a
JSON object (representing the input record) and a context object (which encapsulates CDAP metrics and logger),
and returns true if the input record should be filtered and false if not.

**lookup:** The configuration of the lookup tables to be used in your script.
For example, if lookup table "purchases" is configured, then you will be able to perform
operations with that lookup table in your script: ``context.getLookup('purchases').lookup('key')``
Currently supports ``KeyValueTable``.


Example
-------
This example filters out any records whose ``'count'`` field contains a value greater than 100:

    {
        "name": "ScriptFilter",
        "type": "transform",
        "properties": {
            "script": "function shouldFilter(input, context) {
                if (input.count < 0) {
                    context.getLogger().info("Got input record with negative count");
                    context.getMetrics().count("negative.count", 1);
                }
                return input.count > 100;
            }",
            "lookup": "{
                \"tables\":{
                    \"purchases\":{
                        \"type\":\"DATASET\",
                        \"datasetProperties\":{
                            \"dataset_argument1\":\"foo\",
                            \"dataset_argument2\":\"bar\"
                        }
                    }
                }
            }"
        }
    }

**Note:** This transform will emit a metric named ``filtered`` that records how many records it filtered out.
