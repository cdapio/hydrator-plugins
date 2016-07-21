# Kafka Streaming Source


Description
-----------
Kafka streaming source. Emits a record with the schema specified by the user. If no schema
is specified, it will emit a record with two fields: 'key' (nullable string) and 'message'
(bytes).


Use Case
--------
This source is used whenever you want to read from Kafka. For example, you may want to read messages
from Kafka and write them to a Table.


Properties
----------

**brokers:** Comma-separated list of Kafka brokers specified in host1:port1,host2:port2 form.

**topics:** Comma-separated list of Kafka topics to read from.

**schema:** Optional schema for the body of Kafka events.
The schema is used in conjunction with the format to parse Kafka payloads.
Some formats (such as the 'avro' format) require schema while others do not.
The schema given is for the body of the Kafka event.

**format:** Optional format of the Kafka event. Any format supported by CDAP is supported.
For example, a value of 'csv' will attempt to parse Kafka payloads as comma-separated values.
If no format is given, Kafka message payloads will be treated as bytes, resulting in a two-field schema:
'key' of type string (which is nullable) and 'payload' of type bytes.


Example
-------
This example reads from ten partitions of the 'purchases' topic of a Kafka instance running
on brokers host1.example.com:9092 and host2.example.com:9092. It then
parses Kafka messages using the 'csv' format into records with the specified schema:

    {
        "name": "Kafka",
        "type": "realtimesource",
        "properties": {
            "topics": "purchases",
            "brokers": "host1.example.com:9092,host2.example.com:9092",
            "format": "csv",
            "schema": "{
                \"type\":\"record\",
                \"name\":\"purchase\",
                \"fields\":[
                    {\"name\":\"user\",\"type\":\"string\"},
                    {\"name\":\"item\",\"type\":\"string\"},
                    {\"name\":\"count\",\"type\":\"int\"},
                    {\"name\":\"price\",\"type\":\"double\"}
                ]
            }"
        }
    }

For each Kafka message read, it will output a record with the schema:

    +================================+
    | field name  | type             |
    +================================+
    | user        | string           |
    | item        | string           |
    | count       | int              |
    | price       | double           |
    +================================+
