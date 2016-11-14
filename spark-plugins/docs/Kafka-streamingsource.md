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
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**brokers:** List of Kafka brokers specified in host1:port1,host2:port2 form. (Macro-enabled)

**topic:** The Kafka topic to read from. (Macro-enabled)

**partitions:** List of topic partitions to read from. If not specified, all partitions will be read. (Macro-enabled)

**defaultInitialOffset:** The default initial offset for all topic partitions.
An offset of -2 means the smallest offset. An offset of -1 means the latest offset. Defaults to -1.
Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read.
If you wish to set different initial offsets for different partitions, use the initialPartitionOffsets property. (Macro-enabled)

**initialPartitionOffsets:** The initial offset for each topic partition. If this is not specified,
all partitions will use the same initial offset, which is determined by the defaultInitialOffset property.
Any partitions specified in the partitions property, but not in this property will use the defaultInitialOffset.
An offset of -2 means the smallest offset. An offset of -1 means the latest offset.
Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read. (Macro-enabled)

**schema:** Output schema of the source. If you would like the output records to contain a field with the
Kafka message key, the schema must include a field of type bytes or nullable bytes, and you must set the
keyField property to that field's name. Similarly, if you would like the output records to contain a field with
the timestamp of when the record was read, the schema must include a field of type long or nullable long, and you
must set the timeField property to that field's name. Any field that is not the timeField or keyField will be used
in conjuction with the format to parse Kafka message payloads.

**format:** Optional format of the Kafka event message. Any format supported by CDAP is supported.
For example, a value of 'csv' will attempt to parse Kafka payloads as comma-separated values.
If no format is given, Kafka message payloads will be treated as bytes.

**timeField:** Optional name of the field containing the read time of the batch.
If this is not set, no time field will be added to output records.
If set, this field must be present in the schema property and must be a long.

**keyField:** Optional name of the field containing the message key.
If this is not set, no key field will be added to output records.
If set, this field must be present in the schema property and must be bytes.

**partitionField:** Optional name of the field containing the partition the message was read from.
If this is not set, no partition field will be added to output records.
If set, this field must be present in the schema property and must be an int.

**offsetField:** Optional name of the field containing the partition offset the message was read from.
If this is not set, no offset field will be added to output records.
If set, this field must be present in the schema property and must be a long.


Example
-------
This example reads from the 'purchases' topic of a Kafka instance running
on brokers host1.example.com:9092 and host2.example.com:9092. The source will add
a time field named 'readTime' that contains a timestamp corresponding to the micro
batch when the record was read. It will also contain a field named 'key' which will have
the message key in it. It parses the Kafka messages using the 'csv' format
with 'user', 'item', 'count', and 'price' as the message schema.

    {
        "name": "Kafka",
        "type": "streamingsource",
        "properties": {
            "topics": "purchases",
            "brokers": "host1.example.com:9092,host2.example.com:9092",
            "format": "csv",
            "timeField": "readTime",
            "keyField": "key",
            "schema": "{
                \"type\":\"record\",
                \"name\":\"purchase\",
                \"fields\":[
                    {\"name\":\"readTime\",\"type\":\"long\"},
                    {\"name\":\"key\",\"type\":\"bytes\"},
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
    | readTime    | long             |
    | key         | bytes            |
    | user        | string           |
    | item        | string           |
    | count       | int              |
    | price       | double           |
    +================================+

Note that the readTime field is not derived from the Kafka message, but from the time that the
message was read.
