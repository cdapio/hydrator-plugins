# XML Multi Parser Transform

Description
-----------
The XML Multi Parser Transform uses XPath to extract fields from an XML document. It will generate records from
the children of the element specified by the XPath. If there is some error parsing the document or building the record,
the problematic input record will be dropped.


Use Case
--------
You might want to use this transform if your input record contains a field that is in xml format and you want to
parse multiple records from that field. For example, it may contain the contents of an RSS feed, and you want to parse
out the items contained in that feed.


Properties
----------

**field:** The field in the input record that contains the XML document. (Macro-enabled)

**encoding:** The character set encoding of the XML document. Defaults to UTF-8. (Macro-enabled)

**xPath:** The XPath to the element(s) from which to parse out records. Output records will be generated from the
children of the elements referenced by the XPath. For example: /rss/channel/item.

**schema:** The schema of records to output. Each field in the schema must be a child of the XML element referenced by
the XPath. Currently only simply types are supported.

Conditions
----------
If error dataset is configured, then all the erroneous rows, if present in the input, will be committed to the
specified error dataset.
If no error dataset is configured, then pipeline will get completed but with warnings in the logs.

Example
-------

This example parses an XML record received in the "body" field of the input record. It specifies an
xpath of '/rss/channel/item', which means it will generate a record for each item node that matches that XPath.
It generates output records with guid, title, and pubDate fields, which are taken from those child
elements of each item node in the xml.

        {
            "name": "XMLMultiParser",
            "plugin": {
                "name": "XMLMultiParser",
                "type": "transform",
                "properties": {
                    "input": "body",
                    "xPath": "/rss/channel/item",
                    "schema": "{
                        \"type\":\"record\",
                        \"name\":\"record\",
                        \"fields\":[
                            {\"name\":\"guid\",\"type\":\"string\"},
                            {\"name\":\"title\",\"type\":\"string\"},
                            {\"name\":\"pubDate\",\"type\":\"string\"}
                        ]
                    }"
                }
            }
        }

For example, for xml document:

        <rss>
            <channel>
                <item>
                    <guid>id123</guid>
                    <title>Something Happened in the World</name>
                    <pubDate>1970-01-01 12:00:00</pubDate>
                </item>
                <item>
                    <guid>id456</guid>
                    <title>Some Other Thing Happened in the World</name>
                    <pubDate>1970-01-01 13:00:00</pubDate>
                </item>
            </channel>
        </rss>

The transform will output records:

    +======================================================================+
    | guid  | title                                  | pubDate             |
    +======================================================================+
    | id123 | Something Happened in the World        | 1970-01-01 12:00:00 |
    | id456 | Some Other Thing Happened in the World | 1970-01-01 13:00:00 |
    +======================================================================+
