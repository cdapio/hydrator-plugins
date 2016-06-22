# XML Parser Transform

Description
-----------
The XML Parser uses XPath to extract field from a complex XML Event. This should generally be used in conjunction with
the XML Reader Batch Source. The XML Source Reader will provide individual events to the XML Parser and the XML Parser
will be responsible for extracting fields from the events and mapping them to output schema.


Use Case
--------
The transform takes input record that contain xml events/records, parses it using the xpath specified and returns a
structured record according to the schema specified by user.
For example, this plugin can be used in conjuction with XML Reader Batch Source to extract values from XMLNews documents
and to create structured records which will be easier to query.


Properties
----------

**input:** Specifies the field in input that should be considered as source of XML event or record.

**encoding:** Specifies XML encoding type(default is UTF-8).

**xpathMappings:** Specifies a mapping from XPath of XML record to field name.

**fieldTypeMapping:** Specifies the field name as specified in xpathMappings and its corresponding data type.
The data type can be of following types- boolean, int, long, float, double, bytes, string

**processOnError:** Specifies what happens in case of error.
                     1. Ignore the error record
                     2. Stop processing upon encoutering error
                     3. Write error record to different dataset

Example
-------

This example parses the xml record received in "body" field of the input record, according to the
xpathMappings specified, for each field name.
The type output schema will be created, using the type specified for each field in "fieldTypeMapping".

       {
            "name": "XMLParser",
            "plugin": {
                "name": "XMLParser",
                "type": "transform",
                "label": "XMLParser",
                "properties": {
                    "encoding": "UTF-8",
                     "processOnError": "Ignore error and continue",
                      "xpathMappings": "category://book/@category,
                                        title://book/title,
                                        year:/bookstore/book[price>35.00]/year,
                                        price:/bookstore/book[price>35.00]/price,
                                        subcategory://book/subcategory",
                      "fieldTypeMapping": "category:string,title:string,year:int,price:double,subcategory:string",
                      "input": "body"
                }
            }
       }

For example, suppose the transform recieves the input record:

    +=========================================================================================================+
    | offset   | body                                                                                         |
    +=========================================================================================================+
    | 1        | <bookstore><book category="cooking"><subcategory><type>Continental</type></subcategory>      |
    |          | <title lang=\"en\">Everyday Italian</title><author>Giada De Laurentiis</author><year>2005    |
    |          | </year><price>30.00</price></book></bookstore>                                               |
    | 2        | <bookstore><book category="children"><subcategory><type>Series</type></subcategory>          |
    |          | <title lang=\"en\">Harry Potter</title><author>J K. Rowling</author><year>2005</year><price> |
    |          | 49.99</price></book></bookstore>                                                             |
    +=========================================================================================================+

Output record will contain all the output fields specified by user:

    +=========================================================================================================+
    | category  | title              | year   |  price  | subcategory                                         |
    +=========================================================================================================+
    | cooking   | Everyday Italian   | null   |  null   | <subcategory><type>Continental</type></subcategory> |
    | children  | Harry Potter       | 2005   | 49.99   | <subcategory><type>Series</type></subcategory>      |
    +=========================================================================================================+
