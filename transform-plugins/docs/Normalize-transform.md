# Normalize Transform


Description
-----------
Normalize is a transform plugin that breaks one source row into multiple target rows.
Example: Attributes are stored in columns of table or file, may need to be broken into multiple records such as one
record per column attribute.
In general it allows one to convert columns to rows.

Use Case
--------
User want to reduce the restructuring of dataset when new type of data introduced in the collection.
Let’s assume that we are building a customer 360 Master table that aggregates data for a user from multiple sources.
Each of the source has it's own type of data to be added to a Customer Id. Instead of creating wide columns,
normalization allows one to transform into it's canonical form and update the customer 360 profile simultaneously
from multiple sources.


Properties
----------
**fieldMapping:** Specify the input schema field mapping to output schema field.
Example: CustomerID:ID, here value of CustomerID will be saved to ID field of output schema.

**fieldNormalizing:** Specify the normalize field name, to what output field it should be mapped to and where the value
needs to be added.
Example: ItemId:AttributeType:AttributeValue, here ItemId column name will be saved to AttributeType field and its
value will be saved to AttributeValue field of output schema.


Example
-------
This example creates customer 360 profile data from multiple sources.
Assume we have a source 'Customer Profile' table and 'Customer Purchase' table, which will be normalize to
Customer-360 table.

Customer Profile table:

    +==============================================================================================================+
    | CustomerId | First Name |  Last Name | Shipping Address | Credit Card | Billing Address | Last Update Date   |
    +==============================================================================================================+
    | S23424242  | Joltie     |  Root      | 32826 Mars Way,  | 2334-232132 | 32826 Mars Way, | 05/12/2015         |
    |            |            |            | Marsville,  MR,  | -2323       | Marsville,  MR, |                    |
    |            |            |            | 24344            |             | 24344           |                    |
    +--------------------------------------------------------------------------------------------------------------+
    | R45764646  | Root       | Joltie     | 32423, Your Way, | 2343-12312- | 32421 MyVilla,  | 04/03/2012         |
    |            |            |            | YourVille, YR,   | 12313       | YourVille, YR,  |                    |
    |            |            |            | 65765            |             | 23423           |                    |
    +==============================================================================================================+

Map "CustomerId" column to "ID" column of output schema, and "Last Update Date" to "Date" column of output schema.
Normalize "First Name", "Last Name", "Credit Card" and "Billing Address" columns where each column name mapped to
"Attribute Type" and value mapped to "Attribute Value" columns of the output schema.

The plugin JSON Representation will be:

    {
        "name": "Normalize",
        "plugin": {
            "name": "Normalize",
            "type": "transform",
            "label": "Normalize",
            "properties": {
               "fieldMapping": "CustomerId:ID,Last Update Date:Date",
               "fieldNormalizing": "First Name:Attribute Type:Attribute Value,Last Name:Attribute Type:Attribute Value,
                                   Credit Card:Attribute Type:Attribute Value,
                                   Billing Address:Attribute Type:Attribute Value"
            }
        }
    }


After transformation of Normalize plugin, the output records in Customer-360 table will be:

    +====================================================================================+
    | ID	      | Attribute Type	| Attribute Value	                       | Date        |
    +====================================================================================+
    | S23424242	| First Name	    | Joltie	                               | 05/12/2015  |
    | S23424242	| Last Name	      | Root	                                 | 05/12/2015  |
    | S23424242	| Credit Card	    | 2334-232132-2323	                     | 05/12/2015  |
    | S23424242	| Billing Address	| 32826 Mars Way, Marsville,  MR, 24344	 | 05/12/2015  |
    | R45764646	| First Name	    | Root                                   | 04/03/2012  |
    | R45764646	| Last Name	      | Joltie                                 | 04/03/2012  |
    | R45764646	| Credit Card	    | 2343-12312-12313	                     | 04/03/2012  |
    | R45764646	| Billing Address	| 32421, MyVilla Ct, YourVille, YR, 23423| 04/03/2012  |
    +====================================================================================+

Create a new pipeline to normalize Customer profile table to Customer-360 table.

Customer Purchase table:

    +===========================================================+
    | CustomerId | Item ID	        | Item Cost	| Purchase Date |
    +===========================================================+
    | S23424242  | UR-AR-243123-ST	| 245.67	  | 08/09/2015    |
    | S23424242  | SKU-234294242942	| 67.90 	  | 10/12/2015    |
    | R45764646  | SKU-567757543532	| 14.15 	  | 06/09/2014    |
    +===========================================================+

Map "CustomerId" column to "ID" column of output schema, and "Purchase Date" to "Date" column of output schema.
Normalize "Item ID", "Item Cost" columns where each column name mapped to "Attribute Type" and value mapped to
"Attribute Value" columns of the output schema.

The plugin JSON Representation will be:

    {
        "name": "Normalize",
        "plugin": {
            "name": "Normalize",
            "type": "transform",
            "label": "Normalize",
            "properties": {
               "fieldMapping": "CustomerId:ID,Purchase Date:Date",
               "fieldNormalizing": "Item ID:Attribute Type:Attribute Value,Item Cost:Attribute Type:Attribute Value"
            }
        }
    }

After transformation of Normalize plugin, the output records in Customer-360 table will be:

    +====================================================================================+
    | ID	      | Attribute Type	| Attribute Value	                       | Date        |
    +====================================================================================+
    | S23424242	| First Name	    | Joltie	                               | 05/12/2015  |
    | S23424242	| Last Name	      | Root	                                 | 05/12/2015  |
    | S23424242	| Credit Card	    | 2334-232132-2323	                     | 05/12/2015  |
    | S23424242	| Billing Address	| 32826 Mars Way, Marsville,  MR, 24344	 | 05/12/2015  |
    | R45764646	| First Name	    | Root                                   | 04/03/2012  |
    | R45764646	| Last Name	      | Joltie                                 | 04/03/2012  |
    | R45764646	| Credit Card	    | 2343-12312-12313	                     | 04/03/2012  |
    | R45764646	| Billing Address | 32421, MyVilla Ct, YourVille, YR, 23423| 08/09/2015  |
    | S23424242	| Item ID	        | UR-AR-243123-ST                        | 08/09/2015  |
    | S23424242	| Item Cost     	| 245.67                                 | 08/09/2015  |
    | S23424242	| Item ID        	| SKU-234294242942                       | 10/12/2015  |
    | S23424242	| Item Cost	      | 67.90                                  | 10/12/2015  |
    | R45764646	| Item ID       	| SKU-567757543532                       | 06/09/2014  |
    | R45764646	| Item Cost       | 14.15                                  | 06/09/2014  |
    +====================================================================================+

Above Customer-360 table has normalized data from 'Customer Profile' and 'Customer Purchase' tables.