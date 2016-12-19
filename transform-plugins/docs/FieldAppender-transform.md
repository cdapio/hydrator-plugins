# Field Appender Transform


Description
-----------
Adds the specified fields to the record as it passes through.


Configuration
-------------
**fieldsToAppend:** the list of fields to append to the record. (Macro-Enabled)

fieldsToAppend supports the following "mini-macros":

**~uuid~** generates a uuid based on the time-based UUID generation algorithm described in http://www.ietf.org/rfc/rfc4122.txt

**~datetime:yyyy-MM-dd~** generates the timestamp in the format specified. Any Java SimpleDateFormat should work.

**~field:field1~** substitutes the value of the specified field as a string, in this case, field1. If the field is
               not found, the text "null" will be substituted.