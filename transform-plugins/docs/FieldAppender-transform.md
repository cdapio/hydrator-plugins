# Field Appender Transform


Description
-----------
Adds the specified fields to the record as it passes through.


Configuration
-------------
**fieldsToAppend:** The list of fields to append to the record. (Macro-Enabled)

fieldsToAppend supports the following "mini-macro":

**~field:field1~** Substitutes the value of the specified field as a string, in this case, field1. If the field is
               not found, the text "null" will be substituted.