# XML to JSON Converter Transform


Description
-----------
Accepts a field that contains a properly formatted XML string and 
outputs a stringified JSON version of that string. This is meant to 
be used in conjunction with the Javascript transform for parsing of 
complex XML documents into parts. Once the XML is in a JSON string, you
can convert it into a Javascript object using:

``var jsonObj = JSON.parse(input.jsonBody);``


Configuration
-------------
**field:** Specifies the input field containing an XML string to be 
converted to a JSON string.
