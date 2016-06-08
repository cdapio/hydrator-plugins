# JSON Parser Transform

## Description

Parses input Json event into a record. The input Json event could be either a map of string field to values or it could 
be a complex nested Json structure. Plugin allows one to express Json paths for extracting fields from complex nested 
input Json.

## Parsing Simple Json

Simple Json (which is simply defined as a mapping from key to value) parsing is achieved by specifying the 
just the output schema fields. The field name in the output schema should be the same as the key in the input Json. 
The type of the field should also be the same as the input value. No implicit conversions are perform on the 
Json values.

Following is an example of event that is mapped to the output schema:
<pre>
  {
     "id" : 1000,
     "first" : "Joltie",
     "last" : "Neutrino",
     "email_id" : "joltie.neutrino@gmail.com",
     "address" : "666, Mars Street",
     "city" : "Marshfield",
     "state" : "MR",
     "country" : "Marshyland",
     "zip" : 34553423,
     "planet" : "Mars"
  }
</pre>

So, when output schema is specified it should be specified as follows

| Field | Type   | Null |
|-------|--------|------|
| id    | int    | |
| first | string | |
| last  | string | |
| email_id | string | |
| address | string | |
| city | string | | 
| state | string | | 
| country | string | | 
| zip | long | | 

> Note that the field "planet" has been excluded from the output schema meaning that the field would be ignored and 
not processed when the Json event is mapped. 

## Nested Json

Parser also allows extracting fields from a complex nested Json. In order to extract fields it uses Json paths similar
to XPath expression for XML. To extract fields using expression in Json, this plugin uses 'JsonPath' library. Plugin
allows one to define a mapping from output field name to Json path expression that should be applied on input to extract
value from the Json event. For example, let's say you have a Json that is as follows:

<pre>
  {
    "employee" : {
      "name" : {
        "first" : "Joltie",
        "last" : "Neutrino"
      },
      "email" : "joltie.neutrino@gmail.com",
      "address" :  {
        "street1" : "666, Mars Street",
        "street2" : "",
        "apt" : "",
        "city" : "Marshfield",
        "state" : "MR",
        "zip" : 34553423,
        "country" : "Marshyland"
      }
    }
  }
</pre>

then one can specify the mapping for extracting following fields from the input Json event
 
1. first, 
2. last, 
3. email, 
4. street1, 
5. city, 
6. state, &
7. zip

Then the mappings in the plugin will look as follows:

| Output Field Name | Input Json Path Expression |
|-------------------|----------------------------|
| first             | $.employee.name.first |
| last              | $.employee.name.last |
| email             | $.employee.email |
| street            | $.employee.address.street1 |
| city              | $.employee.address.city |
| state             | $.employee.address.state |
| zip               | $.employee.address.zip |
| country           | $.employee.address.country |

### Expression

The "root member object" for parsing any Json is referred to as ```$``` regardless of whether it's an array or an 
object. It also uses dot notation or bracket notation for defining the levels of parsing. 
For example: ```$.employee.name``` or ```$[employee][name]```

#### Supported Operators

| Operator | Description |
|----------|-------------|
| ```$``` | The root element of the query. |
| ```*``` | Wildcard |
| ```..``` | Deep scan |
| ```.<name>```| dot notation representing child |
| ```[?(<expression>)]``` | Filter expression, should be boolean result always |

#### Supported Functions

The functions perform aggregations at the tail end of the path. The functions take output of the expression as 
input for performing aggregations. The aggregation function can be only be applied where the expression results in 
an array. 
 
| Function | Type | Desription |
|----------|------|------------|
| min      | double | min value of array of numbers |
| max      | double | max value of array of numbers |
| avg      | double | average value of array of numbers |
| stddev   | double | standard deviation value of array of numbers |
| length   | int    | length of the array |

Configuration
-------------
| Config | Description |
|--------|-------------|
|**field** | Specifies the input field that should be parsed as a CSV Record.|
|**mapping** | Mapping specifying output field name to input Json path for extracting the field.|
|**schema** | Specifies the output schema for the JSON Record.|


