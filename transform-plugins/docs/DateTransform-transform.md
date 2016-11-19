# Date Transform

This transform takes a date in either a unix timestamp or a string, and converts it to a formatted string.

**sourcField** The field (or fields separated by commas) that contain the date to format.

**sourceFormat** The SimpleDateFormat of the source fields. If the source fields are longs, this can be omitted.

**secondsOrMilliseconds** If the source fields are longs, you can indicate if they are in seconds or milliseconds.

**targetField** The field (or fields separated by commas) that contain the date to write to in the output schema.

**targetFormat** The SimpleDateFormat of the output field.