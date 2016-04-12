# URL Fetch Real-time Source

This is a real-time source that will fetch data from a specified URL at a given interval and pass the results to the
next plugin.

The plugin expects only two configuration parameters:
* URL - This is the URL that the source will perform a GET request on
* Delay - The amount of time in seconds between each call to the URL

The plugin outputs the following schema to the next plugin:
* ts - The unix timestamp of the time the request was made
* url - The URL that was requested. This is useful for filtering.
* headers - This is a map of header names to values. In the case of multiple values for the same header name, the results are joined with a comma.
* body - The raw response from the request as a UTF-8 string.

