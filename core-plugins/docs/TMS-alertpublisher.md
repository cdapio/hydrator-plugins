# TMS Alert Publisher


Description
-----------
Publishes alerts to the CDAP Transactional Messaging System (TMS) as json objects. The plugin
allows you to specify the topic and namespace to publish to, as well as a rate limit for the
maximum number of alerts to publish per second.


Properties
----------
**topic:** The topic to publish alerts to. (Macro-enabled)

**namespace:** The namespace of the topic to publish alerts to.
If none is specified, defaults to the namespace of the pipeline. (Macro-enabled)

**autoCreateTopic:** Whether to create the topic in the pipeline namespace if the topic does not already exist.
Cannot be set to true if namespace is set. Defaults to false.

**maxAlertsPerSecond:** The maximum number of alerts to publish per second. Defaults to 100.
