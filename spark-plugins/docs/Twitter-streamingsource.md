<!--- 
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 -->
# Twitter Streaming Source


Description
-----------
Samples tweets in real-time through Spark streaming. Output records will have this schema:

| field name  | type             |
| ----------- | ---------------- |
| id          | long             |
| message     | string           |
| lang        | nullable string  |
| time        | nullable long    |
| favCount    | int              |
| rtCount     | int              |
| source      | nullable string  |
| geoLat      | nullable double  |
| geoLong     | nullable double  |
| isRetweet   | boolean          |


Use Case
--------
The source is used whenever you want to sample tweets from Twitter in real-time using Spark streaming.
For example, you may want to read tweets and store them in a table where they can
be accessed by your data scientists to perform experiments.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

See the [Twitter OAuth documentation] for more information on obtaining
your access token and access token secret. The consumer key and secret
are specific to your Twitter app. Login, view [your apps], then click on
the relevant app to find the consumer key and secret.

  [Twitter OAuth documentation]: https://dev.twitter.com/oauth/overview
  [your apps]: https://apps.twitter.com/

**ConsumerKey:** Twitter Consumer Key.

**ConsumerSecret:** Twitter Consumer Secret.

**AccessToken:** Twitter Access Token.

**AccessTokenSecret:** Twitter Access Token Secret.


Example
-------

```json
{
    "name": "Twitter",
    "type": "streamingsource",
    "properties": {
        "AccessToken": "GetAccessTokenFromTwitter",
        "AccessTokenSecret": "GetAccessTokenSecretFromTwitter",
        "ConsumerKey": "GetConsumerKeyFromTwitter",
        "ConsumerSecret": "GetConsumerSecretFromTwitter"
    }
}
```
