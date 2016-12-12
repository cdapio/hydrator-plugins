# Tweet Post Action


Description
-----------
Posts a Tweet at the end of a pipeline run.


Use Case
--------
This action can be used when you want to send an email at the end of a pipeline run.
For example, you may want to configure a pipeline so that an email is sent whenever
the run failed for any reason.


Properties
----------
**runCondition:**" When to run the action. Must be 'completion', 'success', or 'failure'. Defaults to 'completion'.
If set to 'completion', the action will be executed regardless of whether the pipeline run succeeded or failed.
If set to 'success', the action will only be executed if the pipeline run succeeded.
If set to 'failure', the action will only be executed if the pipeline run failed.

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

**Tweet:** The text to post in the Tweet.

**Namespace:** The namespace the pipeline will be run in.

**PipelineName:** The name of the pipeline.

**GeckoDriverPath:** The path to the Gecko driver used by Selenium.

Example
-------
This example sends an email from 'team-ops@example.com' to 'team-alerts@example.com' whenever a run fails:

    {
        "name": "Tweet",
        "type": "postaction",
        "properties": {
            "consumerKey": "XXXX",
            "consumerSecret": "XXXX",
            "accessToken": "XXXX",
            "accessTokenSecret": "XXXX",
            "tweet": "Just finished a #BigData pipeline with #CaskHydrator",
            "runCondition": "failure"
        }
    }
