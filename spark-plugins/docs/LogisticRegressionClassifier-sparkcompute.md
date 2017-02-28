# LogisticRegressionClassifier


Description
-----------
Loads a Logistic Regression model from a file of a FileSet dataset and uses it to classify records.

Use Case
--------
This transform can be used when you have a saved Logistic Regression model and want to classify data.

Properties
----------
**fileSetName:** The name of the FileSet to load the model from.

**path:** Path of the FileSet to load the model from.

**fieldsToClassify:** A space-separated sequence of fields to use for classification.

**predictionField:** The field on which prediction needs to be set. It will be of type double.

**numFeatures:** The number of features to use when classifying with the trained model. This should be the same as
the number of features used to train the model in LogisticRegressionTrainer. The default value if none is provided
will be 100.


Example
-------
This example uses the ``text`` and ``imp`` fields of a record to use for classification and sets the prediction
on to the ``isSpam`` field.

    {
        "name": "LogisticRegressionClassifier",
        "type": "sparkcompute",
        "properties": {
            "fileSetName": "modelFileSet",
            "path": "output",
            "fieldToClassify": "text,imp",
            "predictionField": "isSpam",
            "numFeatures": "100"
        }
    }


For example, suppose the classifier receives input records where each record represents an SMS message:

    +=========================================================================+
    | sender | receiver | text                                      | imp     |
    +=========================================================================+
    | john   | jane     | how are you doing                         | yes     |
    | john   | alice    | did you get my email                      | yes     |
    | alice  | jane     | you have won the lottery                  | no      |
    | alice  | bob      | you could be entitled to debt forgiveness | no      |
    | bob    | john     | I'll be late today                        | yes     |
    | bob    | bob      | sorry I couldn't make it                  | yes     |
    +=========================================================================+

Output records will contain all fields in addition to a field for the prediction:

    +====================================================================================+
    | sender | receiver | text                                      | imp     | isSpam   |
    +====================================================================================+
    | john   | jane     | how are you doing                         | yes     | 0.0      |
    | john   | alice    | did you get my email                      | yes     | 0.0      |
    | alice  | jane     | you have won the lottery                  | no      | 1.0      |
    | alice  | bob      | you could be entitled to debt forgiveness | no      | 1.0      |
    | bob    | john     | I'll be late today                        | yes     | 0.0      |
    | bob    | bob      | sorry I couldn't make it                  | yes     | 0.0      |
    +====================================================================================+

