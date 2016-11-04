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

**featureFieldsToInclude:** A comma-separated sequence of fields that needs to be used for training.

**featureFieldsToExclude:** A comma-separated sequence of fields that needs to be excluded from being used in training.

**predictionField:** The field on which prediction needs to be set. It will be of type double.

**numFeatures:** The number of features to use when classifying with the trained model. This should be the same as
the number of features used to train the model in LogisticRegressionTrainer. Default is 100.

Both *featureFieldsToInclude* and *featureFieldsToExclude* fields cannot be specified simultaneously.
If inputs for *featureFieldsToInclude* and *featureFieldsToExclude* has not been provided then all the fields except
predictionField field will be used as feature fields.


Example
-------
This example uses the ``read`` and ``imp`` fields from an input record to use for classification and sets the prediction
on to the ``isSpam`` field.

    {
        "name": "LogisticRegressionClassifier",
        "type": "sparkcompute",
        "properties": {
            "fileSetName": "modelFileSet",
            "path": "output",
            "featureFieldsToInclude": "read,imp",
            "predictionField": "isSpam",
            "numFeatures": "100"
        }
    }


For example, suppose the classifier receives input records where each record represents a part of an SMS message:

    +==========================================+
    | sender | receiver | read       | imp     |
    +==========================================+
    | john   | jane     | 1          | 1       |
    | john   | alice    | 1          | 1       |
    | alice  | jane     | 0          | 0       |
    | alice  | bob      | 0          | 0       |
    | bob    | john     | 1          | 1       |
    | bob    | bob      | 1          | 1       |
    +==========================================+

Output records will contain all fields in addition to a field for the prediction:

    +=====================================================+
    | sender | receiver | read       | imp     | isSpam   |
    +=====================================================+
    | john   | jane     | 1          | 1       | 0.0      |
    | john   | alice    | 1          | 1       | 0.0      |
    | alice  | jane     | 0          | 0       | 1.0      |
    | alice  | bob      | 0          | 0       | 1.0      |
    | bob    | john     | 1          | 1       | 0.0      |
    | bob    | bob      | 1          | 1       | 0.0      |
    +=====================================================+

