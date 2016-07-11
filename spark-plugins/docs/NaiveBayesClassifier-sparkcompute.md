# NaiveBayesClassifier


Description
-----------
Loads a NaiveBayes model from a file of a FileSet dataset and uses it to classify records.

Use Case
--------
This transform can be used when you have a saved NaiveBayes model and want to classify data.

Properties
----------
**fileSetName:** The name of the FileSet to load the model from.

**path:** Path of the FileSet to load the model from.

**fieldToClassify:** A space-separated sequence of words to classify.

**predictionField:** The field on which to set the prediction. It will be of type double.

**numFeatures:** The number of features to use when classifying with the trained model. This should be the same as
the number of features used to train the model in NaiveBayesTrainer. The default value if none is provided will be 100.


Example
-------
This example uses the ``text`` field of a record to use for classification and sets the prediction
on to the ``isSpam`` field.

    {
        "name": "NaiveBayesClassifier",
        "type": "sparkcompute",
        "properties": {
            "fileSetName": "modelFileSet",
            "path": "output",
            "fieldToClassify": "text",
            "predictionField": "isSpam",
            "numFeatures": "100"
        }
    }


For example, suppose the classifier receives input records where each record represents an SMS message:

    +===============================================================+
    | sender | receiver | text                                      |
    +===============================================================+
    | john   | jane     | how are you doing                         |
    | john   | alice    | did you get my email                      |
    | alice  | jane     | you have won the lottery                  |
    | alice  | bob      | you could be entitled to debt forgiveness |
    | bob    | john     | I'll be late today                        |
    | bob    | bob      | sorry I couldn't make it                  |
    +===============================================================+

Output records will contain all fields in addition to a field for the prediction:

    +=========================================================================+
    | sender | receiver | text                                      | isSpam  |
    +=========================================================================+
    | john   | jane     | how are you doing                         | 0.0     |
    | john   | alice    | did you get my email                      | 0.0     |
    | alice  | jane     | you have won the lottery                  | 1.0     |
    | alice  | bob      | you could be entitled to debt forgiveness | 1.0     |
    | bob    | john     | I'll be late today                        | 0.0     |
    | bob    | bob      | sorry I couldn't make it                  | 0.0     |
    +=========================================================================+

