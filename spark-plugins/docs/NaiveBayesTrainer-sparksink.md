# NaiveBayesTrainer


Description
-----------
Using a Naive Bayes algorithm, trains a model based upon a particular label and text field of a record.
Saves this model to a file in a FileSet dataset.

Use Case
--------
This sink can be used when you have sample data that you want to use to build a Naive Bayes model, which
can be used for classification later on.

Properties
----------
**fileSetName:** The name of the FileSet to save the model to.

**path:** Path of the FileSet to save the model to.

**fieldToClassify:** A space-separated sequence of words to use for training.

**predictionField:** The field from which to get the prediction. It must be of type double.

**numFeatures:** The number of features to train the model with. This should be the same as the number of features
used for the NaiveBayesClassifier. The default value if none is provided will be 100.


Example
-------
This example uses the ``text`` field of a record to use as the features and the ``isSpam`` field to use
 as the label to train the model.

    {
        "name": "NaiveBayesTrainer",
        "type": "sparksink",
        "properties": {
            "fileSetName": "modelFileSet",
            "path": "output",
            "fieldToClassify": "text",
            "predictionField": "isSpam",
            "numFeatures": "100"
        }
    }
