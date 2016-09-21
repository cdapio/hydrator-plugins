# Logistic Regression Trainer


Description
-----------
Trains a classification model based upon a particular label and features of a record. Saves this model to a FileSet.

Use Case
--------
This sink can be used when you have sample data that you want to use to build a Logistic Regression model,
which can be used for classification later on.

Properties
----------
**fileSetName:** The name of the FileSet to save the model to.

**path:** Path of the FileSet to save the model to.

**featureFieldsToInclude:** A comma-separated sequence of fields that needs to be used for training.

**featureFieldsToExclude:** A comma-separated sequence of fields that needs to be excluded from being used in training.

**labelField:** The field from which to get the prediction. It must be of type double.

**numFeatures:** The number of features to train the model with. This should be the same as the number of features
used for the LogisticRegressionClassifier. The default value if none is provided will be 100.

**numClasses:** The number of classes to use in training the model. It must be of type integer. Default is 2.

Both *featureFieldsToInclude* and *featureFieldsToExclude* fields cannot be specified simultaneously.
If inputs for *featureFieldsToInclude* and *featureFieldsToExclude* has not been provided then all the fields except
label field will be used as feature fields.


Example
-------
This example uses the ``read`` and ``impMsg`` fields from an input record to use as the features and the ``isSpam`` field to use
as the label to train the model.

    {
        "name": "LogisticRegressionTrainer",
        "type": "sparksink",
        "properties": {
            "fileSetName": "modelFileSet",
            "path": "output",
            "featureFieldsToInclude": "read,impMsg",
            "labelField": "isSpam",
            "numFeatures": "100",
            "numClasses": "2"
        }
    }
