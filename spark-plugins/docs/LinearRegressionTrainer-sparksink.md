# LinearRegressionTrainer


Description
-----------
Trains a regression model based upon a particular label and features of a record. Saves this model to a FileSet.

Use Case
--------
This sink can be used when you have a sample data and you want to use it to build a Linear Regression model.

Properties
----------
**fileSetName:** The name of the FileSet to save the model to.

**path:** Path of the FileSet to save the model to.

**featuresToInclude:** A comma-separated sequence of fields to use for training. If empty, all fields except the label
will be used for training. Features to be used, must be from one of the following types: int, long, float or double.
Both *featuresToInclude* and *featuresToExclude* fields cannot be specified.

**featuresToExclude:** A comma-separated sequence of fields to excluded when training. If empty, all fields except the
label will be used for training.  Both *featuresToInclude* and *featuresToExclude* fields cannot be specified.

**labelField:** The field from which to get the prediction. It must be of type double.

**numIterations:** The number of iterations to be used for training the model. It must be of type Integer. Default is
100.

**stepSize:** The step size to be used for training the model. It must be of type Double. Default is 1.0.

Example
-------
This example uses the field 'age' from the input record as featuresToInclude and 'lung_capacity' field as the
label to train the model.

    {
        "name": "LinearRegressionTrainer",
        "type": "sparksink",
        "properties": {
            "fileSetName": "linear-regression-model",
            "path": "linearRegression",
            "featuresToInclude": "age",
            "labelField": "lung_capacity",
            "numIterations": "50",
            "stepSize": "0.001"
        }
    }
