# LinearRegressionPredictor


Description
-----------
Loads a Linear Regression model from a FileSet and uses it to label the records based on the predicted values.

Use Case
--------
This transform can be used when you have a saved Linear Regression model and want to predict the label for some data.

Properties
----------
**fileSetName:** The name of the FileSet to load the model from.

**path:** Path of the FileSet to load the model from.

**featuresToInclude:** A comma-separated sequence of fields for Linear Regression. If empty, all fields will be
used for prediction. Features to be used, must be from one of the following types: int, long, float or double.
Both *featuresToInclude* and *featuresToExclude* fields cannot be specified.

**featuresToExclude:** A comma-separated sequence of fields to be excluded when calculating for prediction. If empty,
 all fields will be used for prediction. Both *featuresToInclude* and *featuresToExclude* fields cannot be
specified.

**predictionField:** The field on which to set the prediction. It will be of type double.

Example
-------
This example uses the field 'age' from the input record as features, to predict 'lung_capacity' field.

    {
        "name": "LinearRegressionPredictor",
        "type": "sparkcompute",
        "properties": {
            "fileSetName": "linear-regression-model",
            "path": "linearRegression",
            "featuresToInclude": "age",
            "predictionField": "lung_capacity"
        }
    }

For example, suppose the predictor receives the input records, where each record represents a person's detail:


    +===================================+
    | age  | height  | smoke  | gender  |
    +===================================+
    | 11   | 58.7    | no     | female  |
    | 8    | 63.3    | no     | male    |
    | 18   | 74.7    | yes    | female  |
    +===================================+

Output records will contain all the fields along with the predicted field:


    +====================================================+
    | age  | height  | smoke  | gender  | lung_capacity  |
    +====================================================+
    | 11   | 58.7    | no     | female  | 6.471          |
    | 8    | 63.3    | no     | male    | 4.706          |
    | 18   | 74.7    | yes    | female  | 10.025         |
    +====================================================+

