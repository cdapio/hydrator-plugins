# DecisionTreePredictor


Description
-----------
Loads a Decision Tree Regression model from a FileSet and uses it to label the records based on the predicted values.

Use Case
--------
This transform can be used when you have a saved Decision Tree Regression model and want to predict the label for some
data.

Properties
----------
**fileSetName:** The name of the FileSet to load the model from.

**path:** Path of the FileSet to load the model from.

**featureFieldsToInclude:** A comma-separated sequence of fields to be used for prediction. If both
featureFieldsToInclude and featureFieldsToExclude are empty, all fields will be used for prediction. Features to be
used, must be from one of the following types: int, long, float or double. Both *featureFieldsToInclude* and
*featureFieldsToExclude* fields cannot be specified.

**featureFieldsToExclude:** A comma-separated sequence of fields to be excluded for prediction. If both
featureFieldsToInclude and featureFieldsToExclude are empty, all fields will be used for prediction. Both
*featureFieldsToInclude* and *featureFieldsToExclude* fields cannot be specified.

**predictionField:** The field on which to set the prediction. It will be of type double.


Example
-------
This example uses ``dofM, dofW, scheduleDepTime, scheduledArrTime, carrier, elapsedTime, originId, destId`` fields from
the input record as features to predict ``delayed`` field.

    {
        "name": "DecisionTreeRegression",
        "type": "sparkcompute",
        "properties": {
            "fileSetName": "decision-tree-model",
            "path": "decisionTree",
            "featuresToInclude": "dofM,dofW,scheduleDepTime,scheduledArrTime,carrier,elapsedTime,originId,destId",
            "predictionField": "delayed"
        }
    }


For example, suppose the regressor receives the input records, where each record represents a flight detail:

    +====================================================================================================+
    | DayOf | DayOf | Carrier | TailNum | FlightNum | OriginID | DestId | Distance | Departure | Arrival |
    | Month | Week  |         |         |           |          |        |          |           |         |
    +====================================================================================================+
    | 2     | 4     | 3.0     | N523US  | 1756      | 102      | 185    | 442      | 2058      | 2229    |
    | 20    | 1     | 1.0     | N3GEAA  | 1162      | 105      | 222    | 641      | 905       | 1205    |
    | 4     | 6     | 1.0     | N3CBAA  | 1314      | 205      | 245    | 964      | 1225      | 1420    |
    +====================================================================================================+


Output records will contain all the fields along with the predicted field:

    +==============================================================================================================+
    | DayOf | DayOf | Carrier | TailNum | FlightNum | OriginId | DestId | Distance | Departure | Arrival | Delayed |
    | Month | Week  |         |         |           |          |        |          |           |         |         |
    +==============================================================================================================+
    | 2     | 4     | 3.0     | N523US  | 1756      | 102      | 185    | 442      | 2058      | 2229    | 1.0     |
    | 20    | 1     | 1.0     | N3GEAA  | 1162      | 105      | 222    | 641      | 905       | 1205    | 0.0     |
    | 4     | 6     | 1.0     | N3CBAA  | 1314      | 205      | 245    | 964      | 1225      | 1420    | 1.0     |
    +==============================================================================================================+
