# DecisionTreeRegressor


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

**features:** A comma-separated sequence of fields for regression. Features to be used, must be of simple type : String,
int, double, float, long, bytes, boolean.

**predictionField:** The field on which to set the prediction. It will be of type double.


Example
-------
This example uses ``dofM, dofW, scheduleDepTime, scheduledArrTime, carrier, elapsedTime, origin, dest`` fields from the
input record as features to predict ``delayed`` field.

    {
      "name": "DecisionTreeRegression",
      "type": "sparkcompute",
      "properties": {
            "fileSetName": "decision-tree-model",
            "path": "decisionTree",
            "features": "dofM,dofW,scheduleDepTime,scheduledArrTime,carrier,elapsedTime,origin,dest",
            "predictionField": "delayed"
       }
    }


For example, suppose the regressor receives the input records, where each record represents a flight detail:

     +=======================================================================================================+
     | DayOf | DayOf | Carrier | TailNum | FlightNum | Origin | Destination | Distance | Departure | Arrival |
     | Month | Week  |         |         |           |        |             |          |           |         |
     +=======================================================================================================+
     | 2     | 4     | DL      | N523US  | 1756      | ATL    | MLB         | 442      | 2058      | 2229    |
     | 20    | 1     | AA      | N3GEAA  | 1162      | DEN    | DFW         | 641      | 905       | 1205    |
     | 4     | 6     | AA      | N3CBAA  | 1314      | MIA    | IAH         | 964      | 1225      | 1420    |
     +=======================================================================================================+


Output records will contain all the fields along with the predicted field:

     +=================================================================================================================+
     | DayOf | DayOf | Carrier | TailNum | FlightNum | Origin | Destination | Distance | Departure | Arrival | Dealyed |
     | Month | Week  |         |         |           |        |             |          |           |         |         |
     +=================================================================================================================+
     | 2     | 4     | DL      | N523US  | 1756      | ATL    | MLB         | 442      | 2058      | 2229    | 1.0     |
     | 20    | 1     | AA      | N3GEAA  | 1162      | DEN    | DFW         | 641      | 905       | 1205    | 0.0     |
     | 4     | 6     | AA      | N3CBAA  | 1314      | MIA    | IAH         | 964      | 1225      | 1420    | 1.0     |
     +=================================================================================================================+