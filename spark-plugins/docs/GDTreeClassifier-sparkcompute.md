# GD-Tree Trainer


Description
-----------
Loads a GD-Tree model from a FileSet dataset and uses it to classify records.

Use Case
--------
User wants to predict if the flight will be delayed or not based on some features of airline data:
Label → delayed and not delayed - delayed if 1.0 and 0.0 otherwise
Features → {dayOfMonth, weekday, scheduledDepTime, scheduledArrTime, carrier, elapsedTime, origin, dest}

Properties
----------
**fileSetName:** The name of the FileSet to load the model from.

**path:** Path of the FileSet to load the model from.

**featuresToInclude:** A comma-separated sequence of fields to use for classification.

**featuresToExclude:** A comma-separated sequence of fields to exclude from being used for classification.

**predictionField:** The field on which prediction needs to be set. It will be of type double.

Both *featureFieldsToInclude* and *featureFieldsToExclude* fields cannot be specified simultaneously.
If inputs for *featureFieldsToInclude* and *featureFieldsToExclude* has not been provided then all the fields except
predictionField field will be used as feature fields.

Example
-------

This example uses the fields ``dofM, dofW, scheduleDepTime, scheduledArrTime, carrier, elapsedTime, origin, dest``
from the input record as features to use for classification and sets the prediction on to the ``delayed`` field.

{
  "name": "GDTreeClassifier",
  "type": "sparksink",
  "properties": {
        "fileSetName": "gd-tree-model",
        "path": "/home/cdap",
        "featuresToInclude": "dofM,dofW,scheduleDepTime,scheduledArrTime,carrier,elapsedTime,origin,dest",
        "predictionField": "delayed"
   }
}
