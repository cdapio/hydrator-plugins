# GD-Tree Trainer


Description
-----------
Using a GD-Tree Classifier algorithm, trains a model based upon a particular label and various text fields of a record.
Saves this model to a FileSet dataset.

Use Case
--------
User wants to predict if the flight will be delayed or not based on some features of airline data:
Label → delayed and not delayed - delayed if 1.0 and 0.0 otherwise
Features → {dayOfMonth, weekday, scheduledDepTime, scheduledArrTime, carrier, elapsedTime, origin, dest}

Properties
----------
**fileSetName:** The name of the FileSet to save the model to.

**path:** Path of the FileSet to save the model to.

**featuresToInclude:** A comma-separated sequence of fields to use for training.

**featuresToExclude:** A comma-separated sequence of fields to exclude from being used for training.

**labelField:** The field from which to get the prediction. It must be of type double.

**cardinalityMapping:** Mapping of the feature to the cardinality of that feature; required for categorical features.

**maxClass:** The number of classes to use in training the model. It must be of type integer.

**maxDepth:** Maximum depth of the tree.
For example, depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes. Default is 10

**maxIteration:** The number of trees in the model. Each iteration produces one tree. Increased iteration value improves
 training data accuracy.


Both *featureFieldsToInclude* and *featureFieldsToExclude* fields cannot be specified simultaneously.
If inputs for *featureFieldsToInclude* and *featureFieldsToExclude* has not been provided then all the fields except
label field will be used as feature fields.

Example
-------

This example uses the fields dofM, dofW, scheduleDepTime, scheduledArrTime, carrier, elapsedTime, origin, dest
from the input record as features and delayed field as the label to train the model.

{
  "name": "GDTreeTrainer",
  "type": "sparksink",
  "properties": {
        "fileSetName": "gd-tree-model",
        "path": "/home/cdap",
        "featuresToInclude": "dofM,dofW,scheduleDepTime,scheduledArrTime,carrier,elapsedTime,origin,dest",
        "predictionField": "delayed",
        "maxClass": "2",
        "maxDepth": "9",
        "maxIteration": "3"
   }
}
