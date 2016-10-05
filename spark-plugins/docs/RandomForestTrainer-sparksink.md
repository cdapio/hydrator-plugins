# RandomForestTrainer


Description
-----------
Trains a regression model based upon a particular label and features of a record. Saves this model to a FileSet.

Use Case
--------
This sink can be used when you have a sample data and you want to use it to build a Random Forest Regression model.

Properties
----------
**fileSetName:** The name of the FileSet to save the model to.

**path:** Path of the FileSet to save the model to.

**featuresToInclude:** A comma-separated sequence of fields to be used for training. If both featuresToInclude and
featuresToExclude are empty, all fields except the label will be used for training. Features to be used, must be from
one of the following types: int, long, float or double. Both *featuresToInclude* and *featuresToExclude* fields cannot
be specified.

**featuresToExclude:** A comma-separated sequence of fields to be excluded for training. If both featuresToInclude and
 featuresToExclude are empty, all fields except the label will be used for training. Both *featuresToInclude* and
 *featuresToExclude* fields cannot be specified.

**cardinalityMapping:** Mapping of the feature to the cardinality of that feature; required for categorical features.

**labelField:** The field from which to get the prediction. It must be of type double.

**maxDepth:** Maximum depth of the tree.
For example, depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes. Default is 10.

**maxBins:** Maximum number of bins used for splitting when discretizing continuous features. MaxBins should be at least
 as large as the number of values in each categorical feature. Default is 100.

**numTrees:** Number of trees in the random forest.

**featureSubsetStrategy:** Number of features to consider for splits at each node. Supported: "auto", "all", "sqrt",
"log2", "onethird". If "auto" is set, this parameter is set based on numTrees:
`if numTrees == 1, set to "all"; if numTrees > 1 (forest) set to "sqrt".`

**seed:** Random seed for bootstrapping and choosing feature subsets. Default is 12345.

Example
-------
This example uses the fields ``dofM, dofW, scheduleDepTime, scheduledArrTime, carrier, elapsedTime, originId, destId``
from the input record as features and ``delayed`` field as the label to train the model.

    {
        "name": "RandomForestTrainer",
        "type": "sparkcompute",
        "properties": {
            "fileSetName": "random-forest-model",
            "path": "RandomForest",
            "featuresToInclude": "dofM,dofW,scheduleDepTime,scheduledArrTime,carrier,elapsedTime,originId,destId",
            "labelField": "delayed",
            "maxDept": "9",
            "maxBins": "100",
            "numTrees": "5",
            "featureSubsetStrategy": "auto",
            "seed": 12345
        }
    }
