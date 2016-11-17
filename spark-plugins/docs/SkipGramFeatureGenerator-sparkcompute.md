# SkipGramFeatureGenerator


Description
-----------
SparkCompute to generate text based feature from string using stored skip-gram model (Spark's Word2Vec).

Use Case
--------
This transform can be used when user has a saved Word2Vec model and wants to generate text based feature.

Properties
----------
**fileSetName:** The name of the FileSet to load the model from.

**path:** Path of the FileSet to load the model from.

**outputColumnMapping:** A comma-separated list of the input fields to map to the transformed output fields. The key
specifies the name of the field to generate feature vector from, with its corresponding value specifying the output
column in which the vector would be emitted.

**pattern:** Pattern to split the input string fields on. Default is '\s+'.


Example
-------
This example uses saved model ``feature-generator`` to transform column ``text`` and emit the generated dense vector as
array of double in column ``result``.

    {
        "name": "FeatureGenerator",
        "type": "sparkcompute",
        "properties": {
            "fileSetName": "feature-generator",
            "path": "feature",
            "outputColumnMapping": "text:result",
            "pattern": " "
        }
    }

For example, suppose the feature generator receives the input records:

    +==============================================+
    | offset |  text                               |
    +==============================================+
    | 1      | Spark ML plugins                    |
    | 2      | Classes in Java                     |
    | 3      | Logistic regression to predict data |
    +==============================================+


Output records will contain all the fields along with the output fields mentioned in ``outputColumnMapping``:

    +===================================================================================================================+
    | offset |  text                               |  result                                                            |
    +===================================================================================================================+
    | 1      | Spark ML plugins                    |[0.040902843077977494, -0.010430609186490376, -0.04750693837801615] |
    | 2      | Classes in Java                     |[-0.04352385476231575, 3.2448768615722656E-4, 0.02223073500208557]  |
    | 3      | Logistic regression to predict data |[0.011901815732320149, 0.019348077476024628, -0.0074237411220868426]|
    +===================================================================================================================+
