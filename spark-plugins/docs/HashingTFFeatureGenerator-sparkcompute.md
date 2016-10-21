# HashingTFFeatureGenerator


Description
-----------
SparkCompute to generate text based feature from string using Hashing TF technique.

Use Case
--------
This transform can be used when user wants to generate text based feature using Hashing TF technique.

Properties
----------
**pattern:** Pattern to split the input string fields on. Default is '\s+'.

**numFeatures:** The number of features to use in training the model. It must be of type integer. Default is 2^20.

**outputColumnMapping:** A comma-separated list of the input fields to map to the transformed output fields. The key
specifies the name of the field to generate feature vector from, with its corresponding value specifying the output
columns(size, indices and value) to emit the sparse vector.


Example
-------
This example transforms column ``text`` to generate fixed length vector of size 10 and emit the generated sparse vector
as a record in ``result`` with three fields: ``size``, ``indices``, ``vectorValues``.

    {
        "name": "FeatureGenerator",
        "type": "sparkcompute",
        "properties": {
            "pattern": " ",
            "numFeatures": "10",
            "outputColumnMapping": "text:result"
        }
    }

For example, suppose the feature generator receives the input records:

    +==============================================+
    | offset |  text                               |
    +==============================================+
    | 1      | Hi I heard about Spark              |
    | 2      | Logistic regression models are neat |
    +==============================================+


Output records will contain the input fields ``offset`` and ``text`` along with the additional output fields as
mentioned in ``outputColumnMapping``(in this case ``result``). The output fields will represent the Sparse Vector
generator for the text and will be of type Record containing 3 fields: ``size``, ``indices`` and ``vectorValues``.

    +==============================================================================================================================+
    | offset |  text                               | result                                                                        |
    +==============================================================================================================================+
    | 1      | Hi I heard about Spark              | {"size":10,"indices":[3, 6, 7, 9],"vectorValues":[2.0, 1.0, 1.0, 1.0]}        |
    | 2      | Logistic regression models are neat | {"size":10,"indices":[0, 2, 4, 5, 8],"vectorValues":[1.0, 1.0, 1.0, 1.0, 1.0] |
    +==============================================================================================================================+
