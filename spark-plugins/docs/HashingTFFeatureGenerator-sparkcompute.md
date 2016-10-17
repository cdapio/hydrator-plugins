# HashingTFFeatureGenerator


Description
-----------
SparkCompute to generate text based feature from string using Hashing TF technique.

Use Case
--------
This transform can be used when user wants to generate text based feature using Hashing TF technique.

Properties
----------
**numFeatures:** The number of features to use in training the model. It must be of type integer. Deafult is 100.

**outputColumnMapping:** A comma-separated list of the input fields to map to the transformed output fields. The key
specifies the name of the field to generate feature vector from, with its corresponding value specifying the output
columns(size, indices and value) to emit the sparse vector.


Example
-------
This example transforms column ``text`` to generate fixed length vector of size 10 and emit the generated sparse vector
as a cobination of three columns: result_size, result_indices, result_value.

    {
        "name": "FeatureGenerator",
        "type": "sparkcompute",
        "properties": {
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


Output records will contain all the fields along with the 3 additional fields (for size, indices and values)
representing a sparse vector for each output field mentioned in ``outputColumnMapping``:

    +==========================================================================================================+
    | offset |  text                               | result_size | result_indices  | result_value              |
    +==========================================================================================================+
    | 1      | Hi I heard about Spark              | 10          | [3, 6, 7, 9]    | [2.0, 1.0, 1.0, 1.0]      |
    | 2      | Logistic regression models are neat | 10          | [0, 2, 4, 5, 8] | [1.0, 1.0, 1.0, 1.0, 1.0] |
    +==========================================================================================================+
