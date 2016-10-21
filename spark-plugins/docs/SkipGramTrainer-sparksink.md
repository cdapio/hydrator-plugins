# SkipGramTrainer


Description
-----------
Spark Sink plugin that trains a model using SkipGram (Spark's Word2Vec). Saves this model to a FileSet.

Use Case
--------
This sink can be used when you have a sample data and you want to use it to build a SkipGram (Spark's Word2Vec) model.

Properties
----------
**fileSetName:** The name of the FileSet to save the model to.

**path:** Path of the FileSet to save the model to.

**inputCol:** Field to be used for training.

**pattern:** Pattern to split the input string fields on. Default is '\s+'.

**vectorSize:** The dimension of codes after transforming from words. Default 100.

**minCount:** The minimum number of times a token must appear to be included in the word2vec model's vocabulary.
Default is 0.

**numPartitions:** Sets number of partitions. Use a small number for accuracy. Default is 1.

**numIterations:** Maximum number of iterations (>= 0). Default is 1.

**windowSize:** The window size (context words from [-window, window]). Default is 5.


Example
-------
This example uses the field ``text`` from the input record as input column to train the model for vector size and
window size as 3. The model's vocabulary will contain words with min count 2 and number of partitions for sentences of
words and number of iterations as 1.

    {
        "name": "FeatureTrainer",
        "type": "sparksink",
        "properties": {
            "fileSetName": "feature-generator",
            "path": "feature",
            "inputCol": "text",
            "pattern": " ",
            "vectorSize": "3",
            "minCount": "2",
            "numPartitions": "1",
            "numIterations ": "1",
            "windowSize ": "3"
        }
    }
