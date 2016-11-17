# Tokenizer Spark Compute

Description
-----------
Tokenization is the process of taking text (such as a sentence) and breaking it into individual terms (usually words) 
on the basis of pattern.

Tokenizer splits data on the basis of specified pattern and emits the output as string array of tokens.

Use Case
--------
User wants to extract the hashtags from the twitter feeds. User would tokenize the words based on space and then can
identify the words that start with hashtags.

Properties
----------
**columnToBeTokenized:** Column on which tokenization is to be done.

**patternSeparator:** Pattern separator for tokenization.

**outputColumn:** Output column name for tokenized data.

Example
-------
This example tokenizes "sentence" column into output column "words" using pattern "/".

    {
        "name": "Tokenizer",
        "type": "sparkcompute",
        "properties": {
            "columnToBeTokenized": "sentence",
            "patternSeparator": "/",
            "outputColumn": "words"
        }
    }


For example, suppose the tokenizer receives below input records:

    +=======================================================+
    | topic | sentence                                      |
    +=======================================================+
    | java  | Hello world / is the /basic application       |
    | HDFS  | HDFS/ is a /file system                       |
    | Spark | Spark /is an engine for /bigdata processing   |
    +=======================================================+

Output schema will contain additional column "words" having tokenized data in string array form:

    +=====================================================================================================+
    | topic | sentence                                     | words                                        |
    +=====================================================================================================+
    | java  | Hello world / is the /basic application      | [Hello world , is the ,basic application]    |
    | HDFS  | HDFS/ is a /file system                      | [Hdfs, is a ,file system]                    |
    | Spark | Spark /is an engine for /bigdata processing  | [Spark ,is an engine for ,bigdata processing]|
    +=====================================================================================================+
