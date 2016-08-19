# Tokenizer Spark Compute


Description
-----------
Tokenization is the process of taking text (such as a sentence)
and breaking it into individual terms (usually words) on the basis of delimiter.

Tokenizer splits data on the basis of specified delimiter and emits the output as string array of tokens.
Tokenized data will be converted to lower case.

Use Case
--------
Tokenize the data on the basis of delimiter.
Tokenized output will be an array of delimited tokens.

Properties
----------
**columnToBeTokenized:** Column on which tokenization is to be done.

**delimiter:** Delimiter for tokenization.

**outputColumn:** Output column name for tokenized data.

Example
-------
This example tokenizes "sentence" column into output column "words" using delimiter "/".

    {
        "name": "Tokenizer",
        "type": "sparkcompute",
        "properties": {
            "columnToBeTokenized": "sentence",
            "delimiter": "/",
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

Output schema will contain only a single column "words" having tokenized data in string array form:

    +===============================================+
    | words                                         |
    +===============================================+
    | [hello world, is the, basic application]      |
    | [hdfs, is a ,file system]                     |
    | [spark ,is an engine for ,bigdata processing] |
    +===============================================+

