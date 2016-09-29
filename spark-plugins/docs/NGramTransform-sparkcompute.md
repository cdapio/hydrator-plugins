# NGramTransform Spark Compute

Description
-----------
Transforms the input features into n-grams, where n-gram is a sequence of n tokens (typically words) for some integer 'n'.

Use Case
--------
A bio data scientist wants to  study the sequence of the nucleotides using the input stream of DNA sequencing to identify the bonds.
The input Stream contains the DNA sequence eg AGCTTCGA. The output contains the bigram sequence AG, GC, CT, TT, TC, CG, GA.

Properties
----------
**fieldToBeTransformed:** Field to be used to transform input features into n-grams.

**ngramSize:** NGram size. (Macro-enabled)

**outputField:** Transformed field for sequence of n-gram.

Example
-------
This example transforms features from "tokens" field into n-grams(output field name is ngrams) using ngram size as "2".

    {
        "name": "NGramTransform",
        "type": "sparkcompute",
        "properties": {
            "fieldToBeTransformed": "tokens",
            "ngramSize": "2",
            "outputField": "ngrams"
        }
    }


For example, suppose the NGramTransform receives below input records:

    +===================================+
    | topic | tokens                    |
    +===================================+
    | java  | [hi,i,heard,about,spark]  |
    | hdfs  | [hdfs,is,a,file,system]   |
    | spark | [spark,is,an,engine]      |
    +===================================+

Output schema will contain only a single field "ngrams" having transformed ngrams in string array form:

    +==========================================+
    | ngrams                                   |
    +==========================================+
    | [hi i,i heard,heard about,about spark]   |
    | [hdfs is,is a,a file,file system]        |
    | [spark is,is an,an engine]               |
    +==========================================+