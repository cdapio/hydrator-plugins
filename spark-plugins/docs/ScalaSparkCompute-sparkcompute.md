# Spark Computation in Scala

Description
-----------
Executes user-provided Spark code in Scala that transforms RDD to RDD with full
access to all Spark features.

Use Case
--------
This plugin can be used when you want to have complete control on the Spark computation.
For example, you may want to join the input RDD with another Dataset and select a subset
of the join result using Spark SQL.

Properties
----------
**scalaCode** Spark code in Scala defining how to transform RDD to RDD. 
The code must implement a function called ``transform``, which the signature should either be

    def transform(rdd: RDD[StructuredRecord]) : RDD[StructuredRecord]

or

    def transform(rdd: RDD[StructuredRecord], context: SparkExecutionPluginContext) : RDD[StructuredRecord]
    
In the latter case, using the ``SparkExecutionPluginContext`` can access CDAP
entities such as Stream and Dataset, as well as providing access to the underlying ``SparkContext`` in use.

For example:

    def transform(rdd: RDD[StructuredRecord], context: SparkExecutionPluginContext) : RDD[StructuredRecord] = {
      val outputSchema = context.getOutputSchema
      rdd
        .flatMap(_.get[String]("body").split("\\s+"))
        .map(s => (s, 1))
        .reduceByKey(_ + _)
        .map(t => StructuredRecord.builder(outputSchema).set("word", t._1).set("count", t._2).build)
    }
        
The will perform a word count on the input field ``'body'``, 
and produces records of two fields, ``'word'`` and ``'count'``.

The following imports are included automatically and are ready for the user code to use:

      import co.cask.cdap.api.data.format._
      import co.cask.cdap.api.data.schema._;
      import co.cask.cdap.etl.api.batch._
      import org.apache.spark._
      import org.apache.spark.api.java._
      import org.apache.spark.rdd._
      import org.apache.spark.sql._
      import org.apache.spark.SparkContext._
      import scala.collection.JavaConversions._


**schema:** The schema of output objects. If no schema is given, it is assumed that the output
schema is the same as the input schema.