# Spark Program in Scala

Description
-----------
Executes user-provided Spark code in Scala.

Use Case
--------
This plugin can be used when you want arbitrary Spark code.

Properties
----------
**mainClass** The fully qualified class name for the Spark application.
It must either be an ``object`` that has a ``main`` method define inside, with the method signature as 
``def main(args: Array[String]): Unit``; or it is a class that extends from the CDAP 
``co.cask.cdap.api.spark.SparkMain`` trait that implements the ``run`` method, with the method signature as
``def run(implicit sec: SparkExecutionContext): Unit``

**scalaCode** The self-contained Spark application written in Scala.
For example, an application that reads from CDAP stream with name ``streamName``, 
performs a simple word count logic and logs the result can be written as:

    import co.cask.cdap.api.spark._
    import org.apache.spark._
    import org.slf4j._

    class SparkProgram extends SparkMain {
      import SparkProgram._

      override def run(implicit sec: SparkExecutionContext): Unit = {
        val sc = new SparkContext
        val result = sc.fromStream[String]("streamName")
          .flatMap(_.split("\\s+"))
          .map((_, 1))
          .reduceByKey(_ + _)
          .collectAsMap
          
        LOG.info("Result is: {}", result)
      }
    }

    object SparkProgram {
      val LOG = LoggerFactory.getLogger(getClass())
    }
 
Please refer to the [CDAP documentation](https://docs.cask.co/cdap/current/en/developers-manual/building-blocks/spark-programs.html#cdap-spark-program) on the enhancements that CDAP brings to Spark.