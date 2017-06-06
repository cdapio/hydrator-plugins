/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin.spark.dynamic;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.spark.dynamic.CompilationFailureException;
import co.cask.cdap.api.spark.dynamic.SparkCompiler;
import co.cask.cdap.api.spark.dynamic.SparkInterpreter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.reflect.ClassTag$;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import javax.annotation.Nullable;

/**
 * A {@link SparkCompute} that takes any scala code and executes it.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("ScalaSparkCompute")
@Description("Executes user-provided Spark code written in Scala that performs RDD to RDD transformation")
public class ScalaSparkCompute extends SparkCompute<StructuredRecord, StructuredRecord> {

  private static final String PACKAGE_NAME = "co.cask.hydrator.plugin.spark.dynamic.generated";
  private static final String CLASS_NAME = "UserSparkCompute";
  private static final String FULL_CLASS_NAME = PACKAGE_NAME + "." + CLASS_NAME;

  private final Config config;
  // A strong reference is needed to keep the compiled classes around
  @SuppressWarnings("FieldCanBeLocal")
  private SparkInterpreter interpreter;
  private Method method;
  private boolean takeContext;

  public ScalaSparkCompute(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    try {
      if (!config.containsMacro("schema")) {
        stageConfigurer.setOutputSchema(
          config.getSchema() == null ? stageConfigurer.getInputSchema() : Schema.parseJson(config.getSchema())
        );
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse output schema " + config.getSchema(), e);
    }

    if (!config.containsMacro("scalaCode")) {
      SparkInterpreter interpreter = SparkCompilers.createInterpreter();
      if (interpreter != null) {
        try {
          interpreter.compile(generateSourceClass());
        } catch (CompilationFailureException e) {
          throw new IllegalArgumentException(e.getMessage(), e);
        }
      }
    }
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    interpreter = context.createSparkInterpreter();
    StringWriter writer = new StringWriter();

    try (PrintWriter sourceWriter = new PrintWriter(writer, false)) {
      sourceWriter.println("package " + PACKAGE_NAME);
      // Includes some commonly used imports.
      sourceWriter.println("import co.cask.cdap.api.data.format._");
      sourceWriter.println("import co.cask.cdap.api.data.schema._");
      sourceWriter.println("import co.cask.cdap.etl.api.batch._");
      sourceWriter.println("import org.apache.spark._");
      sourceWriter.println("import org.apache.spark.api.java._");
      sourceWriter.println("import org.apache.spark.rdd._");
      sourceWriter.println("import org.apache.spark.sql._");
      sourceWriter.println("import org.apache.spark.SparkContext._");
      sourceWriter.println("import scala.collection.JavaConversions._");
      sourceWriter.println("object " + CLASS_NAME + " {");
      sourceWriter.println(config.getScalaCode());
      sourceWriter.println("}");
    }

    interpreter.compile(writer.toString());

    // Use reflection to load the class and get the transform method
    try {
      Class<?> computeClass = interpreter.getClassLoader().loadClass(FULL_CLASS_NAME);

      // First see if it has the transform(RDD[StructuredRecord], SparkExecutionPluginContext) method
      try {
        method = computeClass.getDeclaredMethod("transform", RDD.class, SparkExecutionPluginContext.class);
        takeContext = true;
      } catch (NoSuchMethodException e) {
        // Try to find the transform(RDD[StructuredRecord])
        method = computeClass.getDeclaredMethod("transform", RDD.class);
        takeContext = false;
      }

      Type[] parameterTypes = method.getGenericParameterTypes();

      // The first parameter should be of type RDD[StructuredRecord]
      validateRDDType(parameterTypes[0],
                      "The first parameter of the 'transform' method should have type as 'RDD[StructuredRecord]'");

      // If it has second parameter, then must be SparkExecutionPluginContext
      if (takeContext && !SparkExecutionPluginContext.class.equals(parameterTypes[1])) {
        throw new IllegalArgumentException(
          "The second parameter of the 'transform' method should have type as SparkExecutionPluginContext");
      }

      // The return type of the method must be RDD[StructuredRecord]
      validateRDDType(method.getGenericReturnType(),
                      "The return type of the 'transform' method should be 'RDD[StructuredRecord]'");

      method.setAccessible(true);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
        "Missing a `transform` method that has signature either as " +
        "'def transform(rdd: RDD[StructuredRecord]) : RDD[StructuredRecord]' or " +
        "'def transform(rdd: RDD[StructuredRecord], context: SparkExecutionPluginContext) : RDD[StructuredRecord]'", e);
    }
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext sparkExecutionPluginContext,
                                             JavaRDD<StructuredRecord> javaRDD) throws Exception {
    RDD<StructuredRecord> rdd;
    if (takeContext) {
      //noinspection unchecked
      rdd = (RDD<StructuredRecord>) method.invoke(null, javaRDD.rdd(), sparkExecutionPluginContext);
    } else {
      //noinspection unchecked
      rdd = (RDD<StructuredRecord>) method.invoke(null, javaRDD.rdd());
    }
    return JavaRDD.fromRDD(rdd, ClassTag$.MODULE$.<StructuredRecord>apply(StructuredRecord.class));
  }

  private String generateSourceClass() {
    StringWriter writer = new StringWriter();

    try (PrintWriter sourceWriter = new PrintWriter(writer, false)) {
      sourceWriter.println("package " + PACKAGE_NAME);
      sourceWriter.println("import co.cask.cdap.api.data.format._");
      sourceWriter.println("import co.cask.cdap.api.data.schema._");
      sourceWriter.println("import co.cask.cdap.etl.api.batch._");
      sourceWriter.println("import org.apache.spark._");
      sourceWriter.println("import org.apache.spark.api.java._");
      sourceWriter.println("import org.apache.spark.rdd._");
      sourceWriter.println("import org.apache.spark.SparkContext._");
      sourceWriter.println("object " + CLASS_NAME + " {");
      sourceWriter.println(config.getScalaCode());
      sourceWriter.println("}");
    }
    return writer.toString();
  }

  /**
   * Validates the given type is {@code RDD<StructuredRecord>}.
   */
  private void validateRDDType(Type rddType, String errorMessage) {
    if (!(rddType instanceof ParameterizedType)) {
      throw new IllegalArgumentException(errorMessage);
    }
    if (!RDD.class.equals(((ParameterizedType) rddType).getRawType())) {
      throw new IllegalArgumentException(errorMessage);
    }

    Type[] typeParams = ((ParameterizedType) rddType).getActualTypeArguments();
    if (typeParams.length < 1 || !typeParams[0].equals(StructuredRecord.class)) {
      throw new IllegalArgumentException(errorMessage);
    }
  }

  /**
   * Configuration object for the plugin
   */
  public static final class Config extends PluginConfig {

    @Description("Spark code in Scala defining how to transform RDD to RDD. " +
      "The code must implement a function " +
      "called 'transform', which has signature as either \n" +
      "  def transform(rdd: RDD[StructuredRecord]) : RDD[StructuredRecord]\n" +
      "  or\n" +
      "  def transform(rdd: RDD[StructuredRecord], context: SparkExecutionPluginContext) : RDD[StructuredRecord]\n" +
      "For example:\n" +
      "'def transform(rdd: RDD[StructuredRecord]) : RDD[StructuredRecord] = {\n" +
      "   rdd.filter(_.get(\"gender\") == null)\n" +
      " }'\n" +
      "will filter out incoming records that does not have the 'gender' field."
    )
    @Macro
    private final String scalaCode;

    @Description("The schema of output objects. If no schema is given, it is assumed that the output schema is " +
      "the same as the input schema.")
    @Nullable
    @Macro
    private final String schema;

    public Config(String scalaCode, @Nullable String schema) {
      this.scalaCode = scalaCode;
      this.schema = schema;
    }

    public String getScalaCode() {
      return scalaCode;
    }

    @Nullable
    public String getSchema() {
      return schema;
    }
  }
}
