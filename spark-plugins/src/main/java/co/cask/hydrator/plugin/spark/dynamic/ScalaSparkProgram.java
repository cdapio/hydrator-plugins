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
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.api.spark.SparkExecutionContext;
import co.cask.cdap.api.spark.SparkMain;
import co.cask.cdap.api.spark.dynamic.CompilationFailureException;
import co.cask.cdap.api.spark.dynamic.SparkInterpreter;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * A Spark program that executes user Spark code written in scala.
 */
@Plugin(type = "sparkprogram")
@Name("ScalaSparkProgram")
@Description("Executes user-provided Spark program")
public class ScalaSparkProgram implements JavaSparkMain {

  private final Config config;

  public ScalaSparkProgram(Config config) throws CompilationFailureException {
    this.config = config;

    if (!config.containsMacro("scalaCode")) {
      // Since we don't really be able to distinguish whether it is configure time or runtime,
      // we have to compile here using an explicitly constructed SparkInterpreter and then compile again
      // using SparkInterpreter in the run method
      SparkInterpreter interpreter = SparkCompilers.createInterpreter();
      if (interpreter != null) {
        interpreter.compile(config.getScalaCode());

        // Just create the callable without calling it for validating the class and method needed exists.
        if (!config.containsMacro("mainClass")) {
          getMethodCallable(interpreter.getClassLoader(), config.getMainClass(), null);
        }
      }
    }
  }

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    try (SparkInterpreter interpreter = sec.createInterpreter()) {
      interpreter.compile(config.getScalaCode());
      getMethodCallable(interpreter.getClassLoader(), config.getMainClass(), sec).call();
    }
  }

  /**
   * Creates a {@link Callable} that invoke the user method based on the type of the main class.
   */
  private Callable<Void> getMethodCallable(ClassLoader classLoader, String mainClass,
                                           @Nullable JavaSparkExecutionContext sec) {
    try {
      final Class<?> cls = classLoader.loadClass(mainClass);
      final Method method;
      final Object arg;

      // Determine which method to call on the user class
      if (JavaSparkMain.class.isAssignableFrom(cls)) {
        // Implements JavaSparkMain
        try {
          method = cls.getMethod("run", JavaSparkExecutionContext.class);
        } catch (NoSuchMethodException e) {
          // This shouldn't happen as we checked the class type already
          throw new IllegalArgumentException("Unexpected exception", e);
        }
        arg = sec;
      } else if (SparkMain.class.isAssignableFrom(cls)) {
        // Implements SparkMain
        try {
          method = cls.getMethod("run", SparkExecutionContext.class);
        } catch (NoSuchMethodException e) {
          // This shouldn't happen as we checked the class type already
          throw new IllegalArgumentException("Unexpected exception", e);
        }
        arg = sec == null ? null : sec.getSparkExecutionContext();
      } else {
        // Use static main(String[]) method
        try {
          method = cls.getDeclaredMethod("main", String[].class);
          if (!Modifier.isStatic(method.getModifiers())) {
            throw new IllegalArgumentException("The main class " + cls.getName() +
                                                 " must have a static main(args: Array[String]) method to" +
                                                 " run it as Spark program.");
          }
        } catch (NoSuchMethodException e) {
          throw new IllegalArgumentException("The main class " + cls.getName() +
                                               " must have a static main(args: Array[String]) method to" +
                                               " run it as Spark program.");
        }
        arg = sec == null ? null : RuntimeArguments.toPosixArray(sec.getRuntimeArguments());
      }

      return new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          Object instance = null;
          if (!Modifier.isStatic(method.getModifiers())) {
            instance = cls.newInstance();
          }
          method.invoke(instance, arg);
          return null;
        }
      };

    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Main class " + mainClass +
                                           " provided by the 'mainClass' configuration does not exists.");
    }
  }

  /**
   * Plugin configuration
   */
  public static final class Config extends PluginConfig {

    @Description("The fully qualified class name for the Spark main class defined in the scala code.")
    @Macro
    private final String mainClass;

    @Description(
      "The source code of the Spark program written in Scala. " +
      "The content must be a valid Scala source file.")
    @Macro
    private final String scalaCode;

    public Config(String scalaCode, String mainClass) {
      this.scalaCode = scalaCode;
      this.mainClass = mainClass;
    }

    public String getScalaCode() {
      return scalaCode;
    }

    public String getMainClass() {
      return mainClass;
    }
  }
}
