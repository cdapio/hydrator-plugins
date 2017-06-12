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

import co.cask.cdap.api.spark.dynamic.SparkInterpreter;
import scala.Function0;
import scala.Option$;
import scala.reflect.io.VirtualDirectory;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;
import scala.tools.nsc.Settings;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import javax.annotation.Nullable;

/**
 * Helper class for Spark compilation. We don't need this class when CDAP-11812 is resolved.
 */
public final class SparkCompilers {

  @Nullable
  public static SparkInterpreter createInterpreter() {
    try {
      ClassLoader classLoader = SparkInterpreter.class.getClassLoader();
      Settings settings = (Settings) classLoader
        .loadClass("co.cask.cdap.app.runtime.spark.dynamic.AbstractSparkCompiler")
        .getDeclaredMethod("setClassPath", Settings.class)
        .invoke(null, new Settings());

      Class<?> interpreterClass = classLoader
        .loadClass("co.cask.cdap.app.runtime.spark.dynamic.DefaultSparkInterpreter");

      // There should be a constructor
      Constructor<?>[] constructors = interpreterClass.getDeclaredConstructors();
      if (constructors.length != 1) {
        return null;
      }
      Constructor<?> constructor = constructors[0];

      // Create a empty URLAdder implementation using proxy, because that class is not available via cdap api
      InvocationHandler handler = new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
          // no-op
          return null;
        }
      };
      Class<?> urlAdderClass = classLoader.loadClass("co.cask.cdap.app.runtime.spark.dynamic.URLAdder");
      Object urlAdder = Proxy.newProxyInstance(classLoader, new Class<?>[]{urlAdderClass}, handler);
      Function0<BoxedUnit> onCloseFunc = new AbstractFunction0<BoxedUnit>() {
        @Override
        public BoxedUnit apply() {
          return BoxedUnit.UNIT;
        }
      };

      // For Spark 1, the constructor has 4 parameters. For Spark 2, it has 3 parameters
      SparkInterpreter sparkInterpreter = null;
      if (constructor.getParameterTypes().length == 4) {
        // Spark 1
        VirtualDirectory virtualDirectory = new VirtualDirectory("memory", Option$.MODULE$.<VirtualDirectory>empty());
        sparkInterpreter = (SparkInterpreter) constructor.newInstance(settings, virtualDirectory,
                                                                      urlAdder, onCloseFunc);

      } else if (constructor.getParameterTypes().length == 3) {
        // spark 2
        sparkInterpreter = (SparkInterpreter) constructor.newInstance(settings, urlAdder, onCloseFunc);
      }

      return sparkInterpreter;
    } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException
                                    | IllegalAccessException | InstantiationException e) {
      return null;
    }
  }

  private SparkCompilers() {
    // no-op
  }
}
