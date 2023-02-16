/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.plugin.common.db;

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.List;
import javax.annotation.Nullable;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

/**
 * Utility methods for Database plugins shared by Database plugins.
 */
public final class DBUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DBUtils.class);
  public static final String PLUGIN_TYPE_JDBC = "jdbc";
  public static final String OVERRIDE_SCHEMA = "io.cdap.hydrator.db.override.schema";
  public static final String PATTERN_TO_REPLACE = "io.cdap.plugin.db.pattern.replace";
  public static final String REPLACE_WITH = "io.cdap.plugin.db.replace.with";
  public static final String CONNECTION_ARGUMENTS = "io.cdap.hydrator.db.connection.arguments";
  public static final String FETCH_SIZE = "io.cdap.hydrator.db.fetch.size";
  public static final String POSTGRESQL_TAG = "postgresql";
  public static final String POSTGRESQL_DEFAULT_SCHEMA = "public";

  /**
   * Performs any Database related cleanup
   *
   * @param driverClass the JDBC driver class
   */
  public static void cleanup(Class<? extends Driver> driverClass) {
    ClassLoader pluginClassLoader = driverClass.getClassLoader();
    if (pluginClassLoader == null) {
      // This could only be null if the classLoader is the Bootstrap/Primordial classloader. This should never be the
      // case since the driver class is always loaded from the plugin classloader.
      LOG.warn("PluginClassLoader is null. Cleanup not necessary.");
      return;
    }
    shutDownMySQLAbandonedConnectionCleanupThread(pluginClassLoader);
    unregisterOracleMBean(pluginClassLoader);
  }

  /**
   * Ensures that the JDBC Driver specified in configuration is available and can be loaded. Also registers it with
   * {@link DriverManager} if it is not already registered.
   */
  public static DriverCleanup ensureJDBCDriverIsAvailable(Class<? extends Driver> jdbcDriverClass,
    String connectionString, String jdbcPluginName, String jdbcPluginType)
    throws IllegalAccessException, InstantiationException, SQLException {

    try {
      DriverManager.getDriver(connectionString);
      return new DriverCleanup(null);
    } catch (SQLException e) {
      // Driver not found. We will try to register it with the DriverManager.
      LOG.debug("Plugin Type: {} and Plugin Name: {}; Driver Class: {} not found. Registering JDBC driver via shim {} ",
                jdbcPluginType, jdbcPluginName, jdbcDriverClass.getName(), JDBCDriverShim.class.getName());
      final JDBCDriverShim driverShim = new JDBCDriverShim(jdbcDriverClass.newInstance());
      try {
        DBUtils.deregisterAllDrivers(jdbcDriverClass);
      } catch (NoSuchFieldException | ClassNotFoundException e1) {
        LOG.error("Unable to deregister JDBC Driver class {}", jdbcDriverClass);
      }
      DriverManager.registerDriver(driverShim);
      return new DriverCleanup(driverShim);
    }
  }


  /**
   * Given the result set, get the metadata of the result set and return list of {@link
   * io.cdap.cdap.api.data.schema.Schema.Field}, using the schemaStr
   *
   * @param resultsetSchema the schema from the db
   * @param schema       Overriden Schema if it is compatible with the resultsetSchema
   * @return list of schema fields only if all the fields are compatible with overridden schemas
   */
  public static List<Schema.Field> getSchemaFields(Schema resultsetSchema, @Nullable Schema schema) {
    if (schema != null) {
      for (Schema.Field field : schema.getFields()) {
        Schema.Field resultsetField = resultsetSchema.getField(field.getName());
        if (resultsetField == null) {
          throw new IllegalArgumentException(
            String.format("Schema field '%s' is not present in input record.", field.getName()));
        }
        Schema resultsetFieldSchema =
          resultsetField.getSchema().isNullable() ? resultsetField.getSchema().getNonNullable() :
            resultsetField.getSchema();
        Schema simpleSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();

        if (!isCompatible(resultsetFieldSchema, simpleSchema)) {
          throw new IllegalArgumentException(String
            .format("Schema field '%s' has type '%s' but in input record " + "found type '%s'.", field.getName(),
              simpleSchema.getDisplayName(), resultsetFieldSchema.getDisplayName()));
        }
      }
      return schema.getFields();
    }
    return resultsetSchema.getFields();
  }

  private static boolean isCompatible(Schema resultSetSchema, Schema mappedSchema) {
    if (resultSetSchema.equals(mappedSchema)) {
      return true;
    }
    //allow mapping from string to datetime, values will be validated for format
    if (resultSetSchema.getType() == Schema.Type.STRING &&
      mappedSchema.getLogicalType() == Schema.LogicalType.DATETIME) {
      return true;
    }

    // Allow mapping from Double to String type
    if (mappedSchema.getType() == Schema.Type.STRING
        && resultSetSchema.getType() == Schema.Type.DOUBLE) {
      return true;
    }

    return false;
  }

  /**
   * load the specified JDBC driver class
   *
   * @param configurer     the plugin configurer
   * @param jdbcPluginName the jdbc plugin name
   * @param jdbcPluginId   the unique id of this usage
   * @param collector      the failure collector
   * @return the loaded JDBC driver class
   */
  public static Class<? extends Driver> loadJDBCDriverClass(PluginConfigurer configurer, String jdbcPluginName,
                                                            String jdbcPluginType, String jdbcPluginId,
                                                            @Nullable FailureCollector collector) {
    Class<? extends Driver> jdbcDriverClass =
      configurer.usePluginClass(jdbcPluginType, jdbcPluginName, jdbcPluginId, PluginProperties.builder().build());
    if (jdbcDriverClass == null) {
      String error = String.format("Unable to load JDBC Driver class for plugin name '%s'.", jdbcPluginName);
      String action = String.format(
        "Ensure that plugin '%s' of type '%s' containing the driver has been deployed.", jdbcPluginName,
        jdbcPluginType);
      if (collector != null) {
        collector.addFailure(error, action).withConfigProperty(DBConnectorProperties.PLUGIN_JDBC_PLUGIN_NAME)
          .withPluginNotFound(jdbcPluginId, jdbcPluginName, jdbcPluginType);
      } else {
        throw new IllegalArgumentException(error + " " + action);
      }
    }
    return jdbcDriverClass;
  }

  /**
   * De-register all SQL drivers that are associated with the class
   */
  public static void deregisterAllDrivers(Class<? extends Driver> driverClass)
    throws NoSuchFieldException, IllegalAccessException, ClassNotFoundException {
    Field field = DriverManager.class.getDeclaredField("registeredDrivers");
    field.setAccessible(true);
    List<?> list = (List<?>) field.get(null);
    for (Object driverInfo : list) {
      Class<?> driverInfoClass = DBUtils.class.getClassLoader().loadClass("java.sql.DriverInfo");
      Field driverField = driverInfoClass.getDeclaredField("driver");
      driverField.setAccessible(true);
      Driver d = (Driver) driverField.get(driverInfo);
      if (d == null) {
        LOG.debug("Found null driver object in drivers list. Ignoring.");
        continue;
      }
      LOG.debug("Removing non-null driver object from drivers list.");
      ClassLoader registeredDriverClassLoader = d.getClass().getClassLoader();
      if (registeredDriverClassLoader == null) {
        LOG.debug("Found null classloader for default driver {}. Ignoring since this may be using system classloader.",
          d.getClass().getName());
        continue;
      }
      // Remove all objects in this list that were created using the classloader of the caller.
      if (d.getClass().getClassLoader().equals(driverClass.getClassLoader())) {
        LOG.debug("Removing default driver {} from registeredDrivers", d.getClass().getName());
        list.remove(driverInfo);
      }
    }
  }

  /**
   * Shuts down a cleanup thread com.mysql.jdbc.AbandonedConnectionCleanupThread that mysql driver fails to destroy If
   * this is not done, the thread keeps a reference to the classloader, thereby causing OOMs or too many open files
   *
   * @param classLoader the unfiltered classloader of the jdbc driver class
   */
  private static void shutDownMySQLAbandonedConnectionCleanupThread(ClassLoader classLoader) {
    try {
      Class<?> mysqlCleanupThreadClass;
      try {
        mysqlCleanupThreadClass = classLoader.loadClass("com.mysql.jdbc.AbandonedConnectionCleanupThread");
      } catch (ClassNotFoundException e) {
        // Ok to ignore, since we may not be running mysql
        LOG.trace("Failed to load MySQL abandoned connection cleanup thread class. Presuming DB App is " +
          "not being run with MySQL and ignoring", e);
        return;
      }
      Method shutdownMethod = mysqlCleanupThreadClass.getMethod("shutdown");
      shutdownMethod.invoke(null);
      LOG.debug("Successfully shutdown MySQL connection cleanup thread.");
    } catch (Throwable e) {
      // cleanup failed, ignoring silently with a log, since not much can be done.
      LOG.warn("Failed to shutdown MySQL connection cleanup thread. Ignoring.", e);
    }
  }

  private static void unregisterOracleMBean(ClassLoader classLoader) {
    try {
      classLoader.loadClass("oracle.jdbc.driver.OracleDriver");
    } catch (ClassNotFoundException e) {
      LOG.debug("Oracle JDBC Driver not found. Presuming that the DB App is not being run with an Oracle DB. " +
                  "Not attempting to cleanup Oracle MBean.");
      return;
    }
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    Hashtable<String, String> keys = new Hashtable<>();
    keys.put("type", "diagnosability");
    keys.put(
      "name", classLoader.getClass().getName() + "@" + Integer.toHexString(classLoader.hashCode()).toLowerCase());
    ObjectName oracleJdbcMBeanName;
    try {
      oracleJdbcMBeanName = new ObjectName("com.oracle.jdbc", keys);
    } catch (MalformedObjectNameException e) {
      // This should never happen, since we're constructing the ObjectName correctly
      LOG.debug("Exception while constructing Oracle JDBC MBean Name. Aborting cleanup.", e);
      return;
    }
    try {
      mbs.getMBeanInfo(oracleJdbcMBeanName);
    } catch (InstanceNotFoundException e) {
      LOG.debug("Oracle JDBC MBean not found. No cleanup necessary.");
      return;
    } catch (IntrospectionException | ReflectionException e) {
      LOG.debug("Exception while attempting to retrieve Oracle JDBC MBean. Aborting cleanup.", e);
      return;
    }

    try {
      mbs.unregisterMBean(oracleJdbcMBeanName);
      LOG.debug("Oracle MBean unregistered successfully.");
    } catch (InstanceNotFoundException | MBeanRegistrationException e) {
      LOG.debug("Exception while attempting to cleanup Oracle JDBCMBean. Aborting cleanup.", e);
    }
  }

  private DBUtils() {
    throw new AssertionError("Should not instantiate static utility class.");
  }
}
