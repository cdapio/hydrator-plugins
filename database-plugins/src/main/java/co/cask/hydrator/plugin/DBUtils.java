/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
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
  public static final String OVERRIDE_SCHEMA = "co.cask.hydrator.db.override.schema";
  public static final String CONNECTION_ARGUMENTS = "co.cask.hydrator.db.connection.arguments";

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
                                                          String connectionString,
                                                          String jdbcPluginType, String jdbcPluginName)
    throws IllegalAccessException, InstantiationException, SQLException {

    try {
      DriverManager.getDriver(connectionString);
      return new DriverCleanup(null);
    } catch (SQLException e) {
      // Driver not found. We will try to register it with the DriverManager.
      LOG.debug("Plugin Type: {} and Plugin Name: {}; Driver Class: {} not found. Registering JDBC driver via shim {} ",
                jdbcPluginType, jdbcPluginName, jdbcDriverClass.getName(),
                JDBCDriverShim.class.getName());
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
   * Given the result set, get the metadata of the result set and return
   * list of {@link co.cask.cdap.api.data.schema.Schema.Field},
   * where name of the field is same as column name and type of the field is obtained using
   * {@link DBUtils#getType(int, int, int)}
   *
   * @param resultSet result set of executed query
   * @param schemaStr schema string to override resultant schema
   * @return list of schema fields
   * @throws SQLException
   * @throws IOException
   */
  public static List<Schema.Field> getSchemaFields(ResultSet resultSet, @Nullable String schemaStr)
    throws SQLException {
    Schema resultsetSchema = Schema.recordOf("resultset", getSchemaFields(resultSet));
    Schema schema;

    if (!Strings.isNullOrEmpty(schemaStr)) {
      try {
        schema = Schema.parseJson(schemaStr);
      } catch (IOException e) {
        throw new IllegalArgumentException(String.format("Unable to parse schema string %s", schemaStr), e);
      }
      for (Schema.Field field : schema.getFields()) {
        Schema.Field resultsetField = resultsetSchema.getField(field.getName());
        if (resultsetField == null) {
          throw new IllegalArgumentException(String.format("Schema field %s is not present in input record",
                                                           field.getName()));
        }
        Schema resultsetFieldSchema = resultsetField.getSchema().isNullable() ?
          resultsetField.getSchema().getNonNullable() : resultsetField.getSchema();
        Schema simpleSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();

        if (!resultsetFieldSchema.equals(simpleSchema)) {
          throw new IllegalArgumentException(String.format("Schema field %s has type %s but in input record found " +
                                                             "type %s ",
                                                           field.getName(), simpleSchema.getType(),
                                                           resultsetFieldSchema.getType()));
        }
      }
      return schema.getFields();

    }
    return resultsetSchema.getFields();
  }

  /**
   * Given the result set, get the metadata of the result set and return
   * list of {@link co.cask.cdap.api.data.schema.Schema.Field},
   * where name of the field is same as column name and type of the field is obtained using
   * {@link DBUtils#getType(int, int, int)}
   *
   * @param resultSet result set of executed query
   * @return list of schema fields
   * @throws SQLException
   */
  public static List<Schema.Field> getSchemaFields(ResultSet resultSet) throws SQLException {
    List<Schema.Field> schemaFields = Lists.newArrayList();
    ResultSetMetaData metadata = resultSet.getMetaData();
    // ResultSetMetadata columns are numbered starting with 1
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      String columnName = metadata.getColumnName(i);
      int columnSqlType = metadata.getColumnType(i);
      int columnSqlPrecision = metadata.getPrecision(i); // total number of digits
      int columnSqlScale = metadata.getScale(i); // digits after the decimal point
      Schema columnSchema = Schema.of(getType(columnSqlType, columnSqlPrecision, columnSqlScale));
      if (ResultSetMetaData.columnNullable == metadata.isNullable(i)) {
        columnSchema = Schema.nullableOf(columnSchema);
      }
      Schema.Field field = Schema.Field.of(columnName, columnSchema);
      schemaFields.add(field);
    }
    return schemaFields;
  }

  // given a sql type return schema type
  private static Schema.Type getType(int sqlType, int precision, int scale) throws SQLException {
    // Type.STRING covers sql types - VARCHAR,CHAR,CLOB,LONGNVARCHAR,LONGVARCHAR,NCHAR,NCLOB,NVARCHAR
    Schema.Type type = Schema.Type.STRING;
    switch (sqlType) {
      case Types.NULL:
        type = Schema.Type.NULL;
        break;

      case Types.ROWID:
        break;

      case Types.BOOLEAN:
      case Types.BIT:
        type = Schema.Type.BOOLEAN;
        break;

      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        type = Schema.Type.INT;
        break;

      case Types.BIGINT:
        type = Schema.Type.LONG;
        break;

      case Types.REAL:
      case Types.FLOAT:
        type = Schema.Type.FLOAT;
        break;

      case Types.NUMERIC:
      case Types.DECIMAL:
        // if there are no digits after the point, use integer types
        type = scale != 0 ? Schema.Type.DOUBLE :
          // with 10 digits we can represent 2^32 and LONG is required
          precision > 9 ? Schema.Type.LONG : Schema.Type.INT;
        break;

      case Types.DOUBLE:
        type = Schema.Type.DOUBLE;
        break;

      case Types.DATE:
      case Types.TIME:
      case Types.TIMESTAMP:
        type = Schema.Type.LONG;
        break;

      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
        type = Schema.Type.BYTES;
        break;

      case Types.ARRAY:
      case Types.DATALINK:
      case Types.DISTINCT:
      case Types.JAVA_OBJECT:
      case Types.OTHER:
      case Types.REF:
      case Types.SQLXML:
      case Types.STRUCT:
        throw new SQLException(new UnsupportedTypeException("Unsupported SQL Type: " + sqlType));
    }

    return type;
  }

  @Nullable
  public static Object transformValue(int sqlType, int precision, int scale,
                                      ResultSet resultSet, String fieldName) throws SQLException {
    Object original = resultSet.getObject(fieldName);
    if (original != null) {
      switch (sqlType) {
        case Types.SMALLINT:
        case Types.TINYINT:
          return ((Number) original).intValue();
        case Types.NUMERIC:
        case Types.DECIMAL:
          BigDecimal decimal = (BigDecimal) original;
          if (scale != 0) {
            // if there are digits after the point, use double types
            return decimal.doubleValue();
          } else if (precision > 9) {
            // with 10 digits we can represent 2^32 and LONG is required
            return decimal.longValue();
          } else {
            return decimal.intValue();
          }
        case Types.DATE:
          return resultSet.getDate(fieldName).getTime();
        case Types.TIME:
          return resultSet.getTime(fieldName).getTime();
        case Types.TIMESTAMP:
          return resultSet.getTimestamp(fieldName).getTime();
        case Types.ROWID:
          return resultSet.getString(fieldName);
        case Types.BLOB:
          Blob blob = (Blob) original;
          try {
            return blob.getBytes(1, (int) blob.length());
          } finally {
            blob.free();
          }
        case Types.CLOB:
          Clob clob = (Clob) original;
          try {
            return clob.getSubString(1, (int) clob.length());
          } finally {
            clob.free();
          }
      }
    }
    return original;
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
   * Shuts down a cleanup thread com.mysql.jdbc.AbandonedConnectionCleanupThread that mysql driver fails to destroy
   * If this is not done, the thread keeps a reference to the classloader, thereby causing OOMs or too many open files
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
    keys.put("name",
             classLoader.getClass().getName() + "@" + Integer.toHexString(classLoader.hashCode()).toLowerCase());
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
