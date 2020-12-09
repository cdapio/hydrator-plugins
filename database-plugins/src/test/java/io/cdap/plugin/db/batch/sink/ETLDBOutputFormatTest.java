/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.db.batch.sink;

import org.junit.Assert;
import org.junit.Test;

public class ETLDBOutputFormatTest {
  @Test
  public void testRewriteSqlserverUrlWithCdapSocketFactory() throws Exception {
    ETLDBOutputFormat outputFormat = new ETLDBOutputFormat();
    String url = "jdbc:sqlserver://127.0.0.1;databaseName=mydb;socketFactoryClass=" +
      "io.cdap.socketfactory.sqlserver.SocketFactory";
    Assert.assertEquals(url, outputFormat.rewriteUrl(url));
    Assert.assertNull(outputFormat.getDelegateClass());
    Assert.assertEquals(ETLDBOutputFormat.SQLSERVER_SOCKET_FACTORY, outputFormat.getSocketFactory());
  }

  @Test
  public void testRewriteSqlserverUrlWithGcloudSocketFactory() throws Exception {
    ETLDBOutputFormat outputFormat = new ETLDBOutputFormat();
    String url = "jdbc:sqlserver://;databaseName=mydb;socketFactoryClass=" +
      "com.google.cloud.sql.sqlserver.SocketFactory;socketFactoryConstructorArg=project:us-west1:mssql";
    String expected = "jdbc:sqlserver://;databaseName=mydb;socketFactoryClass=" +
      "io.cdap.socketfactory.sqlserver.SocketFactory;socketFactoryConstructorArg=project:us-west1:mssql";
    Assert.assertEquals(expected, outputFormat.rewriteUrl(url));
    Assert.assertEquals("com.google.cloud.sql.sqlserver.SocketFactory", outputFormat.getDelegateClass());
    Assert.assertEquals(ETLDBOutputFormat.SQLSERVER_SOCKET_FACTORY, outputFormat.getSocketFactory());
  }

  @Test
  public void testRewriteSqlserverUrlWithoutSocketFactory() throws Exception {
    ETLDBOutputFormat outputFormat = new ETLDBOutputFormat();
    String url = "jdbc:sqlserver://localhost;databaseName=mydb";
    String expected = "jdbc:sqlserver://localhost;databaseName=mydb;socketFactoryClass=" +
      "io.cdap.socketfactory.sqlserver.SocketFactory";
    Assert.assertEquals(expected, outputFormat.rewriteUrl(url));
    Assert.assertNull(outputFormat.getDelegateClass());
    Assert.assertEquals(ETLDBOutputFormat.SQLSERVER_SOCKET_FACTORY, outputFormat.getSocketFactory());
  }

  @Test
  public void testRewriteMySqlUrlWithCdapSocketFactory() throws Exception {
    ETLDBOutputFormat outputFormat = new ETLDBOutputFormat();
    String url = "jdbc:mysql://google/mydb?socketFactory=io.cdap.socketfactory.mysql.SocketFactory";
    Assert.assertEquals(url, outputFormat.rewriteUrl(url));
    Assert.assertNull(outputFormat.getDelegateClass());
    Assert.assertEquals(ETLDBOutputFormat.MYSQL_SOCKET_FACTORY, outputFormat.getSocketFactory());
  }

  @Test
  public void testRewriteMySqlUrlWithGCloudSocketFactory() throws Exception {
    ETLDBOutputFormat outputFormat = new ETLDBOutputFormat();
    String url = "jdbc:mysql://google/mydb?cloudSqlInstance=project:us-west1:mysql&socketFactory=" +
      "com.google.cloud.sql.mysql.SocketFactory&useSSL=false";
    String expected = "jdbc:mysql://google/mydb?cloudSqlInstance=project:us-west1:mysql&socketFactory=" +
      "io.cdap.socketfactory.mysql.SocketFactory&useSSL=false";
    Assert.assertEquals(expected, outputFormat.rewriteUrl(url));
    Assert.assertEquals("com.google.cloud.sql.mysql.SocketFactory", outputFormat.getDelegateClass());
    Assert.assertEquals(ETLDBOutputFormat.MYSQL_SOCKET_FACTORY, outputFormat.getSocketFactory());
  }

  @Test
  public void testRewriteMySqlUrlWithoutFactory() throws Exception {
    ETLDBOutputFormat outputFormat = new ETLDBOutputFormat();
    String url = "jdbc:mysql://localhost/mydb";
    Assert.assertEquals(url, outputFormat.rewriteUrl(url));
    Assert.assertNull(outputFormat.getDelegateClass());
    Assert.assertEquals(ETLDBOutputFormat.MYSQL_SOCKET_FACTORY, outputFormat.getSocketFactory());
  }

  @Test
  public void testRewritePostgresUrlWithCdapSocketFactory() throws Exception {
    ETLDBOutputFormat outputFormat = new ETLDBOutputFormat();
    String url = "jdbc:postgresql://google/mydb?socketFactory=io.cdap.socketfactory.postgres.SocketFactory";
    Assert.assertEquals(url, outputFormat.rewriteUrl(url));
    Assert.assertNull(outputFormat.getDelegateClass());
    Assert.assertEquals(ETLDBOutputFormat.POSTGRES_SOCKET_FACTORY, outputFormat.getSocketFactory());
  }

  @Test
  public void testRewritePostgresUrlWithGCloudSocketFactory() throws Exception {
    ETLDBOutputFormat outputFormat = new ETLDBOutputFormat();
    String url = "jdbc:postgresql://google/mydb?cloudSqlInstance=project:us-west1:postgres&socketFactory=" +
      "com.google.cloud.sql.postgres.SocketFactory&useSSL=false";
    String expected = "jdbc:postgresql://google/mydb?cloudSqlInstance=project:us-west1:postgres&socketFactory=" +
      "io.cdap.socketfactory.postgres.SocketFactory&useSSL=false";
    Assert.assertEquals(expected, outputFormat.rewriteUrl(url));
    Assert.assertEquals("com.google.cloud.sql.postgres.SocketFactory", outputFormat.getDelegateClass());
    Assert.assertEquals(ETLDBOutputFormat.POSTGRES_SOCKET_FACTORY, outputFormat.getSocketFactory());
  }

  @Test
  public void testRewritePostgresUrlWithoutFactory() throws Exception {
    ETLDBOutputFormat outputFormat = new ETLDBOutputFormat();
    String url = "jdbc:postgresql://localhost/mydb";
    Assert.assertEquals(url, outputFormat.rewriteUrl(url));
    Assert.assertNull(outputFormat.getDelegateClass());
    Assert.assertEquals(ETLDBOutputFormat.POSTGRES_SOCKET_FACTORY, outputFormat.getSocketFactory());
  }

  @Test
  public void testRewriteOracleUrl() throws Exception {
    ETLDBOutputFormat outputFormat = new ETLDBOutputFormat();
    String url = "jdbc:oracle:thin:scott/tiger@//myhost:1521/myservicename";
    Assert.assertEquals(url, outputFormat.rewriteUrl(url));
    Assert.assertNull(outputFormat.getDelegateClass());
    Assert.assertNull(outputFormat.getSocketFactory());
  }
}
