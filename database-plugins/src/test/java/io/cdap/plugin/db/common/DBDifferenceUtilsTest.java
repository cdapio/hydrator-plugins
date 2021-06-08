/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.db.common;

import org.junit.Assert;
import org.junit.Test;

public class DBDifferenceUtilsTest {

  @Test
  public void testGetTableQueryWithLimit() {
    //Oracle
    Assert.assertEquals("SELECT * FROM schema.table WHERE ROWNUM = 1",
      DBDifferenceUtils.getTableQueryWithLimit(DBDifferenceUtils.DB_PRODUCT_NAME_ORACLE, "schema", "table", 1));

    //Postgres
    Assert.assertEquals("SELECT * FROM schema.table LIMIT 1",
      DBDifferenceUtils.getTableQueryWithLimit(DBDifferenceUtils.DB_PRODUCT_NAME_POSTGRESQL, "schema", "table", 1));

    //MySQL
    Assert.assertEquals("SELECT * FROM table LIMIT 1",
      DBDifferenceUtils.getTableQueryWithLimit(DBDifferenceUtils.DB_PRODUCT_NAME_POSTGRESQL, null, "table", 1));

    //SQLServer
    Assert.assertEquals("SELECT TOP(1) * FROM schema.table",
      DBDifferenceUtils.getTableQueryWithLimit(DBDifferenceUtils.DB_PRODUCT_NAME_ORACLE, "schema", "table", 1));
  }

  @Test
  public void testGetDatabasesQuery() {
    Assert.assertEquals("SELECT NAME AS TABLE_CAT FROM V$DATABASE", DBDifferenceUtils.getOracleDatabasesQuery());
    Assert.assertEquals("SELECT datname AS TABLE_CAT FROM pg_database WHERE datistemplate = false;",
      DBDifferenceUtils.getPostgreDatabasesQuery());
  }
}
