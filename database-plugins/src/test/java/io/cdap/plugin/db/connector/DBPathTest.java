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

package io.cdap.plugin.db.connector;

import org.junit.Assert;
import org.junit.Test;

public class DBPathTest {

  @Test
  public void testValidPath() {
    testValidPath(true);
    testValidPath(false);
  }

  private void testValidPath(boolean supportSchema) {
    //empty path
    DBPath path = new DBPath("", supportSchema);
    Assert.assertNull(path.getDatabase());
    Assert.assertNull(path.getSchema());
    Assert.assertNull(path.getTable());

    //root path
    path = new DBPath("/", supportSchema);
    Assert.assertNull(path.getDatabase());
    Assert.assertNull(path.getSchema());
    Assert.assertNull(path.getTable());

    //database path
    path = new DBPath("/database", supportSchema);
    Assert.assertEquals("database", path.getDatabase());
    Assert.assertNull(path.getSchema());
    Assert.assertNull(path.getTable());

    //database path
    path = new DBPath("/database/", supportSchema);
    Assert.assertEquals("database", path.getDatabase());
    Assert.assertNull(path.getSchema());
    Assert.assertNull(path.getTable());

    if (supportSchema) {
      //schema path
      path = new DBPath("/database/schema", supportSchema);
      Assert.assertEquals("database", path.getDatabase());
      Assert.assertEquals("schema", path.getSchema());
      Assert.assertNull(path.getTable());

      //schema path
      path = new DBPath("/database/schema/", supportSchema);
      Assert.assertEquals("database", path.getDatabase());
      Assert.assertEquals("schema", path.getSchema());
      Assert.assertNull(path.getTable());

      //table path
      path = new DBPath("/database/schema/table", supportSchema);
      Assert.assertEquals("database", path.getDatabase());
      Assert.assertEquals("schema", path.getSchema());
      Assert.assertEquals("table", path.getTable());

      //table path
      path = new DBPath("/database/schema/table/", supportSchema);
      Assert.assertEquals("database", path.getDatabase());
      Assert.assertEquals("schema", path.getSchema());
      Assert.assertEquals("table", path.getTable());
    } else {
      //table path
      path = new DBPath("/database/table", supportSchema);
      Assert.assertEquals("database", path.getDatabase());
      Assert.assertNull(path.getSchema());
      Assert.assertEquals("table", path.getTable());

      //table path
      path = new DBPath("/database/table/", supportSchema);
      Assert.assertEquals("database", path.getDatabase());
      Assert.assertNull(path.getSchema());
      Assert.assertEquals("table", path.getTable());
    }
  }


  @Test
  public void testInvalidPath() {
    testInvalidPath(true);
    testInvalidPath(false);
  }

  private void testInvalidPath(boolean supportSchema) {
    //null path
    Assert
      .assertThrows("Path should not be null.", IllegalArgumentException.class, () -> new DBPath(null, supportSchema));

    //more than maximum parts in the path
    if (supportSchema) {
      Assert.assertThrows("Path should not contain more than 3 parts.", IllegalArgumentException.class,
        () -> new DBPath("/a/b/c/d", supportSchema));
    } else {
      Assert.assertThrows("Path should not contain more than 2 parts.", IllegalArgumentException.class,
        () -> new DBPath("/a/b/c", supportSchema));
    }

    //empty database
    Assert.assertThrows("Database should not be empty.", IllegalArgumentException.class,
      () -> new DBPath("//", supportSchema));

    //empty schema or table
    if (supportSchema) {
      Assert.assertThrows("Schema should not be empty.", IllegalArgumentException.class,
        () -> new DBPath("database//", supportSchema));
      Assert.assertThrows("Schema should not be empty.", IllegalArgumentException.class,
        () -> new DBPath("database//table", supportSchema));

      Assert.assertThrows("Table should not be empty.", IllegalArgumentException.class,
        () -> new DBPath("database/schema" + "//", supportSchema));
    } else {
      Assert.assertThrows("Table should not be empty.", IllegalArgumentException.class,
        () -> new DBPath("database//", supportSchema));

    }
  }
}
