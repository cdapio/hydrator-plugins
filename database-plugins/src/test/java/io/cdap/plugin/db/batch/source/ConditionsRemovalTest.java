/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.db.batch.source;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link DBSource#removeConditionsClause(String)}
 */
public class ConditionsRemovalTest {

  @Test
  public void testCasePreserved() {
    Assert.assertEquals("SeLecT * from mY_TAble",
                        DBSource.removeConditionsClause("SeLecT * from mY_TAble where $CONDITIONS"));
  }

  @Test
  public void testAndConditions() {
    Assert.assertEquals("select * from my_table where id > 3",
                        DBSource.removeConditionsClause("select * from my_table where id > 3 and $CONDITIONS"));
  }

  @Test
  public void testConditionsAnd() {
    Assert.assertEquals("select * from my_table where id > 3",
                        DBSource.removeConditionsClause("select * from my_table where $CONDITIONS and id > 3"));
  }

  @Test
  public void testConditionsInMiddleAnd() {
    Assert.assertEquals(
      "select * from my_table where id > 3 and id < 10",
      DBSource.removeConditionsClause("select * from my_table where id > 3 and $CONDITIONS and id < 10"));
  }

  @Test
  public void testConditionsInMiddleOr() {
    Assert.assertEquals(
      "select * from my_table where id > 3 or id < 10",
      DBSource.removeConditionsClause("select * from my_table where id > 3 or $CONDITIONS or id < 10"));
  }
}
