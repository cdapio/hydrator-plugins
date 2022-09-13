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

package io.cdap.plugin.common;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for reference name util
 */
public class ReferenceNamesTest {

  @Test
  public void testValidation() {
    // valid names
    ReferenceNames.validate("111-22-33.csv");
    ReferenceNames.validate("abc$2.txt");
    ReferenceNames.validate("1$-2.random");

    // invalid names
    testInValidReferenceName("111-22-33(1).csv");
    testInValidReferenceName("1*!.csv");
    testInValidReferenceName("!@#$%^&");
  }

  @Test
  public void testCleanse() {
    // valid names
    Assert.assertEquals("111-22-33.csv", ReferenceNames.cleanseReferenceName("111-22-33.csv"));
    Assert.assertEquals("abc$2.txt", ReferenceNames.cleanseReferenceName("abc$2.txt"));
    Assert.assertEquals("1$-2.random", ReferenceNames.cleanseReferenceName("1$-2.random"));

    // invalid names
    Assert.assertEquals("111-22-331.csv", ReferenceNames.cleanseReferenceName("111-22-33(1).csv"));
    Assert.assertEquals("1.csv", ReferenceNames.cleanseReferenceName("1*!.csv"));
    Assert.assertEquals("$", ReferenceNames.cleanseReferenceName("!@#$%^&"));

    // invalid name with no valid characters
    Assert.assertEquals("sample", ReferenceNames.cleanseReferenceName("!@#%^&*()"));
  }

  private void testInValidReferenceName(String name) {
    try {
      ReferenceNames.validate(name);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testNormalizeFqnForIndividualChars() {
    // chars that are retained
    Assert.assertEquals("$", ReferenceNames.normalizeFqn("$"));
    Assert.assertEquals("-", ReferenceNames.normalizeFqn("-"));
    Assert.assertEquals("_", ReferenceNames.normalizeFqn("_"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("."));

    // chars that are replaced
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("!"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("@"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("#"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("%"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("^"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("&"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("*"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("("));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn(")"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("["));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("]"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("{"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("}"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("`"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("~"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn(":"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn(";"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("\""));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("|"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("<"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn(">"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("+"));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("="));
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("\\"));
  }

  @Test
  public void testNormalizeFqn() {
    // valid strings
    Assert.assertEquals("111-22-33.csv", ReferenceNames.normalizeFqn("111-22-33.csv"));
    Assert.assertEquals("abc$2.txt", ReferenceNames.normalizeFqn("abc$2.txt"));
    Assert.assertEquals("1$-2.random", ReferenceNames.normalizeFqn("1$-2.random"));

    // FQNs
    Assert.assertEquals("bigquery.myProject.myDataset.myTable",
                        ReferenceNames.normalizeFqn("bigquery:myProject.myDataset.myTable"));
    Assert.assertEquals("dataplex.myProject.us.myLake.myZone.myTable",
                        ReferenceNames.normalizeFqn("dataplex:myProject.us.myLake.myZone.myTable"));
    Assert.assertEquals("gs.myBucket.myFolder._SUCCESS",
                        ReferenceNames.normalizeFqn("gs://myBucket/myFolder/_SUCCESS"));
    Assert.assertEquals("cloudsql.myProject.myLocation.myInstance.mySchema.myTable",
                        ReferenceNames.normalizeFqn("cloudsql:myProject.myLocation.myInstance.mySchema.myTable"));
    Assert.assertEquals("oracle.myhost.1521.mydb.mytable",
                        ReferenceNames.normalizeFqn("oracle://myhost:1521/mydb.mytable"));
    Assert.assertEquals("ftp.host.port.path", ReferenceNames.normalizeFqn("ftp://host:port/path"));
    Assert.assertEquals("sap.odata.10.132.0.30.myservice.myentity",
                        ReferenceNames.normalizeFqn("sap:odata//10.132.0.30.myservice.myentity"));
    Assert.assertEquals("s3.mybucket.mypathfolder.mypathsubfolder",
                        ReferenceNames.normalizeFqn("s3://mybucket/mypathfolder/mypathsubfolder"));

    // test groups of chars
    Assert.assertEquals("111-22-33.1..csv", ReferenceNames.normalizeFqn("111-22-33(1).csv"));
    Assert.assertEquals("1..csv", ReferenceNames.normalizeFqn("1*!.csv"));
    Assert.assertEquals(".$.", ReferenceNames.normalizeFqn("!@#$%^&"));

    // string with no valid characters
    Assert.assertEquals(".", ReferenceNames.normalizeFqn("!@#%^&*()"));
  }
}
