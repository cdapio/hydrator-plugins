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
 *
 */

package io.cdap.plugin.format.connector;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for file type detector
 */
public class FileTypeDetectorTest {

  @Test
  public void testDetectFileType() {
    Assert.assertEquals("application/json", FileTypeDetector.detectFileType("/tmp/john/work/pipeline.json"));
    Assert.assertEquals("application/json", FileTypeDetector.detectFileType("/tmp/john/work/pipeline.json///"));
    Assert.assertEquals("application/xml", FileTypeDetector.detectFileType("/a/b/c/d/e/a.txt/b.xml"));
    Assert.assertEquals("application/avro", FileTypeDetector.detectFileType("data.avro"));
    Assert.assertEquals("application/protobuf", FileTypeDetector.detectFileType("a/d/1.pb"));
    Assert.assertEquals("application/excel", FileTypeDetector.detectFileType("/work/sheet.xlsx"));
    Assert.assertEquals("text/plain", FileTypeDetector.detectFileType("simple.txt"));
    Assert.assertEquals("text/plain", FileTypeDetector.detectFileType("simple.txt/"));
    // if no extension, the file type should be text
    Assert.assertEquals("text/plain", FileTypeDetector.detectFileType("text"));
    Assert.assertEquals("text/plain", FileTypeDetector.detectFileType("a/b/c/noext"));
    Assert.assertEquals("text/plain", FileTypeDetector.detectFileType("a/b/c/noext///"));
    // if no match, unknown should return
    Assert.assertEquals("unknown", FileTypeDetector.detectFileType("a/c/d/file.nomatch"));
  }
}
