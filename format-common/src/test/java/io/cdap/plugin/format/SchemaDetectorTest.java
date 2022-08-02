/*
 *  Copyright ©2022 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */
package io.cdap.plugin.format;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.InputFile;
import io.cdap.cdap.etl.api.validation.InputFiles;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Tests for {@link SchemaDetector}
 */
public class SchemaDetectorTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testRegexFiltering() throws IOException {
    File inputDir = TMP_FOLDER.newFolder();

    File usersFile = new File(inputDir, "users.txt");
    try (FileOutputStream fos = new FileOutputStream(usersFile)) {
      fos.write("id,name\n0,alice".getBytes(StandardCharsets.UTF_8));
    }

    File purchasesFile = new File(inputDir, "purchases.txt");
    try (FileOutputStream fos = new FileOutputStream(purchasesFile)) {
      fos.write("ts,item,count\n1234567890,donut,12".getBytes(StandardCharsets.UTF_8));
    }

    FormatContext formatContext = new FormatContext(new MockFailureCollector(), null);
    TrackingFormat inputFormat = new TrackingFormat(inputDir);
    SchemaDetector schemaDetector = new SchemaDetector(inputFormat);

    schemaDetector.detectSchema(inputDir.getAbsolutePath(), Pattern.compile(".*users.*"),
                                formatContext, Collections.emptyMap());
    Iterator<InputFile> inputFiles = inputFormat.inputFiles.iterator();
    Assert.assertEquals(usersFile.getName(), inputFiles.next().getName());
    Assert.assertFalse(inputFiles.hasNext());

    schemaDetector.detectSchema(inputDir.getAbsolutePath(), Pattern.compile(".*purchases.*"),
                                formatContext, Collections.emptyMap());
    inputFiles = inputFormat.inputFiles.iterator();
    Assert.assertEquals(purchasesFile.getName(), inputFiles.next().getName());
    Assert.assertFalse(inputFiles.hasNext());
  }

  /**
   * Just tracks which input files are sent for schema detection
   */
  private static class TrackingFormat implements ValidatingInputFormat {
    private final Map<String, String> conf;
    private InputFiles inputFiles;

    TrackingFormat(File baseDir) {
      this.conf = Collections.singletonMap(FileSystem.FS_DEFAULT_NAME_KEY, baseDir.toURI().toString());
    }

    @Override
    public void validate(FormatContext formatContext) {

    }

    @Nullable
    @Override
    public Schema getSchema(FormatContext formatContext) {
      return null;
    }

    @Override
    public String getInputFormatClassName() {
      return null;
    }

    @Override
    public Map<String, String> getInputFormatConfiguration() {
      return conf;
    }

    @Nullable
    @Override
    public Schema detectSchema(FormatContext context, InputFiles inputFiles) {
      this.inputFiles = inputFiles;
      return null;
    }
  }
}
