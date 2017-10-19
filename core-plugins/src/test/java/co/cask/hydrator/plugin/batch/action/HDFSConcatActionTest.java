/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.hydrator.plugin.batch.action;


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class HDFSConcatActionTest {
  @Test
  public void testFileSortOrder() {
    List<FileStatus> fileStatusList = new ArrayList();
    for (int i = 12; i > 5; i--) {
      FileStatus fs1 = new FileStatus();
      fs1.setPath(new Path(String.format("%s%s.avro", "/path1/part-m-00", i)));
      fileStatusList.add(fs1);

      FileStatus fs2 = new FileStatus();
      fs2.setPath(new Path(String.format("%s%s.avro", "/path1/part-r-00", i)));
      fileStatusList.add(fs2);
    }

    Collections.sort(fileStatusList, new HDFSConcatAction(null).new FileStatusComparator());

    Iterator<FileStatus> fileStatusIterator = fileStatusList.iterator();
    for (int i = 6; i <= 12; i++) {
      Assert.assertEquals(String.format("%s%s.avro", "part-m-00", i), fileStatusIterator.next().getPath().getName());
    }

    for (int i = 6; i <= 12; i++) {
      Assert.assertEquals(String.format("%s%s.avro", "part-r-00", i), fileStatusIterator.next().getPath().getName());
    }
  }
}
