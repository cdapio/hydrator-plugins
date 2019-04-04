/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.api.dataset.lib.PartitionOutput;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetProperties;
import io.cdap.cdap.api.dataset.lib.Partitioning;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.plugin.batch.sink.SnapshotFileBatchSink;
import io.cdap.plugin.dataset.SnapshotFileSet;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for {@link SnapshotFileBatchSink}.
 */
public class SnapshotFileSetTest extends HydratorTestBase {
  @Test
  public void testCleanup() throws Exception {
    addDatasetInstance(PartitionedFileSet.class.getName(), "pfs", PartitionedFileSetProperties.builder()
                         .setPartitioning(Partitioning.builder().addLongField(SnapshotFileSet.SNAPSHOT_FIELD).build())
                         .setInputFormat(AvroKeyInputFormat.class)
                         .setOutputFormat(AvroKeyOutputFormat.class)
                         .build());
    PartitionedFileSet partitionedFileSet = (PartitionedFileSet) getDataset("pfs").get();

    // Create partition to be cleaned up
    PartitionKey deletedPartitionKey = PartitionKey.builder().addLongField(SnapshotFileSet.SNAPSHOT_FIELD,
                                                                           System.currentTimeMillis() -
                                                                             (1000 * 60)).build();
    PartitionOutput partitionToDelete = partitionedFileSet.getPartitionOutput(deletedPartitionKey);
    partitionToDelete.addPartition();

    // Create partition to persist
    PartitionKey persistentPartitionKey = PartitionKey.builder().addLongField(SnapshotFileSet.SNAPSHOT_FIELD,
                                                                              System.currentTimeMillis() -
                                                                                (1000 * 20)).build();
    PartitionOutput partitionToKeep = partitionedFileSet.getPartitionOutput(persistentPartitionKey);
    partitionToKeep.addPartition();

    SnapshotFileSet snapshotFileSet = new SnapshotFileSet(partitionedFileSet);
    snapshotFileSet.deleteMatchingPartitionsByTime(System.currentTimeMillis() - (1000 * 30));

    Assert.assertNull(partitionedFileSet.getPartition(deletedPartitionKey));
    Assert.assertNotNull(partitionedFileSet.getPartition(persistentPartitionKey));
  }
}
