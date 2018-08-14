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

package co.cask.hydrator.plugin.batch;

import co.cask.cdap.common.test.TestSuite;
import co.cask.hydrator.plugin.batch.aggregator.DedupTestRun;
import co.cask.hydrator.plugin.batch.aggregator.GroupByTestRun;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * This is a test suite that runs all tests in for ETL batch. This avoids starting/stopping the unit-test framework
 * for every test class.
 */
@RunWith(TestSuite.class)
@Suite.SuiteClasses({
  ETLSnapshotTestRun.class,
  ETLStreamConversionTestRun.class,
  ETLTPFSTestRun.class,
  ETLMapReduceTestRun.class,
  GroupByTestRun.class,
  DedupTestRun.class
// TODO: CDAP-12368
//  ETLFTPTestRun.class,
//  EmailActionTestRun.class,
//  FileActionTestRun.class
})
public class BatchPluginsTestSuite extends ETLBatchTestBase {
}
