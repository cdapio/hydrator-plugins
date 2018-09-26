/*
 * Copyright © 2018 Cask Data, Inc.
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
package co.cask.hydrator.common;

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.api.lineage.field.ReadOperation;
import co.cask.cdap.api.lineage.field.WriteOperation;
import co.cask.cdap.etl.api.batch.BatchContext;
import co.cask.cdap.etl.api.lineage.field.FieldReadOperation;
import co.cask.cdap.etl.api.lineage.field.FieldWriteOperation;
import com.google.common.base.Throwables;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A helper class for creating external dataset and recording it's lineage
 */
public class LineageRecorder {

  private final BatchContext context;
  private final String dataset;

  public LineageRecorder(BatchContext context, String dataset) {
    this.context = context;
    this.dataset = dataset;
  }

  /**
   * Creates an external dataset with given schema is not null
   *
   * @param schema the schema of the external dataset
   */
  public void createDataset(@Nullable String schema) {
    if (schema != null) {
      try {
        context.createDataset(dataset, Constants.EXTERNAL_DATASET_TYPE,
                              DatasetProperties.of(Collections.singletonMap(DatasetProperties.SCHEMA, schema)));
      } catch (DatasetManagementException e) {
        Throwables.propagate(e);
      }
    }
  }

  /**
   * Records a {@link ReadOperation}
   *
   * @param operationName the name of the operation
   * @param operationDescription description for the operation
   * @param fields output fields of this read operation
   */
  public void recordRead(String operationName, String operationDescription, List<String> fields) {
    context.record(Collections.singletonList(new FieldReadOperation(operationName,
                                                                    operationDescription,
                                                                    EndPoint.of(context.getNamespace(), dataset),
                                                                    fields)));
  }

  /**
   * Records a {@link WriteOperation}
   *
   * @param operationName the name of the operation
   * @param operationDescription description for the operation
   * @param fields input fields of this read operation
   */
  public void recordWrite(String operationName, String operationDescription, List<String> fields) {
    context.record(Collections.singletonList(new FieldWriteOperation(operationName,
                                                                     operationDescription,
                                                                     EndPoint.of(context.getNamespace(), dataset),
                                                                     fields)));
  }
}
