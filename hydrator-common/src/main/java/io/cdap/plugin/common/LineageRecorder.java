/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.InstanceConflictException;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.lineage.field.ReadOperation;
import io.cdap.cdap.api.lineage.field.WriteOperation;
import io.cdap.cdap.etl.api.batch.BatchContext;
import io.cdap.cdap.etl.api.lineage.field.FieldReadOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldWriteOperation;

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
   * Creates an external dataset if a dataset with the given {@link LineageRecorder#dataset} name does not already
   * exists. If a non null schema is provided then the external dataset will have the given schema upon creation.
   *
   * @param schema the schema of the external dataset
   * @throws RuntimeException if the dataset creation fails
   */
  public void createExternalDataset(@Nullable Schema schema) {
    DatasetProperties datasetProperties;
    if (schema == null) {
      datasetProperties = DatasetProperties.EMPTY;
    } else {
      datasetProperties = DatasetProperties.of(Collections.singletonMap(DatasetProperties.SCHEMA, schema.toString()));
    }
    try {
      if (!context.datasetExists(dataset)) {
        // if the dataset does not already exists then create it with the given schema. If it does exists then there is
        // no need to create it.
        context.createDataset(dataset, Constants.EXTERNAL_DATASET_TYPE, datasetProperties);
      }
    } catch (InstanceConflictException e) {
      // This will happen when multiple pipelines run simultaneously and are trying to create the same
      // external dataset. Both might enter the if block after checking for existence and try to create the dataset.
      // One will succeed and another will receive a InstanceConflictException. This exception can be ignored.
      return;
    } catch (DatasetManagementException e) {
      throw new RuntimeException(String.format("Failed to create dataset %s with schema %s.", dataset, schema), e);
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
