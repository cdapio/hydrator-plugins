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

package io.cdap.plugin.format.text.input;

import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * A {@link CombineFileSplit} that also contains the header for the files in the split. This must extend
 * CombineFileSplit because {@link CombineFileRecordReader} expects a CombineFileSplit.
 */
public class CombineHeaderFileSplit extends CombineFileSplit {
  private String header;

  public CombineHeaderFileSplit() {
    // exists for Hadoop deserialization
  }

  public CombineHeaderFileSplit(CombineFileSplit split, @Nullable String header) throws IOException {
    super(split.getPaths(), split.getStartOffsets(), split.getLengths(), split.getLocations());
    this.header = header;
  }

  @Nullable
  public String getHeader() {
    return header;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeBoolean(header != null);
    if (header != null) {
      out.writeUTF(header);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    if (in.readBoolean()) {
      header = in.readUTF();
    }
  }

}
