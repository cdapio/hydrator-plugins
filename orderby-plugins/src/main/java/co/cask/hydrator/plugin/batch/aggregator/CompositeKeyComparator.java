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

package co.cask.hydrator.plugin.batch.aggregator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Custom sort class to sort the received structured records.
 */
public class CompositeKeyComparator extends WritableComparator {

  public CompositeKeyComparator() {
    super(CompositeKey.class, true);
  }

  @Override
  /**
   * This comparator controls the sort oder of the objects depending on the keys.
   */
  public int compare(WritableComparable wc1, WritableComparable wc2) {

    CompositeKey compositeKey1 = (CompositeKey) wc1;
    CompositeKey compositeKey2 = (CompositeKey) wc2;
    return compositeKey1.compareTo(compositeKey2);
  }
}
