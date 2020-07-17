<!--- 
 * Copyright © 2020 Cask Data, Inc.
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
 -->
 
# Data Cacher

Description
-----------
Data cacher plugin will cache any record that is passed through it. This is 
useful for pipelines that have auto-caching disabled but would still like to cache 
records at certain points in the pipeline. Spark caching prevents unnecessary
recomputation of previous stages, this is particularly helpful for pipelines with 
branching paths. 

Properties
----------
**storageLevel:** This determines the method in which the data will be cached. The allowed values are:
1. `DISK_ONLY`: Save data to disk
1. `DISK_ONLY_2`: Save data to disk, replicated 2 times
1. `MEMORY_ONLY`*: Save data to memory in raw java objects
1. `MEMORY_ONLY_2`*: Save data to memory in raw java objects, replicated 2 times
1. `MEMORY_ONLY_SER`*: Save data to memory in serialized objects 
1. `MEMORY_ONLY_SER_2`*: Save data to memory in serialized objects, replicated 2 times
1. `MEMORY_AND_DISK`: Save data to memory in raw java objects, if memory is not large enough then spill to disk
1. `MEMORY_AND_DISK_2`: Save data to memory in raw java objects, if memory is not large enough then spill to disk, replicated 2 times
1. `MEMORY_AND_DISK_SER`: Save data to memory in serialized objects, if memory is not large enough then spill to disk
1. `MEMORY_AND_DISK_SER_2`: Save data to memory in serialized objects, if memory is not large enough then spill to disk, replicated 2 times

**\*Caching data in memory only can cause OutOfMemory errors if the data is large** 

