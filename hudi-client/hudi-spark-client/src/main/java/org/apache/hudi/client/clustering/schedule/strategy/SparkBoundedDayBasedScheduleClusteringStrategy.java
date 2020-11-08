/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client.clustering.schedule.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkMergeOnReadTable;
import org.apache.hudi.table.action.cluster.strategy.PartitionAwareScheduleClusteringStrategy;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.config.HoodieClusteringConfig.SORT_COLUMNS_PROPERTY;

/**
 * Clustering Strategy based on following.
 * 1) Spark execution engine.
 * 2) Limits amount of data per clustering operation.
 */
public class SparkBoundedDayBasedScheduleClusteringStrategy<T extends HoodieRecordPayload<T>>
    extends PartitionAwareScheduleClusteringStrategy<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {
  private static final Logger LOG = LogManager.getLogger(SparkBoundedDayBasedScheduleClusteringStrategy.class);

  public SparkBoundedDayBasedScheduleClusteringStrategy(HoodieSparkCopyOnWriteTable<T> table,
                                                        HoodieSparkEngineContext engineContext,
                                                        HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  public SparkBoundedDayBasedScheduleClusteringStrategy(HoodieSparkMergeOnReadTable<T> table,
                                                        HoodieSparkEngineContext engineContext,
                                                        HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  protected Stream<HoodieClusteringGroup> buildClusteringGroupsForPartition(String partitionPath, List<FileSlice> fileSlices) {
    List<Pair<List<FileSlice>, Integer>> fileSliceGroups = new ArrayList<>();
    List<FileSlice> currentGroup = new ArrayList<>();
    int totalSizeSoFar = 0;
    for (FileSlice currentSlice : fileSlices) {
      // assume each filegroup size is ~= parquet.max.file.size
      totalSizeSoFar += currentSlice.getBaseFile().isPresent() ? currentSlice.getBaseFile().get().getFileSize() : getWriteConfig().getParquetMaxFileSize();
      // check if max size is reached and create new group, if needed.
      if (totalSizeSoFar >= getWriteConfig().getClusteringMaxBytesInGroup() && !currentGroup.isEmpty()) {
        fileSliceGroups.add(Pair.of(currentGroup, getNumberOfGroups(totalSizeSoFar, getWriteConfig().getClusteringTargetFileMaxBytes())));
        currentGroup = new ArrayList<>();
        totalSizeSoFar = 0;
      }
      currentGroup.add(currentSlice);
    }
    if (!currentGroup.isEmpty()) {
      fileSliceGroups.add(Pair.of(currentGroup, getNumberOfGroups(totalSizeSoFar, getWriteConfig().getClusteringTargetFileMaxBytes())));
    }

    return fileSliceGroups.stream().map(fileSliceGroup -> HoodieClusteringGroup.newBuilder()
        .setSlices(getFileSliceInfo(fileSliceGroup.getLeft()))
        .setNumOutputGroups(fileSliceGroup.getRight())
        .setMetrics(buildMetrics(fileSliceGroup.getLeft()))
        .build());
  }

  @Override
  protected Map<String, String> getStrategyParams() {
    Map<String, String> params = new HashMap<>();
    if (getWriteConfig().getProps().containsKey(SORT_COLUMNS_PROPERTY)) {
      params.put(SORT_COLUMNS_PROPERTY, getWriteConfig().getProps().getProperty(SORT_COLUMNS_PROPERTY));
    }
    return params;
  }

  @Override
  protected List<String> filterPartitionPaths(List<String> partitionPaths) {
    int targetPartitionsForClustering = getWriteConfig().getTargetPartitionsForClustering();
    return partitionPaths.stream().map(partition -> partition.replace("/", "-"))
        .sorted(Comparator.reverseOrder()).map(partitionPath -> partitionPath.replace("-", "/"))
        .limit(targetPartitionsForClustering > 0 ? targetPartitionsForClustering : partitionPaths.size())
        .collect(Collectors.toList());
  }

  private int getNumberOfGroups(long groupSize, long targetFileSize) {
    return (int) Math.ceil(groupSize / (double) targetFileSize);
  }
}
