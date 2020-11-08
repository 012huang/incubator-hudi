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

package org.apache.hudi.config;

import org.apache.hudi.common.config.DefaultHoodieConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Clustering specific configs.
 */
public class HoodieClusteringConfig extends DefaultHoodieConfig {

  public static final String ASYNC_CLUSTERING_ENABLED = "hoodie.clustering.enabled";
  public static final String DEFAULT_ASYNC_CLUSTERING_ENABLED = "false";

  public static final String SCHEDULE_CLUSTERING_STRATEGY_CLASS = "hoodie.clustering.schedule.strategy.class";
  public static final String DEFAULT_SCHEDULE_CLUSTERING_STRATEGY_CLASS =
      "org.apache.hudi.client.clustering.schedule.strategy.SparkBoundedDayBasedScheduleClusteringStrategy";

  public static final String RUN_CLUSTERING_STRATEGY_CLASS = "hoodie.clustering.run.strategy.class";
  public static final String DEFAULT_RUN_CLUSTERING_STRATEGY_CLASS =
      "org.apache.hudi.client.clustering.run.strategy.SparkBulkInsertBasedRunClusteringStrategy";

  // Turn on inline clustering - after few commits an inline clustering will be run
  public static final String INLINE_CLUSTERING_PROP = "hoodie.clustering.inline";
  private static final String DEFAULT_INLINE_CLUSTERING = "false";

  public static final String INLINE_CLUSTERING_NUM_COMMIT_PROP = "hoodie.clustering.inline.num.commits";
  private static final String DEFAULT_INLINE_CLUSTERING_NUM_COMMITS = "4";

  public static final String CLUSTERING_TARGET_PARTITIONS = "hoodie.clustering.target.partitions";
  public static final String DEFAULT_CLUSTERING_TARGET_PARTITIONS = String.valueOf(2);

  // Each clustering operation can create multiple groups. Total amount of data processed by clustering operation
  // is defined by below two properties (CLUSTERING_MAX_GROUP_SIZE * CLUSTERING_MAX_NUM_GROUPS).
  // Max amount of data to be included in one group
  public static final String CLUSTERING_MAX_BYTES_IN_GROUP = "hoodie.clustering.max.bytes.group";
  public static final String DEFAULT_CLUSTERING_MAX_GROUP_SIZE = String.valueOf(2 * 1024 * 1024 * 1024L);

  public static final String CLUSTERING_MAX_NUM_GROUPS = "hoodie.clustering.max.num.groups";
  public static final String DEFAULT_CLUSTERING_MAX_NUM_GROUPS = "1";

  // Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups.
  public static final String CLUSTERING_TARGET_FILE_MAX_BYTES = "hoodie.clustering.target.file.max.bytes";
  public static final String DEFAULT_CLUSTERING_TARGET_FILE_MAX_BYTES = String.valueOf(1 * 1024 * 1024 * 1024L);

  // Any strategy specific params can be saved with this prefix
  public static final String CLUSTERING_STRATEGY_PARAM_PREFIX = "hoodie.clustering.strategy.param.";

  // constants related to clustering that may be used by more than 1 strategy.
  public static final String SORT_COLUMNS_PROPERTY = HoodieClusteringConfig.CLUSTERING_STRATEGY_PARAM_PREFIX + "sort.columns";

  public HoodieClusteringConfig(Properties props) {
    super(props);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final Properties props = new Properties();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.props.load(reader);
        return this;
      }
    }

    public Builder withScheduleClusteringStrategyClass(String clusteringStrategyClass) {
      props.setProperty(SCHEDULE_CLUSTERING_STRATEGY_CLASS, clusteringStrategyClass);
      return this;
    }

    public Builder withRunClusteringStrategyClass(String runClusteringStrategyClass) {
      props.setProperty(RUN_CLUSTERING_STRATEGY_CLASS, runClusteringStrategyClass);
      return this;
    }

    public Builder withClusteringTargetPartitions(int clusteringTargetPartitions) {
      props.setProperty(CLUSTERING_TARGET_PARTITIONS, String.valueOf(clusteringTargetPartitions));
      return this;
    }

    public Builder withClusteringMaxBytesInGroup(long clusteringMaxGroupSize) {
      props.setProperty(CLUSTERING_MAX_BYTES_IN_GROUP, String.valueOf(clusteringMaxGroupSize));
      return this;
    }

    public Builder withClusteringMaxNumGroups(int maxNumGroups) {
      props.setProperty(CLUSTERING_MAX_NUM_GROUPS, String.valueOf(maxNumGroups));
      return this;
    }

    public Builder withClusteringTargetFileMaxBytes(long targetFileSize) {
      props.setProperty(CLUSTERING_TARGET_FILE_MAX_BYTES, String.valueOf(targetFileSize));
      return this;
    }

    public Builder withInlineClustering(Boolean inlineCompaction) {
      props.setProperty(INLINE_CLUSTERING_PROP, String.valueOf(inlineCompaction));
      return this;
    }

    public Builder withInlineClusteringNumCommits(int numCommits) {
      props.setProperty(INLINE_CLUSTERING_NUM_COMMIT_PROP, String.valueOf(numCommits));
      return this;
    }

    public Builder withAsyncClusteringEnabled(Boolean clusteringEnabled) {
      props.setProperty(ASYNC_CLUSTERING_ENABLED, String.valueOf(clusteringEnabled));
      return this;
    }

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public HoodieClusteringConfig build() {
      HoodieClusteringConfig config = new HoodieClusteringConfig(props);
      setDefaultOnCondition(props, !props.containsKey(ASYNC_CLUSTERING_ENABLED),
          ASYNC_CLUSTERING_ENABLED, DEFAULT_ASYNC_CLUSTERING_ENABLED);
      setDefaultOnCondition(props, !props.containsKey(SCHEDULE_CLUSTERING_STRATEGY_CLASS),
          SCHEDULE_CLUSTERING_STRATEGY_CLASS, DEFAULT_SCHEDULE_CLUSTERING_STRATEGY_CLASS);
      setDefaultOnCondition(props, !props.containsKey(RUN_CLUSTERING_STRATEGY_CLASS),
          RUN_CLUSTERING_STRATEGY_CLASS, DEFAULT_RUN_CLUSTERING_STRATEGY_CLASS);
      setDefaultOnCondition(props, !props.containsKey(CLUSTERING_MAX_BYTES_IN_GROUP), CLUSTERING_MAX_BYTES_IN_GROUP,
          DEFAULT_CLUSTERING_MAX_GROUP_SIZE);
      setDefaultOnCondition(props, !props.containsKey(CLUSTERING_MAX_NUM_GROUPS), CLUSTERING_MAX_NUM_GROUPS,
          DEFAULT_CLUSTERING_MAX_NUM_GROUPS);
      setDefaultOnCondition(props, !props.containsKey(CLUSTERING_TARGET_FILE_MAX_BYTES), CLUSTERING_TARGET_FILE_MAX_BYTES,
          DEFAULT_CLUSTERING_TARGET_FILE_MAX_BYTES);
      setDefaultOnCondition(props, !props.containsKey(INLINE_CLUSTERING_PROP), INLINE_CLUSTERING_PROP,
          DEFAULT_INLINE_CLUSTERING);
      setDefaultOnCondition(props, !props.containsKey(INLINE_CLUSTERING_NUM_COMMIT_PROP), INLINE_CLUSTERING_NUM_COMMIT_PROP,
          DEFAULT_INLINE_CLUSTERING_NUM_COMMITS);
      setDefaultOnCondition(props, !props.containsKey(CLUSTERING_TARGET_PARTITIONS), CLUSTERING_TARGET_PARTITIONS,
          DEFAULT_CLUSTERING_TARGET_PARTITIONS);
      return config;
    }
  }
}
