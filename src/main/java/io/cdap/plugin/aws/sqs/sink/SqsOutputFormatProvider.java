/*
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
 */

package io.cdap.plugin.aws.sqs.sink;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.plugin.aws.sqs.sink.util.SqsSinkConstants;
import io.cdap.plugin.aws.sqs.util.SqsConstants;
import io.cdap.plugin.aws.sqs.util.Utility;

import java.util.Map;

/**
 * Provides SqsOutputFormat's class name and configuration.
 */
public class SqsOutputFormatProvider implements OutputFormatProvider {
  private final Map<String, String> configMap;

  /**
   * Gets properties from {@link SqsSink} and stores them as properties in map
   * for {@link SqsRecordWriter}.
   *
   * @param sqsSinkConfig plugin config
   */
  SqsOutputFormatProvider(SqsSinkConfig sqsSinkConfig) {
    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>()
      .put(SqsConstants.PROPERTY_AUTH_METHOD, sqsSinkConfig.getAuthenticationMethod().getValue())
      .put(SqsConstants.PROPERTY_ACCESS_ID, sqsSinkConfig.getAccessId())
      .put(SqsConstants.PROPERTY_ACCESS_KEY, sqsSinkConfig.getAccessKey())
      .put(SqsConstants.PROPERTY_QUEUE_NAME, sqsSinkConfig.getQueueName())
      .put(SqsSinkConstants.PROPERTY_DELAY_SECONDS, String.valueOf(sqsSinkConfig.getDelaySeconds()))
      .put(SqsConstants.PROPERTY_REGION, sqsSinkConfig.getRegion().getName());

    if (!Utility.isNullOrEmpty(sqsSinkConfig.getSqsEndpoint())) {
      builder.put(SqsConstants.PROPERTY_SQS_ENDPOINT, sqsSinkConfig.getSqsEndpoint());
    }
    this.configMap = builder.build();
  }

  @Override
  public String getOutputFormatClassName() {
    return SqsOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return configMap;
  }
}
