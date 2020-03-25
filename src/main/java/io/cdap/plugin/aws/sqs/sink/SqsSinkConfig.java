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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.aws.sqs.common.SqsPluginConfig;
import io.cdap.plugin.aws.sqs.sink.util.SinkMessageFormat;
import io.cdap.plugin.aws.sqs.sink.util.SqsSinkConstants;

import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Configuration for the {@link SqsSink}.
 */
public class SqsSinkConfig extends SqsPluginConfig {
  @Name(SqsSinkConstants.PROPERTY_MESSAGE_FORMAT)
  @Description("Specifies the format of the message published to SQS. " +
    "Supported formats are csv and json. Default value is json.")
  @Macro
  private String messageFormat;

  @Name(SqsSinkConstants.PROPERTY_DELAY_SECONDS)
  @Description("The time, in seconds, for which a specific message is delayed." +
    "The minimum value is `0` and maximum value is `900`")
  @Macro
  private int delaySeconds;

  /**
   * Constructor for SqsSinkConfig object.
   * @param referenceName The reference name
   * @param authenticationMethod The authentication method to be used
   * @param accessId The AWS access id
   * @param accessKey The AWS access key
   * @param region The AWS region
   * @param queueName The queue name
   * @param sqsEndpoint The SQS Endpoint
   * @param messageFormat The message format
   * @param delaySeconds The delay value in seconds
   */
  public SqsSinkConfig(String referenceName, String authenticationMethod, @Nullable String accessId,
                       @Nullable String accessKey, String region, String queueName, @Nullable String sqsEndpoint,
                       String messageFormat, int delaySeconds) {
    super(referenceName, authenticationMethod, accessId, accessKey, region, queueName, sqsEndpoint);
    this.messageFormat = messageFormat;
    this.delaySeconds = delaySeconds;
  }

  /**
   * Returns the message format chosen.
   *
   * @param collector The failure collector to collect the errors
   * @return An instance of SinkMessageFormat
   */
  public SinkMessageFormat getMessageFormat(FailureCollector collector) {
    SinkMessageFormat format = getMessageFormat();
    if (format != null) {
      return format;
    }

    collector.addFailure("Unsupported format value: " + messageFormat,
      String.format("Supported formats are: %s", SinkMessageFormat.getSupportedFormats()))
      .withConfigProperty(SqsSinkConstants.PROPERTY_MESSAGE_FORMAT);
    return null;
  }

  /**
   * Returns the message format chosen.
   *
   * @return An instance of SinkMessageFormat
   */
  public SinkMessageFormat getMessageFormat() {
    Optional<SinkMessageFormat> sinkMessageFormat = SinkMessageFormat.fromValue(messageFormat);

    return sinkMessageFormat.isPresent() ? sinkMessageFormat.get() : null;
  }

  public int getDelaySeconds() {
    return delaySeconds;
  }

  /**
   * Validates {@link SqsSinkConfig} instance.
   *
   * @param collector The failure collector to collect the errors
   */
  public void validate(FailureCollector collector) {
    super.validate(collector);
    validateMessageFormat(collector);
    validateDelay(collector);
  }

  private void validateMessageFormat(FailureCollector collector) {
    if (containsMacro(SqsSinkConstants.PROPERTY_MESSAGE_FORMAT)) {
      return;
    }

    getMessageFormat(collector);
  }

  private void validateDelay(FailureCollector collector) {
    if (containsMacro(SqsSinkConstants.PROPERTY_DELAY_SECONDS)) {
      return;
    }

    if (delaySeconds < 0 || delaySeconds > SqsSinkConstants.MAX_DELAY) {
      collector.addFailure(String.format("Invalid delay '%d'.", delaySeconds),
        String.format("Ensure the delay is 0 or at most '%d'", SqsSinkConstants.MAX_DELAY))
        .withConfigProperty(SqsSinkConstants.PROPERTY_DELAY_SECONDS);
    }
  }
}
