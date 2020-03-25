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

package io.cdap.plugin.aws.sqs.source;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.format.FormatSpecification;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.format.RecordFormats;
import io.cdap.plugin.aws.sqs.common.SqsPluginConfig;
import io.cdap.plugin.aws.sqs.source.util.SqsSourceConstants;
import io.cdap.plugin.aws.sqs.util.Utility;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Configuration for the {@link SqsSource}.
 */
public class SqsSourceConfig extends SqsPluginConfig implements Serializable {
  @Name(SqsSourceConstants.PROPERTY_DELETE)
  @Description("Permanently delete messages from the queue after successfully reading. " +
    "If set to `false`, the messages will stay on queue and will be received again leading to duplicates.")
  @Macro
  private Boolean delete;

  @Name(SqsSourceConstants.PROPERTY_INTERVAL)
  @Description("The amount of time to wait between each poll in seconds. The plugin will wait for the duration " +
          "specified before issuing an receive message call.")
  @Macro
  private final Integer interval;

  @Name(SqsSourceConstants.PROPERTY_WAIT_TIME)
  @Description("The duration (in seconds) for which the call waits for a message to arrive in " +
          "the queue before returning. This will be set as part of the ReceiveMessage API call action. " +
          "The maximum wait time currently allowed by SQS is 20 seconds")
  @Macro
  private final Integer waitTime;

  @Name(SqsSourceConstants.PROPERTY_NUMBER_OF_MESSAGES)
  @Description("The maximum number of messages to return from a single API request. " +
          "The maximum value allowed by SQS is 10.")
  @Macro
  private final Integer numberOfMessages;

  @Name(SqsSourceConstants.PROPERTY_FORMAT)
  @Description(
    "Optional format of the SQS message. Any format supported by CDAP is supported. For "
      + "example, a value of 'csv' will attempt to parse SQS message as comma-separated values. If no format is "
      + "given, SQS messages will be treated as bytes.")
  @Nullable
  private final String format;

  @Name(SqsSourceConstants.PROPERTY_SCHEMA)
  @Description(
    "Output schema of the source, The fields are used in conjunction with the format to parse SQS messages")
  private final String schema;

  /**
   * Constructor for SqsSourceConfig object.
   * @param referenceName The reference name
   * @param authenticationMethod The authentication method to be used
   * @param accessId The AWS access id
   * @param accessKey The AWS access key
   * @param region The AWS region
   * @param queueName The queue name
   * @param sqsEndpoint The SQS Endpoint
   * @param delete The flag to indicate whether to delete message after read or not
   * @param interval The amount of time to wait between each poll in seconds
   * @param waitTime The duration (in seconds) for which the call waits for a message to arrive
   * @param numberOfMessages The maximum number of messages to return
   * @param format The message format
   * @param schema The output schema
   */
  public SqsSourceConfig(String referenceName, String authenticationMethod, @Nullable String accessId,
                         @Nullable String accessKey, String region, String queueName, @Nullable String sqsEndpoint,
                         Boolean delete, Integer interval, Integer waitTime, Integer numberOfMessages,
                         @Nullable String format, String schema) {
    super(referenceName, authenticationMethod, accessId, accessKey, region, queueName, sqsEndpoint);
    this.delete = delete;
    this.interval = interval;
    this.waitTime = waitTime;
    this.numberOfMessages = numberOfMessages;
    this.format = format;
    this.schema = schema;
  }

  public Boolean getDelete() {
    return delete;
  }

  public void setDelete(Boolean delete) {
    this.delete = delete;
  }

  public Integer getInterval() {
    return interval;
  }

  public Integer getWaitTime() {
    return waitTime;
  }

  public Integer getNumberOfMessages() {
    return numberOfMessages;
  }

  @Nullable
  public String getFormat() {
    return Strings.isNullOrEmpty(format) ? null : format;
  }

  public String getSchema() {
    return schema;
  }

  /**
   * Parses the string formatted string and returns the Schema object.
   * @return The instance of Schema object
   */
  public Schema parseSchema() {
    try {
      return Schema.parseJson(schema);
    } catch (IOException | NullPointerException e) {
      throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
    }
  }

  /**
   * Validates {@link SqsSourceConfig} instance.
   *
   * @param collector The failure collector to collect the errors
   */
  public void validate(FailureCollector collector) {
    super.validate(collector);
    validateInterval(collector);
    validateWaitTime(collector);
    validateMaxNumberOfMessages(collector);
    validateSchemaAndFormat(collector);
  }

  private void validateInterval(FailureCollector collector) {
    if (containsMacro(SqsSourceConstants.PROPERTY_INTERVAL)) {
      return;
    }

    if (interval < 0) {
      collector.addFailure(String.format("Invalid interval '%d'.", interval),
        "Ensure the interval is not less than 0")
        .withConfigProperty(SqsSourceConstants.PROPERTY_INTERVAL);
    }
  }

  private void validateWaitTime(FailureCollector collector) {
    if (containsMacro(SqsSourceConstants.PROPERTY_WAIT_TIME)) {
      return;
    }

    if (waitTime < 1 || waitTime > SqsSourceConstants.MAX_WAIT_TIME) {
      collector.addFailure(String.format("Invalid wait time '%d'.", waitTime),
        String.format("Ensure the wait time is 1 or at most '%d'", SqsSourceConstants.MAX_WAIT_TIME))
        .withConfigProperty(SqsSourceConstants.PROPERTY_WAIT_TIME);
    }
  }

  private void validateMaxNumberOfMessages(FailureCollector collector) {
    if (containsMacro(SqsSourceConstants.PROPERTY_NUMBER_OF_MESSAGES)) {
      return;
    }

    if (numberOfMessages < 1 || numberOfMessages > SqsSourceConstants.MAX_MESSAGES_LIMIT) {
      collector.addFailure(String.format("Invalid number of messages '%d'.", numberOfMessages),
        String.format("Ensure the number of messages is at least 1 or at most '%d'",
                      SqsSourceConstants.MAX_MESSAGES_LIMIT))
        .withConfigProperty(SqsSourceConstants.PROPERTY_NUMBER_OF_MESSAGES);
    }
  }

  private Schema validateAndGetSchema(FailureCollector collector) {
    if (Utility.isNullOrEmpty(schema)) {
      collector.addFailure("Schema must be specified.", null)
        .withConfigProperty(SqsSourceConstants.PROPERTY_SCHEMA);
      return null;
    }

    Schema messageSchema = null;
    try {
      messageSchema = parseSchema();

      if (messageSchema.getFields().isEmpty()) {
        collector.addFailure("Schema must be specified.", null)
          .withConfigProperty(SqsSourceConstants.PROPERTY_SCHEMA);
        messageSchema = null;
      }
    } catch (Exception e) {
      collector.addFailure(e.getMessage(), null)
        .withConfigProperty(SqsSourceConstants.PROPERTY_SCHEMA);
      collector.getOrThrowException();
    }

    return messageSchema;
  }

  private void validateSchemaAndFormat(FailureCollector collector) {
    if (containsMacro(SqsSourceConstants.PROPERTY_SCHEMA) || containsMacro(SqsSourceConstants.PROPERTY_FORMAT)) {
      return;
    }

    Schema messageSchema = validateAndGetSchema(collector);
    if (messageSchema == null) {
      return;
    }

    // if format is empty, there must be just a single message field of type bytes or nullable
    // types.
    if (Utility.isNullOrEmpty(format)) {
      List<Schema.Field> messageFields = messageSchema.getFields();
      if (messageFields.size() > 1) {
        List<String> fieldNames = messageFields.stream().map(o -> o.getName()).collect(Collectors.toList());
        collector.addFailure(String.format("Without a format, the schema must contain just a single message field " +
            "of type bytes or nullable bytes. Found %d message fields (%s).", messageFields.size(),
          Joiner.on(',').join(fieldNames)), null)
          .withConfigProperty(SqsSourceConstants.PROPERTY_SCHEMA);
        return;
      }
      Schema.Field messageField = messageFields.get(0);
      Schema messageFieldSchema = messageField.getSchema();
      Schema.Type messageFieldType =
        messageFieldSchema.isNullable()
          ? messageFieldSchema.getNonNullable().getType()
          : messageFieldSchema.getType();
      if (messageFieldType != Schema.Type.BYTES) {
        collector.addFailure(String.format("Without a format, the message field must be of type bytes or nullable " +
          "bytes, but field %s is of type %s.", messageField.getName(), messageField.getSchema()), null)
          .withConfigProperty(SqsSourceConstants.PROPERTY_SCHEMA);
        return;
      }
    } else {
      // otherwise, if there is a format, make sure we can instantiate it.
      FormatSpecification formatSpec =
        new FormatSpecification(format, messageSchema, new HashMap<String, String>());

      try {
        RecordFormats.createInitializedFormat(formatSpec);
      } catch (Exception e) {
        collector.addFailure(String.format("Current schema (%s) does not match the format expected for '%s': %s",
          messageSchema, format, e.getMessage()), null)
          .withConfigProperty(SqsSourceConstants.PROPERTY_FORMAT)
          .withConfigProperty(SqsSourceConstants.PROPERTY_SCHEMA);
        return;
      }
    }
  }
}
