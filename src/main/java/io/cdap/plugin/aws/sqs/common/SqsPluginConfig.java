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

package io.cdap.plugin.aws.sqs.common;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.aws.sqs.exception.SqsInitializationException;
import io.cdap.plugin.aws.sqs.util.SqsAuthMethod;
import io.cdap.plugin.aws.sqs.util.SqsConstants;
import io.cdap.plugin.aws.sqs.util.SqsUtil;
import io.cdap.plugin.aws.sqs.util.Utility;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.common.ReferencePluginConfig;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Common Configuration for the SQS Plugin.
 */
public abstract class SqsPluginConfig extends ReferencePluginConfig {
  @Name(SqsConstants.PROPERTY_AUTH_METHOD)
  @Description("Authentication method to access SQS. The default value is Access Credentials. IAM can only be used " +
    "if the plugin is run in an AWS environment, such as on EMR.")
  @Macro
  private String authenticationMethod;

  @Name(SqsConstants.PROPERTY_ACCESS_ID)
  @Description("Amazon access ID required for authentication.")
  @Nullable
  @Macro
  private String accessId;

  @Name(SqsConstants.PROPERTY_ACCESS_KEY)
  @Description("Amazon access key required for authentication.")
  @Nullable
  @Macro
  private String accessKey;

  @Name(SqsConstants.PROPERTY_REGION)
  @Description("The Amazon Region where the SQS queue is located.")
  @Macro
  private String region;

  @Name(SqsConstants.PROPERTY_QUEUE_NAME)
  @Description("The name of SQS queue to read messages from the plugin")
  @Macro
  private String queueName;

  @Name(SqsConstants.PROPERTY_SQS_ENDPOINT)
  @Description("The endpoint of the SQS server to connect to. For example, https://sqs.us-east-1.amazonaws.com")
  @Nullable
  @Macro
  private String sqsEndpoint;

  /**
   * Constructor for SqsPluginConfig object.
   * @param referenceName The reference name
   * @param authenticationMethod The authentication method to be used
   * @param accessId The AWS access id
   * @param accessKey The AWS access key
   * @param region The AWS region
   * @param queueName The queue name
   * @param sqsEndpoint The SQS Endpoint
   */
  public SqsPluginConfig(String referenceName, String authenticationMethod, @Nullable String accessId,
                         @Nullable String accessKey, String region, String queueName, @Nullable String sqsEndpoint) {
    super(referenceName);
    this.authenticationMethod = authenticationMethod;
    this.accessId = accessId;
    this.accessKey = accessKey;
    this.region = region;
    this.queueName = queueName;
    this.sqsEndpoint = sqsEndpoint;
  }

  /**
   * Returns the auth method chosen.
   *
   * @param collector The failure collector to collect the errors
   * @return An instance of SqsAuthMethod
   */
  public SqsAuthMethod getAuthenticationMethod(FailureCollector collector) {
    SqsAuthMethod authMethod = getAuthenticationMethod();
    if (authMethod != null) {
      return authMethod;
    }

    collector.addFailure("Unsupported auth method value: " + authenticationMethod,
                         String.format("Supported auth methods are: %s", SqsAuthMethod.getSupportedAuthMethods()))
      .withConfigProperty(SqsConstants.PROPERTY_AUTH_METHOD);
    return null;
  }

  /**
   * Returns the auth method chosen.
   *
   * @return An instance of SqsAuthMethod
   */
  public SqsAuthMethod getAuthenticationMethod() {
    Optional<SqsAuthMethod> sqsAuthMethod = SqsAuthMethod.fromValue(authenticationMethod);

    return sqsAuthMethod.isPresent() ? sqsAuthMethod.get() : null;
  }

  @Nullable
  public String getAccessId() {
    return accessId;
  }

  @Nullable
  public String getAccessKey() {
    return accessKey;
  }

  /**
   * Returns the region chosen.
   *
   * @param collector The failure collector to collect the errors
   * @return An instance of Regions
   */
  public Regions getRegion(FailureCollector collector) {
    Regions awsRegion = getRegion();
    if (awsRegion != null) {
      return awsRegion;
    }

    String supportedRegions = Arrays.stream(Regions.values()).map(Regions::getName)
      .collect(Collectors.joining(", "));

    collector.addFailure("Unsupported region value: " + region,
      String.format("Supported regions are: %s", supportedRegions))
      .withConfigProperty(SqsConstants.PROPERTY_REGION);

    return null;
  }

  /**
   * Returns the region chosen.
   *
   * @return An instance of Regions
   */
  public Regions getRegion() {
    try {
      return Regions.fromName(region);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  public String getQueueName() {
    return queueName;
  }

  @Nullable
  public String getSqsEndpoint() {
    return sqsEndpoint;
  }

  /**
   * Validates {@link SqsPluginConfig} instance.
   *
   * @param collector The failure collector to collect the errors
   */
  public void validate(FailureCollector collector) {
    IdUtils.validateReferenceName(referenceName, collector);
    validateCredentials(collector);
    validateQueueName(collector);
    validateRegion(collector);
  }

  private void validateCredentials(FailureCollector collector) {
    if (containsMacro(SqsConstants.PROPERTY_AUTH_METHOD) ||
      containsMacro(SqsConstants.PROPERTY_ACCESS_ID) ||
      containsMacro(SqsConstants.PROPERTY_ACCESS_KEY) ||
      containsMacro(SqsConstants.PROPERTY_SQS_ENDPOINT) ||
      containsMacro(SqsConstants.PROPERTY_QUEUE_NAME)) {
      return;
    }

    SqsAuthMethod authMethod = getAuthenticationMethod(collector);

    if (authMethod == SqsAuthMethod.ACCESS_CREDENTIALS) {
      if (Utility.isNullOrEmpty(accessId)) {
        collector.addFailure("Access ID must be specified.", null)
          .withConfigProperty(SqsConstants.PROPERTY_ACCESS_ID);
      }

      if (Utility.isNullOrEmpty(accessKey)) {
        collector.addFailure("Access Key must be specified.", null)
          .withConfigProperty(SqsConstants.PROPERTY_ACCESS_KEY);
      }
    }

    validateSqsConnection(collector);
  }

  /**
   * Validates the SQS connection.
   *
   * @param collector The failure collector to collect the errors
   */
  @VisibleForTesting
  public void validateSqsConnection(FailureCollector collector) {
    try {
      AmazonSQS sqs = SqsUtil.getSqsClient(getAuthenticationMethod(), accessId, accessKey, sqsEndpoint, region,
        queueName);
      if (sqs != null) {
        sqs.shutdown();
      }
    } catch (SqsInitializationException e) {
      collector.addFailure("Unable to connect to Amazon SQS.",
        "Ensure properties like Auth Method, Access ID, Access Key, Region, Queue name are correct.")
        .withConfigProperty(SqsConstants.PROPERTY_ACCESS_ID)
        .withConfigProperty(SqsConstants.PROPERTY_ACCESS_KEY)
        .withConfigProperty(SqsConstants.PROPERTY_REGION)
        .withConfigProperty(SqsConstants.PROPERTY_QUEUE_NAME)
        .withStacktrace(e.getStackTrace());
    }
  }

  private void validateQueueName(FailureCollector collector) {
    if (containsMacro(SqsConstants.PROPERTY_QUEUE_NAME)) {
      return;
    }

    if (Utility.isNullOrEmpty(queueName)) {
      collector.addFailure("Queue name must be specified.", null)
        .withConfigProperty(SqsConstants.PROPERTY_QUEUE_NAME);
    }
  }

  private void validateRegion(FailureCollector collector) {
    if (containsMacro(SqsConstants.PROPERTY_REGION)) {
      return;
    }

    getRegion(collector);
  }
}
