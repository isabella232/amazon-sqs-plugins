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

package io.cdap.plugin.aws.sqs.util;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import io.cdap.plugin.aws.sqs.exception.SqsInitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Utility class.
 */
public class SqsUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SqsUtil.class);

  public static AmazonSQS getSqsClient(String authMethod, String accessId, String accessKey, String endpoint,
                                       String region, String queueName) {
    SqsAuthMethod method = getAuthenticationMethod(authMethod);
    return getSqsClient(method, accessId, accessKey, endpoint, region, queueName);
  }

  /**
   * Tries to establish a connection with Amazon SQS and returns an instance of AmazonSQS object.
   * @param authMethod The auth method to be used
   * @param accessId The AWS Client Id
   * @param accessKey The AWS Client Key
   * @param endpoint The SQS Endpoint
   * @param region The AWS Region
   * @param queueName The SQS Queue name
   * @return The instance of AmazonSQS object
   * @throws SqsInitializationException
   */
  public static AmazonSQS getSqsClient(SqsAuthMethod authMethod, String accessId, String accessKey, String endpoint,
                                       String region, String queueName) throws SqsInitializationException {
    LOG.debug("AuthMethod={}, accessId={}, accessKey={}, endpoint={}, region={}, queue={}", authMethod.getValue(),
      accessId, accessKey, endpoint, region, queueName);
    AmazonSQS sqs = null;
    try {
      AmazonSQSClientBuilder builder = AmazonSQSClientBuilder.standard();

      if (authMethod == SqsAuthMethod.ACCESS_CREDENTIALS) {
        BasicAWSCredentials credentials = new BasicAWSCredentials(accessId, accessKey);
        builder = builder.withCredentials(new AWSStaticCredentialsProvider(credentials));
      }

      if (Utility.isNullOrEmpty(endpoint)) {
        builder = builder.withRegion(region);
      } else {
        builder = builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));
      }

      sqs = builder.build();

      sqs.getQueueUrl(queueName).getQueueUrl();
    } catch (Exception e) {
      sqs = null;
      throw new SqsInitializationException(e.getMessage(), e);
    }
    return sqs;
  }

  private static SqsAuthMethod getAuthenticationMethod(String authMethod) {
    Optional<SqsAuthMethod> sqsAuthMethod = SqsAuthMethod.fromValue(authMethod);

    return sqsAuthMethod.isPresent() ? sqsAuthMethod.get() : null;
  }
}
