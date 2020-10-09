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

/**
 * AWS SQS constants.
 */
public interface SqsConstants {

  /**
   * AWS SQS plugin name.
   */
  String PLUGIN_NAME = "sqs";

  /**
   * Configuration property name used to specify Authentication method.
   */
  String PROPERTY_AUTH_METHOD = "authenticationMethod";

  /**
   * Configuration property name used to specify Amazon Access ID.
   */
  String PROPERTY_ACCESS_ID = "accessID";

  /**
   * Configuration property name used to specify Amazon Access Key.
   */
  String PROPERTY_ACCESS_KEY = "accessKey";

  /**
   * Configuration property name used to specify Amazon Region.
   */
  String PROPERTY_REGION = "region";

  /**
   * Configuration property name used to specify queue name to read from.
   */
  String PROPERTY_QUEUE_NAME = "queueName";

  /**
   * Configuration property name used to specify Amazon SQS Endpoint url.
   */
  String PROPERTY_SQS_ENDPOINT = "sqsEndpoint";
}
