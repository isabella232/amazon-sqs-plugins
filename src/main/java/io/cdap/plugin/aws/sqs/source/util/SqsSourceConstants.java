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

package io.cdap.plugin.aws.sqs.source.util;

/**
 * AWS SQS Source constants.
 */
public interface SqsSourceConstants {

  /**
   * Configuration property name used to specify interval.
   */
  String PROPERTY_INTERVAL = "interval";

  /**
   * Configuration property name used to specify wait time.
   */
  String PROPERTY_WAIT_TIME = "waitTime";

  /**
   * Configuration property name used to specify number of messages.
   */
  String PROPERTY_NUMBER_OF_MESSAGES = "numberOfMessages";

  /**
   * Configuration property name used to specify message to be deleted from queue or not.
   */
  String PROPERTY_DELETE = "delete";

  /**
   * Configuration property name used to specify format.
   */
  String PROPERTY_FORMAT = "format";

  /**
   * Configuration property name used to specify the schema.
   */
  String PROPERTY_SCHEMA = "schema";

  /**
   * The maximum wait time in seconds.
   */
  int MAX_WAIT_TIME = 20;

  /**
   * The maximum number of messages to be fetched.
   */
  int MAX_MESSAGES_LIMIT = 10;
}
