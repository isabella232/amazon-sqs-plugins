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

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Indicates the auth method to be used for connecting to Amazon SQS.
 */
public enum SqsAuthMethod {

  /**
   * Access credentials to be used for connecting to Amazon SQS.
   */
  ACCESS_CREDENTIALS("Access Credentials"),

  /**
   * IAM to be used for connecting to Amazon SQS.
   */
  IAM("IAM");

  private final String value;

  SqsAuthMethod(String value) {
    this.value = value;
  }

  /**
   * Converts auth method string value into {@link SqsAuthMethod} enum.
   *
   * @param stringValue auth method string value
   * @return auth method in optional container
   */
  public static Optional<SqsAuthMethod> fromValue(String stringValue) {
    return Stream.of(values())
      .filter(keyType -> keyType.value.equalsIgnoreCase(stringValue))
      .findAny();
  }

  public static String getSupportedAuthMethods() {
    return Arrays.stream(SqsAuthMethod.values()).map(SqsAuthMethod::getValue)
      .collect(Collectors.joining(", "));
  }

  public String getValue() {
    return value;
  }
}
