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

import com.google.common.base.Strings;

/**
 * Utility class.
 */
public class Utility {
  /**
   * Checks if string is empty or not after trimming it.
   *
   * @param string The string to be checked for Null or Empty
   * @return true if string is either NULL / empty (after trimming) otherwise returns false
   */
  public static boolean isNullOrEmpty(String string) {
    return Strings.isNullOrEmpty(Strings.nullToEmpty(string).trim());
  }
}
