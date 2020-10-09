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

import com.amazonaws.regions.Regions;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.aws.sqs.sink.util.SinkMessageFormat;
import io.cdap.plugin.aws.sqs.sink.util.SqsSinkConstants;
import io.cdap.plugin.aws.sqs.util.SqsAuthMethod;
import io.cdap.plugin.aws.sqs.util.SqsConstants;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

/**
 * Tests for {@link SqsSinkConfig}.
 */
public class SqsSinkConfigTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testAuthMethodCredentials() {
    SqsAuthMethod authMethod = SqsAuthMethod.ACCESS_CREDENTIALS;
    SqsSinkConfig config = SqsSinkConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(authMethod, config.getAuthenticationMethod(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testAuthMethodIAM() {
    SqsAuthMethod authMethod = SqsAuthMethod.IAM;
    SqsSinkConfig config = SqsSinkConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("IAM")
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(authMethod, config.getAuthenticationMethod(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testAuthMethodInvalid() {
    SqsSinkConfig config = SqsSinkConfigHelper.newConfigBuilder()
      .setAuthenticationMethod(null)
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertNull(config.getAuthenticationMethod(collector));
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(SqsConstants.PROPERTY_AUTH_METHOD, collector.getValidationFailures().get(0).getCauses().get(0)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testMessageFormatCSV() {
    SinkMessageFormat messageFormat = SinkMessageFormat.CSV;
    SqsSinkConfig config = SqsSinkConfigHelper.newConfigBuilder()
      .setMessageFormat("CSV")
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(messageFormat, config.getMessageFormat(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testMessageFormatJSON() {
    SinkMessageFormat messageFormat = SinkMessageFormat.JSON;
    SqsSinkConfig config = SqsSinkConfigHelper.newConfigBuilder()
      .setMessageFormat("JSON")
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(messageFormat, config.getMessageFormat(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testMessageFormatInvalid() {
    SqsSinkConfig config = SqsSinkConfigHelper.newConfigBuilder()
      .setMessageFormat(null)
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertNull(config.getMessageFormat(collector));
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(SqsSinkConstants.PROPERTY_MESSAGE_FORMAT, collector.getValidationFailures().get(0).getCauses().get(0)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testRegionUSEast1() {
    Regions region = Regions.US_EAST_1;
    SqsSinkConfig config = SqsSinkConfigHelper.newConfigBuilder()
      .setRegion("us-east-1")
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(region, config.getRegion(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testRegionUSWest1() {
    Regions region = Regions.US_WEST_1;
    SqsSinkConfig config = SqsSinkConfigHelper.newConfigBuilder()
      .setRegion("us-west-1")
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(region, config.getRegion(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testRegionInvalid() {
    SqsSinkConfig config = SqsSinkConfigHelper.newConfigBuilder()
      .setRegion(null)
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertNull(config.getRegion(collector));
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(SqsConstants.PROPERTY_REGION, collector.getValidationFailures().get(0).getCauses().get(0)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testValidateAccessIdNull() {
    MockFailureCollector collector = new MockFailureCollector();
    SqsSinkConfig config = withSqsValidationMock(SqsSinkConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(null)
      .setAccessKey(SqsSinkConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSinkConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSinkConfigHelper.TEST_REGION)
      .setQueueName(SqsSinkConfigHelper.TEST_QUEUE_NAME)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(SqsConstants.PROPERTY_ACCESS_ID, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateAccessKeyNull() {
    MockFailureCollector collector = new MockFailureCollector();
    SqsSinkConfig config = withSqsValidationMock(SqsSinkConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSinkConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(null)
      .setSqsEndpoint(SqsSinkConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSinkConfigHelper.TEST_REGION)
      .setQueueName(SqsSinkConfigHelper.TEST_QUEUE_NAME)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(SqsConstants.PROPERTY_ACCESS_KEY, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateQueueNameNull() {
    MockFailureCollector collector = new MockFailureCollector();
    SqsSinkConfig config = withSqsValidationMock(SqsSinkConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSinkConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(SqsSinkConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSinkConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSinkConfigHelper.TEST_REGION)
      .setQueueName(null)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(SqsConstants.PROPERTY_QUEUE_NAME, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidCredentials() {
    MockFailureCollector collector = new MockFailureCollector();
    SqsSinkConfig config = withSqsValidationMock(SqsSinkConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSinkConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(SqsSinkConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSinkConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSinkConfigHelper.TEST_REGION)
      .setQueueName(SqsSinkConfigHelper.TEST_QUEUE_NAME)
      .build(), collector);

    config.validate(collector);

    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testDelayLessThan0() {
    MockFailureCollector collector = new MockFailureCollector();
    SqsSinkConfig config = withSqsValidationMock(SqsSinkConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSinkConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(SqsSinkConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSinkConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSinkConfigHelper.TEST_REGION)
      .setQueueName(SqsSinkConfigHelper.TEST_QUEUE_NAME)
      .setMessageFormat("CSV")
      .setDelay(-1)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(SqsSinkConstants.PROPERTY_DELAY_SECONDS, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testDelayGreaterThan900() {
    MockFailureCollector collector = new MockFailureCollector();
    SqsSinkConfig config = withSqsValidationMock(SqsSinkConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSinkConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(SqsSinkConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSinkConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSinkConfigHelper.TEST_REGION)
      .setQueueName(SqsSinkConfigHelper.TEST_QUEUE_NAME)
      .setMessageFormat("CSV")
      .setDelay(901)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(SqsSinkConstants.PROPERTY_DELAY_SECONDS, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  private SqsSinkConfig withSqsValidationMock(SqsSinkConfig config, FailureCollector collector) {
    SqsSinkConfig spy = Mockito.spy(config);
    Mockito.doNothing().when(spy).validateSqsConnection(collector);
    return spy;
  }
}
