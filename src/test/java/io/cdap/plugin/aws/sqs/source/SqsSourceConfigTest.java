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

import com.amazonaws.regions.Regions;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.aws.sqs.source.util.SqsSourceConstants;
import io.cdap.plugin.aws.sqs.util.SqsAuthMethod;
import io.cdap.plugin.aws.sqs.util.SqsConstants;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

/**
 * Tests for {@link SqsSourceConfig}.
 */
public class SqsSourceConfigTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testAuthMethodCredentials() {
    SqsAuthMethod authMethod = SqsAuthMethod.ACCESS_CREDENTIALS;
    SqsSourceConfig config = SqsSourceConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(authMethod, config.getAuthenticationMethod(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testAuthMethodIAM() {
    SqsSourceConfig config = SqsSourceConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("IAM")
      .build();

    try {
      MockFailureCollector collector = new MockFailureCollector();
      config.getAuthenticationMethod(collector);
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(SqsConstants.PROPERTY_AUTH_METHOD, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testAuthMethodInvalid() {
    SqsSourceConfig config = SqsSourceConfigHelper.newConfigBuilder()
      .setAuthenticationMethod(null)
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertNull(config.getAuthenticationMethod(collector));
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(SqsConstants.PROPERTY_AUTH_METHOD, collector.getValidationFailures().get(0).getCauses().get(0)
      .getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testRegionUSEast1() {
    Regions region = Regions.US_EAST_1;
    SqsSourceConfig config = SqsSourceConfigHelper.newConfigBuilder()
      .setRegion("us-east-1")
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(region, config.getRegion(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testRegionUSWest1() {
    Regions region = Regions.US_WEST_1;
    SqsSourceConfig config = SqsSourceConfigHelper.newConfigBuilder()
      .setRegion("us-west-1")
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertEquals(region, config.getRegion(collector));
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testRegionInvalid() {
    SqsSourceConfig config = SqsSourceConfigHelper.newConfigBuilder()
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
    SqsSourceConfig config = withSqsValidationMock(SqsSourceConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(null)
      .setAccessKey(SqsSourceConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSourceConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSourceConfigHelper.TEST_REGION)
      .setQueueName(SqsSourceConfigHelper.TEST_QUEUE_NAME)
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
    SqsSourceConfig config = withSqsValidationMock(SqsSourceConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSourceConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(null)
      .setSqsEndpoint(SqsSourceConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSourceConfigHelper.TEST_REGION)
      .setQueueName(SqsSourceConfigHelper.TEST_QUEUE_NAME)
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
    SqsSourceConfig config = withSqsValidationMock(SqsSourceConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSourceConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(SqsSourceConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSourceConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSourceConfigHelper.TEST_REGION)
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
    SqsSourceConfig config = withSqsValidationMock(SqsSourceConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSourceConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(SqsSourceConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSourceConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSourceConfigHelper.TEST_REGION)
      .setQueueName(SqsSourceConfigHelper.TEST_QUEUE_NAME)
      .build(), collector);

    config.validate(collector);

    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testInvalidInterval() {
    MockFailureCollector collector = new MockFailureCollector();
    SqsSourceConfig config = withSqsValidationMock(SqsSourceConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSourceConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(SqsSourceConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSourceConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSourceConfigHelper.TEST_REGION)
      .setQueueName(SqsSourceConfigHelper.TEST_QUEUE_NAME)
      .setInterval(-1)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(SqsSourceConstants.PROPERTY_INTERVAL, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testWaitTimeLessThan1() {
    MockFailureCollector collector = new MockFailureCollector();
    SqsSourceConfig config = withSqsValidationMock(SqsSourceConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSourceConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(SqsSourceConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSourceConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSourceConfigHelper.TEST_REGION)
      .setQueueName(SqsSourceConfigHelper.TEST_QUEUE_NAME)
      .setInterval(1)
      .setWaitTime(0)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(SqsSourceConstants.PROPERTY_WAIT_TIME, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testWaitTimeGreaterThan20() {
    MockFailureCollector collector = new MockFailureCollector();
    SqsSourceConfig config = withSqsValidationMock(SqsSourceConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSourceConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(SqsSourceConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSourceConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSourceConfigHelper.TEST_REGION)
      .setQueueName(SqsSourceConfigHelper.TEST_QUEUE_NAME)
      .setInterval(1)
      .setWaitTime(21)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(SqsSourceConstants.PROPERTY_WAIT_TIME, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testNumberOfMessagesLessThan1() {
    MockFailureCollector collector = new MockFailureCollector();
    SqsSourceConfig config = withSqsValidationMock(SqsSourceConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSourceConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(SqsSourceConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSourceConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSourceConfigHelper.TEST_REGION)
      .setQueueName(SqsSourceConfigHelper.TEST_QUEUE_NAME)
      .setInterval(1)
      .setWaitTime(20)
      .setNumberOfMessages(0)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(SqsSourceConstants.PROPERTY_NUMBER_OF_MESSAGES, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testNumberOfMessagesGreaterThan10() {
    MockFailureCollector collector = new MockFailureCollector();
    SqsSourceConfig config = withSqsValidationMock(SqsSourceConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSourceConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(SqsSourceConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSourceConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSourceConfigHelper.TEST_REGION)
      .setQueueName(SqsSourceConfigHelper.TEST_QUEUE_NAME)
      .setInterval(1)
      .setWaitTime(20)
      .setNumberOfMessages(11)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(SqsSourceConstants.PROPERTY_NUMBER_OF_MESSAGES, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateSchemaNull() {
    MockFailureCollector collector = new MockFailureCollector();
    SqsSourceConfig config = withSqsValidationMock(SqsSourceConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSourceConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(SqsSourceConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSourceConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSourceConfigHelper.TEST_REGION)
      .setQueueName(SqsSourceConfigHelper.TEST_QUEUE_NAME)
      .setInterval(1)
      .setWaitTime(20)
      .setNumberOfMessages(10)
      .setSchema(null)
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(SqsSourceConstants.PROPERTY_SCHEMA, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateFormatInvalidSchema() {
    MockFailureCollector collector = new MockFailureCollector();
    SqsSourceConfig config = withSqsValidationMock(SqsSourceConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSourceConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(SqsSourceConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSourceConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSourceConfigHelper.TEST_REGION)
      .setQueueName(SqsSourceConfigHelper.TEST_QUEUE_NAME)
      .setInterval(1)
      .setWaitTime(20)
      .setNumberOfMessages(10)
      .setSchema("null")
      .setFormat("")
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(SqsSourceConstants.PROPERTY_SCHEMA, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateFormatSchemaWithNoFields() {
    String schema = "{\n" +
      "          \"name\": \"etlSchemaBody\",\n" +
      "          \"type\": \"record\",\n" +
      "          \"fields\": [\n" +
      "          ]\n" +
      "        }";
    MockFailureCollector collector = new MockFailureCollector();
    SqsSourceConfig config = withSqsValidationMock(SqsSourceConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSourceConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(SqsSourceConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSourceConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSourceConfigHelper.TEST_REGION)
      .setQueueName(SqsSourceConfigHelper.TEST_QUEUE_NAME)
      .setInterval(1)
      .setWaitTime(20)
      .setNumberOfMessages(10)
      .setSchema(schema)
      .setFormat("")
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(SqsSourceConstants.PROPERTY_SCHEMA, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateEmptyFormatAndSchemaWithMoreThan1Field() {
    String schema = "{\n" +
      "          \"name\": \"etlSchemaBody\",\n" +
      "          \"type\": \"record\",\n" +
      "          \"fields\": [\n" +
      "            {\n" +
      "              \"name\": \"title\",\n" +
      "              \"type\": \"string\"\n" +
      "            },\n" +
      "            {\n" +
      "              \"name\": \"message\",\n" +
      "              \"type\": \"string\"\n" +
      "            }\n" +
      "          ]\n" +
      "        }";
    MockFailureCollector collector = new MockFailureCollector();
    SqsSourceConfig config = withSqsValidationMock(SqsSourceConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSourceConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(SqsSourceConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSourceConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSourceConfigHelper.TEST_REGION)
      .setQueueName(SqsSourceConfigHelper.TEST_QUEUE_NAME)
      .setInterval(1)
      .setWaitTime(20)
      .setNumberOfMessages(10)
      .setSchema(schema)
      .setFormat("")
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(SqsSourceConstants.PROPERTY_SCHEMA, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateEmptyFormatAndSchemaWithNonBytesField() {
    String schema = "{" +
      "          \"name\": \"etlSchemaBody\",\n" +
      "          \"type\": \"record\",\n" +
      "          \"fields\": [\n" +
      "            {\n" +
      "              \"name\": \"message\",\n" +
      "              \"type\": \"string\"\n" +
      "            }\n" +
      "          ]\n" +
      "        }";
    MockFailureCollector collector = new MockFailureCollector();
    SqsSourceConfig config = withSqsValidationMock(SqsSourceConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSourceConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(SqsSourceConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSourceConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSourceConfigHelper.TEST_REGION)
      .setQueueName(SqsSourceConfigHelper.TEST_QUEUE_NAME)
      .setInterval(1)
      .setWaitTime(20)
      .setNumberOfMessages(10)
      .setSchema(schema)
      .setFormat("")
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(SqsSourceConstants.PROPERTY_SCHEMA, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  @Test
  public void testValidateInvalidFormat() {
    String schema = "{\n" +
      "          \"name\": \"etlSchemaBody\",\n" +
      "          \"type\": \"record\",\n" +
      "          \"fields\": [\n" +
      "            {\n" +
      "              \"name\": \"title\",\n" +
      "              \"type\": \"string\"\n" +
      "            },\n" +
      "            {\n" +
      "              \"name\": \"message\",\n" +
      "              \"type\": \"string\"\n" +
      "            }\n" +
      "          ]\n" +
      "        }";
    MockFailureCollector collector = new MockFailureCollector();
    SqsSourceConfig config = withSqsValidationMock(SqsSourceConfigHelper.newConfigBuilder()
      .setAuthenticationMethod("Access Credentials")
      .setAccessId(SqsSourceConfigHelper.TEST_ACCESS_ID)
      .setAccessKey(SqsSourceConfigHelper.TEST_ACCESS_KEY)
      .setSqsEndpoint(SqsSourceConfigHelper.TEST_SQS_ENDPOINT)
      .setRegion(SqsSourceConfigHelper.TEST_REGION)
      .setQueueName(SqsSourceConfigHelper.TEST_QUEUE_NAME)
      .setInterval(1)
      .setWaitTime(20)
      .setNumberOfMessages(10)
      .setSchema(schema)
      .setFormat("json")
      .build(), collector);

    try {
      config.validate(collector);
      collector.getOrThrowException();
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      Assert.assertEquals(SqsSourceConstants.PROPERTY_FORMAT, e.getFailures().get(0).getCauses().get(0)
        .getAttribute(CauseAttributes.STAGE_CONFIG));
    }
  }

  private SqsSourceConfig withSqsValidationMock(SqsSourceConfig config, FailureCollector collector) {
    SqsSourceConfig spy = Mockito.spy(config);
    Mockito.doNothing().when(spy).validateSqsConnection(collector);
    return spy;
  }
}
