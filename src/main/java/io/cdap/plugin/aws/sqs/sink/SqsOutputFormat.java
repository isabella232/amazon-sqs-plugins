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

import com.amazonaws.services.sqs.AmazonSQS;
import io.cdap.plugin.aws.sqs.sink.util.SqsSinkConstants;
import io.cdap.plugin.aws.sqs.util.SqsConstants;
import io.cdap.plugin.aws.sqs.util.SqsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Output format to write to SQS.
 */
public class SqsOutputFormat extends OutputFormat<NullWritable, Text> {
  @Override
  public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException,
    InterruptedException {
    return new OutputCommitter() {
      @Override
      public void setupJob(JobContext jobContext) throws IOException {
        // no-op
      }

      @Override
      public void setupTask(TaskAttemptContext taskContext) throws IOException {
        //no-op
      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskContext) throws IOException {
        //no-op
      }

      @Override
      public void abortTask(TaskAttemptContext taskContext) throws IOException {
        //no-op
      }
    };
  }

  @Override
  public RecordWriter<NullWritable, Text> getRecordWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    Configuration configuration = context.getConfiguration();

    String authMethod = configuration.get(SqsConstants.PROPERTY_AUTH_METHOD);
    String accessId = configuration.get(SqsConstants.PROPERTY_ACCESS_ID);
    String accessKey = configuration.get(SqsConstants.PROPERTY_ACCESS_KEY);
    String endpoint = configuration.get(SqsConstants.PROPERTY_SQS_ENDPOINT);
    String queueName = configuration.get(SqsConstants.PROPERTY_QUEUE_NAME);
    Integer delaySeconds = Integer.parseInt(configuration.get(SqsSinkConstants.PROPERTY_DELAY_SECONDS));
    String region = configuration.get(SqsConstants.PROPERTY_REGION);

    AmazonSQS amazonSQS = SqsUtil.getSqsClient(authMethod, accessId, accessKey, endpoint, region, queueName);
    String queueUrl = amazonSQS.getQueueUrl(queueName).getQueueUrl();

    return new SqsRecordWriter(amazonSQS, queueUrl, delaySeconds);
  }
}

