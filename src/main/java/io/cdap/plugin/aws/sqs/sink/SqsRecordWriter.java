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
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Record writer to write messages to SQS.
 */
public class SqsRecordWriter extends RecordWriter<NullWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(SqsRecordWriter.class);
  private final AmazonSQS amazonSQS;
  private final String queueUrl;
  private final Integer delaySeconds;

  /**
   * Constructor for SqsRecordWriter object.
   *
   * @param amazonSQS The SQS instance reference
   * @param queueUrl The SQS Queue url
   * @param delaySeconds The value for delay
   */
  public SqsRecordWriter(AmazonSQS amazonSQS, String queueUrl, Integer delaySeconds) {
    this.amazonSQS = amazonSQS;
    this.queueUrl = queueUrl;
    this.delaySeconds = delaySeconds;
  }

  @Override
  public void write(NullWritable key, Text value) throws IOException, InterruptedException {
    sendMessage(value.toString());
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    if (amazonSQS != null) {
      amazonSQS.shutdown();
    }
  }

  private void sendMessage(final String body) throws IOException, InterruptedException {
    SendMessageRequest sendMessageRequest = new SendMessageRequest()
      .withQueueUrl(queueUrl)
      .withMessageBody(body)
      .withDelaySeconds(delaySeconds);

    amazonSQS.sendMessage(sendMessageRequest);
  }
}
