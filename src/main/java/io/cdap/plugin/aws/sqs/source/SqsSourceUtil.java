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

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import io.cdap.cdap.api.data.format.FormatSpecification;
import io.cdap.cdap.api.data.format.RecordFormat;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.format.RecordFormats;
import io.cdap.plugin.aws.sqs.exception.SqsInitializationException;
import io.cdap.plugin.aws.sqs.util.SqsUtil;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Util method for {@link SqsSource}.
 *
 * <p>This class contains methods for {@link SqsSource} that require spark classes because
 * during validation spark classes are not available. Refer CDAP-15912 for more information.
 */
final class SqsSourceUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SqsSourceUtil.class);

  private SqsSourceUtil() {
    // no-op
  }

  static JavaDStream<StructuredRecord> getJavaDStream(StreamingContext context, SqsSourceConfig conf) {
    return context.getSparkStreamingContext().receiverStream(SqsSourceUtil.getReceiver(conf));
  }

  /**
   * Gets {@link JavaReceiverInputDStream} for {@link SqsSource}.
   *
   * @param conf {@link SqsSourceConfig} config
   */
  private static Receiver<StructuredRecord> getReceiver(SqsSourceConfig conf) {
    return new Receiver<StructuredRecord>(StorageLevel.MEMORY_ONLY()) {
      @Override
      public StorageLevel storageLevel() {
        return StorageLevel.MEMORY_ONLY();
      }

      @Override
      public void onStart() {
        new Thread() {
          @Override
          public void run() {
            AmazonSQS sqs = null;
            try {
              sqs = SqsUtil.getSqsClient(conf.getAuthenticationMethod(), conf.getAccessId(),
                conf.getAccessKey(), conf.getSqsEndpoint(), conf.getRegion().getName(), conf.getQueueName());
            } catch (SqsInitializationException e) {
              throw new RuntimeException(e);
            }

            String queueName = conf.getQueueName();
            String queueUrl = sqs.getQueueUrl(queueName).getQueueUrl();

            // Enable long polling on an existing queue
            SetQueueAttributesRequest setQueueAttributesRequest = new SetQueueAttributesRequest()
              .withQueueUrl(queueUrl)
              .addAttributesEntry("ReceiveMessageWaitTimeSeconds", String.valueOf(conf.getWaitTime()));
            sqs.setQueueAttributes(setQueueAttributesRequest);

            while (!isStopped()) {
              try {
                List<Message> messages = fetchMessages(sqs, queueUrl, conf);
                LOG.debug("messages.size() = {}", messages.size());
                for (Message message : messages) {
                  StructuredRecord record = toRecord(message, conf);
                  if (record == null) {
                    continue;
                  }
                  store(record);
                }
                if (conf.getDelete()) {
                  deleteMessages(sqs, conf, messages);
                }
              } catch (Exception e) {
                LOG.error(String.format("Error getting content from %s queue.", conf.getQueueName()), e);
              }

              if (conf.getInterval() > 0) {
                try {
                  TimeUnit.SECONDS.sleep(conf.getInterval());
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
            }

            sqs.shutdown();
          }

          @Override
          public void interrupt() {
            super.interrupt();
          }
        }.start();
      }

      @Override
      public void onStop() {
        // no-op
      }

      private List<Message> fetchMessages(AmazonSQS sqs, String queueUrl, SqsSourceConfig config) throws Exception {
        if (sqs == null) {
          return Collections.emptyList();
        }

        // Enable long polling on a message receipt
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
          .withQueueUrl(queueUrl)
          .withWaitTimeSeconds(config.getWaitTime())
          .withMaxNumberOfMessages(config.getNumberOfMessages());
        return sqs.receiveMessage(receiveMessageRequest).getMessages();
      }

      private void deleteMessages(AmazonSQS sqs, SqsSourceConfig config, List<Message> messages) throws Exception {
        if (sqs == null) {
          return;
        }

        if (messages == null || messages.isEmpty()) {
          return;
        }

        String queueName = config.getQueueName();
        String queueUrl = sqs.getQueueUrl(queueName).getQueueUrl();

        for (Message message : messages) {
          sqs.deleteMessage(queueUrl, message.getReceiptHandle());
        }
      }

      /**
       * Converts to a StructuredRecord.
       */
      private StructuredRecord toRecord(Message message, SqsSourceConfig config) {
        StructuredRecord structuredRecord = null;
        // merge default schema and credential schema to create output schema
        try {
          LOG.debug("message.toString = {}", message.toString());
          Schema outputSchema = config.parseSchema();
          List<Schema.Field> fieldList = outputSchema.getFields();
          String messageField = "";
          if (!fieldList.isEmpty()) {
            messageField = outputSchema.getFields().get(0).getName();
          }
          String messageBody = message.getBody();
          String format = config.getFormat();

          StructuredRecord.Builder outputBuilder = StructuredRecord.builder(outputSchema);

          if (format == null) {
            /**
             * Transforms SQS message into a structured record when message format is not given.
             * Everything here should be serializable, as Spark Streaming will serialize all functions.
             */
            outputBuilder.set(messageField, messageBody.getBytes());
          } else {
            /**
             * Transforms SQS message into a structured record when message format and schema are given.
             * Everything here should be serializable, as Spark Streaming will serialize all functions.
             */
            FormatSpecification spec = new FormatSpecification(format, outputSchema, new HashMap<>());
            RecordFormat<ByteBuffer, StructuredRecord> recordFormat = RecordFormats.createInitializedFormat(spec);
            StructuredRecord messageRecord = recordFormat.read(ByteBuffer.wrap(messageBody.getBytes()));
            LOG.debug("messageRecord.fields == {}", messageRecord.getSchema().getFields().size());
            for (Schema.Field field : messageRecord.getSchema().getFields()) {
              String fieldName = field.getName();
              LOG.debug("messageRecord.{} == {}", fieldName, messageRecord.get(fieldName));
              outputBuilder.set(fieldName, messageRecord.get(fieldName));
            }
          }

          structuredRecord = outputBuilder.build();
        } catch (Exception e) {
          LOG.error(String.format("Failed parsing SQS Message - %s", message.toString()), e);
        }

        return structuredRecord;
      }
    };
  }
}
