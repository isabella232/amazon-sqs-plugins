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

import com.google.common.collect.Lists;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.aws.sqs.sink.util.SinkMessageFormat;
import io.cdap.plugin.aws.sqs.util.SqsConstants;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.common.ReferenceBatchSink;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A {@link ReferenceBatchSink} that writes messages to Amazon SQS Queue.
 * This {@link SqsSink} takes a {@link StructuredRecord} in, and writes it to the Amazon SQS Queue.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(SqsConstants.PLUGIN_NAME)
@Description("CDAP Amazon SQS Batch Sink takes the structured record from the input source and writes "
  + "to Amazon SQS Queue.")
public class SqsSink extends BatchSink<StructuredRecord, NullWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(SqsSink.class);

  // Configuration for the plugin.
  private final SqsSinkConfig sqsSinkConfig;

  public SqsSink(SqsSinkConfig sqsSinkConfig) {
    this.sqsSinkConfig = sqsSinkConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    StageConfigurer configurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = configurer.getFailureCollector();
    sqsSinkConfig.validate(collector);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    sqsSinkConfig.validate(collector);
    collector.getOrThrowException();

    emitLineage(context);
    context.addOutput(Output.of(sqsSinkConfig.referenceName, new SqsOutputFormatProvider(sqsSinkConfig)));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Text>> emitter) throws Exception {
    List<Schema.Field> fields = input.getSchema().getFields();

    String body;
    if (sqsSinkConfig.getMessageFormat() == SinkMessageFormat.JSON) {
      body = StructuredRecordStringConverter.toJsonString(input);
    } else {
      List<Object> objs = getExtractedValues(input, fields);
      body = StringUtils.join(objs, ",");
    }

    emitter.emit(new KeyValue<>(NullWritable.get(), new Text(body)));
  }

  private void emitLineage(BatchSinkContext context) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, sqsSinkConfig.referenceName);
    Schema inputSchema = context.getInputSchema();

    if (inputSchema == null) {
      return;
    }

    lineageRecorder.createExternalDataset(inputSchema);
    // Record the field level WriteOperation
    lineageRecorder.recordWrite("Write", "Wrote to Amazon SQS sink",
      inputSchema.getFields().stream()
        .map(Schema.Field::getName)
        .collect(Collectors.toList()));

  }

  private List<Object> getExtractedValues(StructuredRecord input, List<Schema.Field> fields) {
    // Extract all values from the structured record
    List<Object> objs = Lists.newArrayList();
    for (Schema.Field field : fields) {
      Object fieldValue = input.get(field.getName());
      objs.add(fieldValue == null ? "" : fieldValue.toString());
    }
    return objs;
  }
}
