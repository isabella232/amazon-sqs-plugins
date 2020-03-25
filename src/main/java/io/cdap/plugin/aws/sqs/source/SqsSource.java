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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.api.streaming.StreamingSource;
import io.cdap.plugin.aws.sqs.util.SqsConstants;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * A {@link StreamingSource} that reads from Amazon SQS Queue and converts each message into
 * a {@link StructuredRecord}.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name(SqsConstants.PLUGIN_NAME)
@Description("Amazon SQS Streaming Source reads messages from Amazon SQS Queue.")
public class SqsSource extends StreamingSource<StructuredRecord> {
  private final SqsSourceConfig config;

  public SqsSource(SqsSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    StageConfigurer configurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = configurer.getFailureCollector();
    config.validate(collector);
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(StreamingContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    context.registerLineage(config.referenceName);
    return SqsSourceUtil.getJavaDStream(context, config);
  }
}
