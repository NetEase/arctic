package com.netease.arctic.spark.writer;

import com.netease.arctic.spark.source.SupportsDynamicOverwrite;
import com.netease.arctic.spark.source.SupportsOverwrite;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.SupportsWriteInternalRow;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

public class ArcticWriter implements DataSourceWriter, SupportsWriteInternalRow,
    SupportsOverwrite, SupportsDynamicOverwrite {

  @Override
  public DataSourceWriter overwriteDynamicPartitions() {
    return null;
  }

  @Override
  public DataSourceWriter overwrite(Filter[] filters) {
    return null;
  }

  @Override
  public DataWriterFactory<InternalRow> createInternalRowWriterFactory() {
    return null;
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {

  }

  @Override
  public void abort(WriterCommitMessage[] messages) {

  }

}
