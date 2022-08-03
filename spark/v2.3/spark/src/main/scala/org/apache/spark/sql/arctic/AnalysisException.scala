package org.apache.spark.sql.arctic

import org.apache.spark.sql.AnalysisException

case object AnalysisException {
  def message(message: String): AnalysisException = {
    new AnalysisException(message)
  }
}
