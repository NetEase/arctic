package com.netease.arctic.spark.sql.catalyst.analysis

import com.netease.arctic.spark.source.{ArcticSource, SupportsDynamicOverwrite}
import org.apache.spark.sql.arctic.AnalysisException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, WriteToDataSourceV2}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SaveMode, SparkSession}

case class ArcticResolutionDelegateHiveRule(spark: SparkSession) extends Rule[LogicalPlan] {

  lazy val arctic = new ArcticSource

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {

    // insert into data source table
    case i @ InsertIntoTable(l: LogicalRelation, _, _, _, _)
      if l.catalogTable.isDefined && arctic.isDelegateTable(l.catalogTable.get) =>
      createWriteToArcticSource(i, l.catalogTable.get)

    // insert into hive table
    case i @ InsertIntoTable(table: HiveTableRelation, _, _, _, _)
      if arctic.isDelegateTable(table.tableMeta) =>
      createWriteToArcticSource(i, table.tableMeta)

    // scan datasource table
    case l @ LogicalRelation(_,_,table,_) if table.isDefined && arctic.isDelegateTable(table.get) =>
      val reader = createArcticReader(table.get)
      val output = reader.readSchema()
        .map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)() )
      DataSourceV2Relation(output, reader)

    // scan hive table
    case h @ HiveTableRelation(tableMeta, dataCols, partitionCols)
      if arctic.isDelegateTable(tableMeta) =>
      val reader = createArcticReader(tableMeta)
      val output = reader.readSchema()
        .map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)() )
      DataSourceV2Relation(output, reader)
  }

  def createWriteToArcticSource(i: InsertIntoTable, tableDesc: CatalogTable): WriteToDataSourceV2 = {
    if (i.ifPartitionNotExists){
      throw AnalysisException.message(
        s"Cannot write, IF NOT EXISTS is not supported for table: ${tableDesc.identifier}")
    }

    val table = arctic.loadTable(tableDesc.identifier)
    val query = addStaticPartitionColumns(table.schema(), i.partition, i.query)
    val mode = if (i.overwrite) {
      SaveMode.Overwrite
    } else {
      SaveMode.Append
    }
    val optWriter = table.createWriter("", query.schema, mode, null)
    if(!optWriter.isPresent){
      throw AnalysisException.message(s"failed to create writer for table ${tableDesc.identifier}")
    }

    val writer = optWriter.get match {
      case w: SupportsDynamicOverwrite =>
        w.overwriteDynamicPartitions()
    }
    WriteToDataSourceV2(writer, query)
  }

  // add any static value as a literal column
  // part copied from spark-3.0 branch Analyzer.scala
  private def addStaticPartitionColumns(
      schema: StructType, partitionSpec: Map[String, Option[String]],
      query: LogicalPlan): LogicalPlan = {
    val staticPartitions = partitionSpec.filter(_._2.isDefined).mapValues(_.get)
    if(staticPartitions.isEmpty){
      query
    }else{
      val output = schema.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
      val conf = SQLConf.get
      val withStaticPartitionValues = {
        val outputNameToStaticName = staticPartitions.keySet.map(staticName =>
          output.find(col => conf.resolver(col.name, staticName)) match {
            case Some(attr) =>
              attr.name -> staticName
            case _ =>
              throw AnalysisException.message(
                s"Cannot add static value for unknown column: $staticName")
          }).toMap

        val queryColumns = query.output.iterator
        output.flatMap { col =>
          outputNameToStaticName.get(col.name).flatMap(staticPartitions.get) match {
            case Some(staticValue) =>
              // for each output column, add the static value as a literal, or use the next input
              // column. this does not fail if input columns are exhausted and adds remaining columns
              // at the end. both cases will be caught by ResolveOutputRelation and will fail the
              // query with a helpful error message.
              Some(Alias(Cast(Literal(staticValue), col.dataType), col.name)())
            case _ if queryColumns.hasNext =>
              Some(queryColumns.next)
            case _ =>
              None
          }
        } ++ queryColumns
      }

      Project(withStaticPartitionValues, query)
    }
  }

  def createArcticReader(table: CatalogTable): DataSourceReader = {
    val arcticTable = arctic.loadTable(table.identifier)
    val reader = arcticTable.createReader(null)
    reader
  }
}