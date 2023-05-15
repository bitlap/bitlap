package org.bitlap.spark.writer

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{ FunctionRegistryBase, SimpleFunctionRegistry }
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.{ DataFrame, SaveMode }
import org.bitlap.common.BitlapConf
import org.bitlap.core.Constants
import org.bitlap.jdbc.BitlapDatabaseMetaData
import org.bitlap.spark.udf.{ BBMAggr, CBMAggr, UDFUtils }
import org.bitlap.spark.{ BitlapOptions, SparkUtils }

import scala.reflect.ClassTag

/** bitlap mdm model writer
 */
class BitlapMdmWriter(val data: DataFrame, val options: BitlapOptions) {

  private val spark      = data.sparkSession
  private val UDF_PREFIX = "bitlap_spark_udf"
  private lazy val serverConf: BitlapConf =
    JdbcUtils.withConnection(options) { conn =>
      val metaData = conn.getMetaData.asInstanceOf[BitlapDatabaseMetaData]
      metaData.getDatabaseConf
    }
  private lazy val tableIdentifier = this.options.tableIdentifier
  private lazy val rootPath: Path  = new Path(this.serverConf.get(BitlapConf.ROOT_DIR_DATA))
  private lazy val tablePath =
    new Path(new Path(rootPath, tableIdentifier.database.getOrElse(Constants.DEFAULT_DATABASE)), tableIdentifier.table)

  def write(): Unit = {
    this.initSpark()

    val mdmView = SparkUtils.withTempView(data -> "bitlap_spark_mdm_view") {
      spark.sql(s"""
           |select
           |  time
           | ,entity
           | ,first_value(dimensions) dimensions
           | ,dimensions_md5
           | ,metric_name
           | ,sum(metric_value) metric_value
           |from (
           |  select
           |    time
           |   ,entity
           |   ,dimensions
           |   ,md5(${UDF_PREFIX}_map_to_string(dimensions)) dimensions_md5
           |   ,metric_name
           |   ,metric_value
           |  from (
           |    select
           |      time
           |     ,entity
           |     ,${UDF_PREFIX}_format_map(${UDF_PREFIX}_from_json_to_map(dimensions)) dimensions
           |     ,metric_name
           |     ,metric_value
           |    from bitlap_spark_mdm_view
           |  ) tt
           |) t
           |group by time, entity, dimensions_md5, metric_name
           |""".stripMargin)
    }

    val mdmCleanView = SparkUtils.withTempView(mdmView -> "bitlap_spark_mdm_clean_view") {
      spark.sql(s"""
           |select
           |  time
           | ,entity
           | ,dimensions
           | ,dense_rank() over(partition by time, entity order by metric_value_sum_over desc, dimensions_md5) dim_id
           | ,metric_name
           | ,metric_value
           |from (
           |  select
           |    *
           |   ,sum(metric_value) over(partition by time, entity, dimensions_md5, metric_name) metric_value_sum_over
           |  from bitlap_spark_mdm_clean_view
           |) t
           |""".stripMargin)
    }
    mdmCleanView.persist()

    this.writeMetric(mdmCleanView)
    this.writeMetricDim(mdmCleanView)
  }

  // TODO: metric_value support double
  private def writeMetric(df: DataFrame): Unit = {
    val rdf = SparkUtils.withTempView(df -> "bitlap_spark_mdm_clean_metric_view") {
      spark.sql(s"""
           |select
           |  time                                       as _time
           | ,metric_name                                as mk
           | ,time                                       as t
           | ,cbm                                        as m
           | ,bbm                                        as e
           | ,${UDF_PREFIX}_to_json(map(
           |   'tm', time
           |  ,'metricKey', metric_name
           |  ,'metricCount', metric_value
           |  ,'entityCount', ${UDF_PREFIX}_bbm_count(bbm)
           |  ,'entityUniqueCount', ${UDF_PREFIX}_rbm_count(bbm)
           | )) meta
           |from (
           |  select
           |    time
           |   ,metric_name
           |   ,sum(metric_value) metric_value
           |   ,${UDF_PREFIX}_cbm_aggr(pmod(dim_id, ${options.dimensionMaxId}), entity, cast(metric_value as bigint)) cbm
           |   ,${UDF_PREFIX}_bbm_aggr(pmod(dim_id, ${options.dimensionMaxId}), entity) bbm
           |  from bitlap_spark_mdm_clean_metric_view
           |  group by time, metric_name
           |) t
           |order by _time, mk, t
           |""".stripMargin)
    }
    // TODO: options
    rdf.write
      .mode(SaveMode.Overwrite)
      .partitionBy("_time")
      .parquet(new Path(tablePath, "m").toString)
  }

  private def writeMetricDim(df: DataFrame): Unit = {
    val rdf = SparkUtils.withTempView(df -> "bitlap_spark_mdm_clean_metric_dimension_view") {
      spark.sql(s"""
           |select
           |  time                                       as _time
           | ,metric_name                                as mk
           | ,dimension_key                              as dk
           | ,dimension_value                            as d
           | ,time                                       as t
           | ,cbm                                        as m
           | ,bbm                                        as e
           | ,${UDF_PREFIX}_to_json(map(
           |   'tm', time
           |  ,'metricKey', metric_name
           |  ,'metricCount', metric_value
           |  ,'entityCount', ${UDF_PREFIX}_bbm_count(bbm)
           |  ,'entityUniqueCount', ${UDF_PREFIX}_rbm_count(bbm)
           | )) meta
           |from (
           |  select
           |    time
           |   ,metric_name
           |   ,dimension_key
           |   ,dimension_value
           |   ,sum(metric_value) metric_value
           |   ,${UDF_PREFIX}_cbm_aggr(pmod(dim_id, ${options.dimensionMaxId}), entity, cast(metric_value as bigint)) cbm
           |   ,${UDF_PREFIX}_bbm_aggr(pmod(dim_id, ${options.dimensionMaxId}), entity) bbm
           |  from (
           |    select
           |      *
           |     ,explode(dimensions) as (dimension_key, dimension_value)
           |    from bitlap_spark_mdm_clean_metric_dimension_view
           |  ) tt
           |  group by time, metric_name, dimension_key, dimension_value
           |) t
           |order by _time, mk, dk, d, t
           |""".stripMargin)
    }
    rdf.write
      .mode(SaveMode.Overwrite)
      .partitionBy("_time")
      .parquet(new Path(tablePath, "md").toString)
  }

  private def initSpark(): Unit = {
    // register udf & udaf
    register[BBMAggr](s"${UDF_PREFIX}_bbm_aggr")
    register[CBMAggr](s"${UDF_PREFIX}_cbm_aggr")
    this.spark.udf.register(s"${UDF_PREFIX}_to_json", UDFUtils.toJson _)
    this.spark.udf.register(s"${UDF_PREFIX}_from_json_to_map", UDFUtils.fromJson _)
    this.spark.udf.register(s"${UDF_PREFIX}_format_map", UDFUtils.formatMap _)
    this.spark.udf.register(s"${UDF_PREFIX}_map_to_string", UDFUtils.map2String _)
    this.spark.udf.register(s"${UDF_PREFIX}_rbm_count", UDFUtils.rbmCount _)
    this.spark.udf.register(s"${UDF_PREFIX}_bbm_count", UDFUtils.bbmCount _)
    this.spark.udf.register(s"${UDF_PREFIX}_cbm_count", UDFUtils.cbmCount _)
  }

  private def register[T <: AggregateFunction](name: String)(implicit classTag: ClassTag[T]): Unit = {
    val (info, builder) = FunctionRegistryBase.build(name, None)(classTag)
    // need reflection
    val udf              = this.spark.udf
    val functionRegistry = udf.getClass.getDeclaredField("functionRegistry")
    functionRegistry.setAccessible(true)
    val fr = functionRegistry.get(udf).asInstanceOf[SimpleFunctionRegistry]
    fr.registerFunction(FunctionIdentifier(name), info, builder)
  }
}
