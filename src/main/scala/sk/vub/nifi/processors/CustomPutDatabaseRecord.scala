package sk.vub.nifi.processors

import java.sql._
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.Collections

import io.circe.parser.parse
import org.apache.nifi.annotation.behavior.InputRequirement
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement
import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.annotation.lifecycle.OnScheduled
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.dbcp.DBCPService
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.processor.util.pattern.RollbackOnFailure
import org.apache.nifi.processor.{ProcessContext, ProcessSession, Relationship}
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.record.Record
import sk.vub.nifi.ops._
import sk.vub.nifi.processors.CustomPutDatabaseRecord._
import sk.vub.nifi.{FlowFileNotNull, ScalaProcessor, _}

import scala.collection.JavaConverters._

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags(scala.Array("custom", "put", "database", "record"))
class CustomPutDatabaseRecord extends ScalaProcessor with FlowFileNotNull {

  def properties: List[PropertyDescriptor] = List(
    P.recordReader,
    P.dbpcService,
    P.statementType,
    P.tableMapping,
    P.catalogName,
    P.schemaName,
    P.tableName,
    P.queryTimeout,
    P.maxBatchSize,
    P.autoCommit
  )

  def relationships: Set[Relationship] = Set(R.success, R.failure)

  val format: DateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME

  var connection: Connection = _

  def setPreparedStatement(record: Record, name: String, value: String, index: Int, preparedStatement: PreparedStatement): Unit =
    try {
      value match {
        case "boolean" =>
          if (record.getAsBoolean(name) == null) preparedStatement.setNull(index, Types.BOOLEAN)
          else preparedStatement.setBoolean(index, record.getAsBoolean(name))
        case "convert-boolean-tinyint" =>
          if (record.getAsBoolean(name) == null) preparedStatement.setNull(index, Types.TINYINT)
          else {
            val res = if (record.getAsBoolean(name)) 1 else 0
            preparedStatement.setByte(index, res.toByte)
          }
        case "string" | "char" | "varchar" =>
          if (record.getAsString(name) == null) preparedStatement.setNull(index, Types.VARCHAR)
          else preparedStatement.setString(index, record.getAsString(name))
        case "tinyint" =>
          if (record.getAsInt(name) == null) preparedStatement.setNull(index, Types.TINYINT)
          else preparedStatement.setByte(index, record.getAsInt(name).toByte)
        case "smallint" =>
          if (record.getAsInt(name) == null) preparedStatement.setNull(index, Types.SMALLINT)
          else preparedStatement.setShort(index, record.getAsInt(name).toShort)
        case "int" =>
          if (record.getAsInt(name) == null) preparedStatement.setNull(index, Types.INTEGER)
          else preparedStatement.setInt(index, record.getAsInt(name))
        case "long" | "bigint" =>
          if (record.getAsLong(name) == null) preparedStatement.setNull(index, Types.BIGINT)
          else preparedStatement.setLong(index, record.getAsLong(name))
        case "double" =>
          if (record.getAsDouble(name) == null) preparedStatement.setNull(index, Types.DOUBLE)
          else preparedStatement.setDouble(index, record.getAsDouble(name))
        case "float" =>
          if (record.getAsFloat(name) == null) preparedStatement.setNull(index, Types.REAL)
          else preparedStatement.setFloat(index, record.getAsFloat(name))
        case "decimal" | "numberic" =>
          if (record.getAsString(name) == null) preparedStatement.setNull(index, Types.DECIMAL)
          else preparedStatement.setBigDecimal(index, new java.math.BigDecimal(record.getAsString(name)))
        case "date-timestamp" =>
          if (record.getAsLong(name) == null) preparedStatement.setNull(index, Types.DATE)
          else preparedStatement.setDate(index, new Date(record.getAsLong(name)))
        case "date-iso" =>
          if (record.getAsString(name) == null) preparedStatement.setNull(index, Types.DATE)
          else {
            preparedStatement.setDate(index, new Date(
              Instant.from(format.parse(record.getAsString(name))).toEpochMilli))
          }
        case "timestamp" =>
          if (record.getAsLong(name) == null) preparedStatement.setNull(index, Types.TIMESTAMP)
          else preparedStatement.setTimestamp(index, new Timestamp(record.getAsLong(name)))
        case "timestamp-iso" =>
          if (record.getAsString(name) == null) preparedStatement.setNull(index, Types.TIMESTAMP)
          else {
            preparedStatement.setTimestamp(index, new Timestamp(
              Instant.from(format.parse(record.getAsString(name))).toEpochMilli))
          }
        case _ =>
          throw new RuntimeException(s"Unsupported type in setPreparedStatement")
      }
    } catch {
      case e: RuntimeException =>
        getLogger.error("Error with setPreparedStatement", e)
        throw e
    }

  def addPreparedStatementToBatch(record: Record, preparedStatement: PreparedStatement, mapping: Seq[(String, String)]): Unit = {
    preparedStatement.clearParameters()
    for {
      ((name, value), index) <- mapping.zipWithIndex
    } {
      setPreparedStatement(record, name, value, index + 1, preparedStatement)
    }
    preparedStatement.addBatch()
  }

  case class ColumnDescription(name: String, dataType: Int)

  @OnScheduled
  def onScheduled(implicit processContext: ProcessContext): Unit = {
    val dBCPService: DBCPService = P.dbpcService.asControllerService[DBCPService]
    val autocommit: Boolean = P.autoCommit.get.toBoolean
    val catalogName: Option[String] = P.catalogName.evaluate.getOpt
    val schemaName: Option[String] = P.schemaName.evaluate.getOpt
    val tableName: String = P.tableName.evaluate.get
    val tableMapping: String = P.tableMapping.evaluate.get

    // initialize connection
    withThrowableAsEither(()) { _ =>
      connection = dBCPService.getConnection(Collections.emptyMap())
      connection.setAutoCommit(autocommit)

      // retrieve column names from table
      val dmd = connection.getMetaData()
      val rs = dmd.getColumns(catalogName.orNull, schemaName.orNull, tableName, "%")

      val columns = Iterator.continually(rs).takeWhile(_.next()).map ( rs =>
        // java.sql.Types
        ColumnDescription(rs.getString("COLUMN_NAME"), rs.getInt("DATA_TYPE"))
      ).toList

      columns.

      println(s"metadata columns $columns")
    }
  }

  override def onTrigger(flowFile: FlowFile)(implicit processContext: ProcessContext, processSession: ProcessSession): Unit = {
    val readerFactory: RecordReaderFactory = P.recordReader.asControllerService[RecordReaderFactory]

    val statementType = P.statementType.evaluate(flowFile).get

//    val sqlStatement = P.sqlStatement.evaluate(flowFile).get
//    val mapping = parse(context
//      .getProperty(P.mapping)
//      .evaluateAttributeExpressions(flowFile)
//      .getValue).getOrElse(throw new Exception("Invalid JSON format"))
//      .as[Seq[Map[String, String]]].getOrElse(throw new Exception(s"Cannot parse property " +
//      s"Mapping to JSON array")).map(_.head)
//    val autoCommit = P.autoCommit.evaluate(flowFile).get.toBoolean
//    val batchSize = P.batchSize.evaluate(flowFile).get.toInt
//    val queryTimeout = P.queryTimeout.evaluate(flowFile).get.toInt
//
//    var counter = 0
//    withThrowableAsEither(()) { _ =>
//      withResource(dBCPService.getConnection(flowFile.getAttributes)) { connection =>
//        connection.setAutoCommit(autoCommit)
//        withResource(connection.prepareStatement(sqlStatement)) { preparedStatement =>
//          preparedStatement.setQueryTimeout(queryTimeout)
//          withResource(flowFile.read()) { in =>
//            withResource(readerFactory.createRecordReader(flowFile, in, logger)) { reader =>
//              val batches =
//                Iterator
//                  .continually(reader.nextRecord())
//                  .takeWhile(_ != null)
//                  .sliding(batchSize, batchSize)
//              for {
//                batch <- batches
//              } {
//                for {
//                  record <- batch
//                } {
//                  addPreparedStatementToBatch(record, preparedStatement, mapping)
//                  counter += 1
//                }
//                preparedStatement.executeBatch()
//                if (!autoCommit) connection.commit()
//              }
//            }
//          }
//        }
//      }
//    } match {
//      case Left(e) =>
//        logger.error("errors", e)
//        flowFile
//          .putAttribute("errors", e.getMessage + e.getCause.getMessage)
//          .transfer(R.failure)
//      case Right(_) =>
//        flowFile
//          .putAttribute("processed", counter.toString)
//          .transfer(R.success)
//    }
  }
}

object CustomPutDatabaseRecord {
  object P {
    val recordReader: PropertyDescriptor = new PropertyDescriptor.Builder()
      .name("put-db-record-record-reader")
      .displayName("Record Reader")
      .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
      .identifiesControllerService(classOf[RecordReaderFactory])
      .required(true)
      .build()

    val dbpcService: PropertyDescriptor = new PropertyDescriptor.Builder()
      .name("put-db-record-dcbp-service")
      .displayName("Database Connection Pooling Service")
      .description("The Controller Service that is used to obtain a connection to the database for sending records.")
      .identifiesControllerService(classOf[DBCPService])
      .required(true)
      .build()

    val statementType: PropertyDescriptor = new PropertyDescriptor.Builder()
      .name("put-db-record-statement-type")
      .displayName("Statement Type")
      .description("Specifies the type of SQL Statement to generate.")
      .allowableValues("INSERT", "UPSERT")
      .required(true)
      .build()

    val tableMapping: PropertyDescriptor = new PropertyDescriptor.Builder()
      .name("put-db-record-table-mapping")
      .displayName("Statement Mapping")
      .description("Mapping of columns to database datatypes")
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)  // TODO Custom validator (JSON with types)
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .defaultValue("{}")
      .required(true)
      .build()

    val catalogName = new PropertyDescriptor.Builder()
      .name("put-db-record-catalog-name")
      .displayName("Catalog Name")
      .description("The name of the catalog that the statement should update. " +
        "This may not apply for the database that you are updating. In this case, leave the field empty")
      .required(false)
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build()

    val schemaName = new PropertyDescriptor.Builder()
      .name("put-db-record-schema-name")
      .displayName("Schema Name")
      .description("The name of the schema that the table belongs to. This may not apply for the database " +
        "that you are updating. In this case, leave the field empty")
      .required(false)
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build()

    val tableName = new PropertyDescriptor.Builder()
      .name("put-db-record-table-name")
      .displayName("Table Name")
      .description("The name of the table that the statement should affect.")
      .required(true)
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build()

    val queryTimeout = new PropertyDescriptor.Builder()
      .name("put-db-record-query-timeout")
      .displayName("Max Wait Time")
      .description("The maximum amount of time allowed for a running SQL statement "
        + ", zero means there is no limit. Max time less than 1 second will be equal to zero.")
      .defaultValue("0 seconds")
      .required(true)
      .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
      .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
      .build();

    val maxBatchSize = new PropertyDescriptor.Builder()
      .name("put-db-record-max-batch-size")
      .displayName("Maximum Batch Size")
      .description("Specifies maximum batch size for INSERT and UPDATE statements. "
        + " Zero means the batch size is not limited.")
      .defaultValue("0")
      .required(false)
      .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
      .build();

    val autoCommit: PropertyDescriptor = new PropertyDescriptor.Builder()
      .name("auto-commit")
      .displayName("SQL auto commit")
      .description(
        """SQL auto commit commits after every row insert,
          | set to false to commit whole batches (better performance)""".stripMargin)
      .required(true)
      .allowableValues("true", "false")
      .defaultValue("false")
      .build()
  }

  object R {
    val success: Relationship = new Relationship.Builder()
      .name("success")
      .description("Successfully created FlowFile from SQL query result set.")
      .build()

    val failure: Relationship = new Relationship.Builder()
      .name("failure")
      .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
        + "such as an invalid query or an integrity constraint violation")
      .build()
  }
}
