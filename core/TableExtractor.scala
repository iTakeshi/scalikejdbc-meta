package macros.scalikejdbcMeta.core

import com.typesafe.config.ConfigFactory
import java.io.File
import java.sql.Types._
import scala.collection.immutable.Seq
import scala.meta._
import scalikejdbc._
import scalikejdbc.metadata._

class TableExtractor(tableName: String) {

  private[this] val conf = ConfigFactory.parseFile(new File("conf/application.conf")).resolve()
  Class.forName(conf.getString("db.default.driver"))
  ConnectionPool.singleton(
    conf.getString("db.default.url"),
    conf.getString("db.default.username"),
    conf.getString("db.default.password"),
  )

  val table: Table =
    using(ConnectionPool.borrow())(conn => DB(conn).getTable(tableName)) match {
      case Some(table) => table
      case None => throw new IllegalStateException(s"the table named `$tableName` does not exist in DB")
    }

  object TypeName {
    val Any = "Any"
    val AnyArray = "Array[Any]"
    val ByteArray = "Array[Byte]"
    val Long = "Long"
    val Boolean = "Boolean"
    val DateTime = "DateTime"
    val LocalDate = "LocalDate"
    val LocalTime = "LocalTime"
    val Timestamp = "DateTime"
    val String = "String"
    val Byte = "Byte"
    val Int = "Int"
    val Short = "Short"
    val Float = "Float"
    val Double = "Double"
    val Blob = "Blob"
    val Clob = "Clob"
    val Ref = "Ref"
    val Struct = "Struct"
    val BigDecimal = "BigDecimal" // scala.math.BigDecimal
  }

  def scalaType(column: Column): Type = {
    val rawType = column.typeCode match {
      case ARRAY => TypeName.AnyArray
      case BIGINT => TypeName.Long
      case BINARY => TypeName.ByteArray
      case BIT => TypeName.Boolean
      case BLOB => TypeName.Blob
      case BOOLEAN => TypeName.Boolean
      case CHAR => TypeName.String
      case CLOB => TypeName.Clob
      case DATALINK => TypeName.Any
      case DATE => TypeName.LocalDate
      case DECIMAL => TypeName.BigDecimal
      case DISTINCT => TypeName.Any
      case DOUBLE => TypeName.Double
      case FLOAT => TypeName.Float
      case INTEGER => TypeName.Int
      case JAVA_OBJECT => TypeName.Any
      case LONGVARBINARY => TypeName.ByteArray
      case LONGVARCHAR => TypeName.String
      case NULL => TypeName.Any
      case NUMERIC => TypeName.BigDecimal
      case OTHER => TypeName.Any
      case REAL => TypeName.Float
      case REF => TypeName.Ref
      case SMALLINT => TypeName.Short
      case STRUCT => TypeName.Struct
      case TIME => TypeName.LocalTime
      case TIMESTAMP => TypeName.Timestamp
      case TINYINT =>
        if (column.typeName.toLowerCase == "tinyint(1)") TypeName.Boolean
        else TypeName.Byte
      case VARBINARY => TypeName.ByteArray
      case VARCHAR => TypeName.String
      case _ => TypeName.Any
    }

    if (column.isRequired) Type.Name(rawType)
    else Type.Apply(Type.Name("Option"), Seq(Type.Name(rawType)))
  }

  def defaultValue(column: Column): Option[String] = Option(column.defaultValue) match {
    case Some(v) =>
      Some {
        val default =
          if (v == "CURRENT_TIMESTAMP" || v == "CURRENT_TIMESTAMP(3)") {
            "DateTime.now"
          } else if (scalaType(column).asInstanceOf[Type.Name].value == TypeName.Boolean) {
            if (v == "0") {
              "false"
            } else {
              "true"
            }
          } else {
            s""""$v""""
          }
        if (column.isRequired) default
        else s"Some($default)"
      }
    case None =>
      if (column.isRequired) None
      else Some("None")
  }

}
