package macros.scalikejdbcMeta

import scala.annotation.StaticAnnotation
import scala.meta._

class scalikejdbcMeta extends StaticAnnotation {

  inline def apply(defn: Any): Any = meta {
    this match {
      case q"new $_(${Lit.String(tableName)})" => core.MacroImpl(defn, tableName)
    }
  }
}

class scalikejdbcMetaDebug extends StaticAnnotation {

  inline def apply(defn: Any): Any = meta {
    val res = this match {
      case q"new $_(${Lit.String(tableName)})" => core.MacroImpl(defn, tableName)
    }
    println(res)
    res
  }
}
