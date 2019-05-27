package macros.scalikejdbcMeta.core

import macros.formatter.core.MacroImpl.appendFormatter
import scala.collection.immutable.Seq
import scala.meta._
import scala.util.matching.Regex
import scalikejdbc._
import scalikejdbc.metadata._

class MacroImpl(tableName: String) {

  val extractor = new TableExtractor(tableName)

  def apply(defn: Stat): Stat = {
    val block = (defn match {
      case Term.Block(Seq(classDefn: Defn.Class, objectDefn: Defn.Object)) =>
        Term.Block(Seq(expandClass(classDefn, tableName), expandObject(objectDefn, tableName)))
      case classDefn: Defn.Class => expandClass(classDefn, tableName, withCompanion = true)
    }).asInstanceOf[Term.Block]
    Term.Block(Seq(
      block.stats(0),
      appendFormatter(block.stats(1).asInstanceOf[Defn.Object], block.stats(0).asInstanceOf[Defn.Class])
    ))
  }

  private[this] def toCamel(s: String): String = {
    val split = s.split("_")
    val tail = split.tail.map(x => x.head.toUpper + x.tail)
    split.head + tail.mkString
  }

  private[this] def fieldTerm(column: Column) = Term.Name(toCamel(column.name))

  private[this] def columnsToParams(columns: List[Column], withDefaultValue: Boolean = true) =
    if (withDefaultValue) columns.map(c => Term.Param(
      Seq(),
      fieldTerm(c),
      Some(extractor.scalaType(c)),
      extractor.defaultValue(c).map(_.parse[Term].get)
    ))
    else columns.map(c => Term.Param(
      Seq(),
      fieldTerm(c),
      Some(extractor.scalaType(c)),
      None
    ))

  private[this] def getBasicTerms(modelName: String, tableName: String) = {
    val termName = Term.Name(modelName)
    val typeName = Type.Name(modelName)
    val table = extractor.table
    (modelName, termName, typeName, table)
  }

  def expandClass(classDefn: Defn.Class, tableName: String, withCompanion: Boolean = false): Stat = {
    val q"""
      ..$mods class $tname[..$tparams] ..$ctorMods (...$paramss) extends { ..$earlyStats } with ..$ctorcalls {
        $selfParam =>
        ..$stats
      }
    """ = classDefn

    assert(mods.exists(_.isInstanceOf[Mod.Case]), "@scalikejdbcMeta can annotate only case classes")

    val (modelName, termName, typeName, table) = getBasicTerms(tname.value, tableName)

    val paramssWithColumns = {
      val (head :: tail) = paramss
      (columnsToParams(table.columns) ++ head) :: tail
    }

    val classTree = q"""
      ..$mods class $typeName[..$tparams] ..$ctorMods (...$paramssWithColumns) extends { ..$earlyStats } with ..$ctorcalls {
        $selfParam =>
        ..$stats;

        def save()(implicit session: DBSession = $termName.autoSession): $typeName = $termName.save(this)(session)

        def destroy()(implicit session: DBSession = $termName.autoSession): Unit = $termName.destroy(this)(session)

      }
    """

    // generate empty companion and expand it, if necessary.
    if (withCompanion) Term.Block(Seq(classTree, expandObject(q"object $termName", tableName)))
    else classTree
  }

  def expandObject(objectDefn: Defn.Object, tableName: String): Stat = {

    val q"""
      ..$mods object $tname extends { ..$earlyStats } with ..$ctorcalls {
        $selfParam =>
        ..$stats
      }
    """ = objectDefn

    val (modelName, termName, typeName, table) = getBasicTerms(tname.value, tableName)

    val pkColumns: List[Column] = {
      val res = table.columns.filter(_.isPrimaryKey)
      if (res.isEmpty) table.columns else res
    }

    val pkParams: List[Term.Param] = columnsToParams(pkColumns, false)

    val pkArgs: List[Term.Arg] = pkColumns.map(fieldTerm(_))

    def pkWhereDef = {

      def eqArgs(c: Column): List[Term.Arg] =
        List(arg"$termName.syn.${fieldTerm(c)}", arg"${fieldTerm(c)}")

      pkColumns match {
        case Nil => q"sqlBuilder.where"
        case head :: tail =>
          tail.foldLeft(q"sqlBuilder.where.eq(..${eqArgs(head)})")((query, c) => q"$query.and.eq(..${eqArgs(c)})")
      }
    }

    val aiColumnOpt: Option[Column] = table.columns.filter(_.isAutoIncrement) match {
      case Nil => None
      case c :: Nil => Some(c)
      case _ => throw new Exception("@scalikejdbcMeta does not support multiple auto-increment columns")
    }
    val notAiColumns: List[Column] = table.columns.filter(!_.isAutoIncrement)

    val createDefBody = aiColumnOpt match {
      case Some(c) =>
        q"""
        val generatedKey = withSQL(createQuery(..${notAiColumns.map(fieldTerm(_))})).updateAndReturnGeneratedKey.apply()
        $termName(..${arg"${fieldTerm(c)} = generatedKey" :: notAiColumns.map(c =>
          arg"${fieldTerm(c)} = ${fieldTerm(c)}")})
      """
      case None => q"""
        withSQL(createQuery(..${notAiColumns.map(fieldTerm(_))})).update.apply()
        $termName(..${notAiColumns.map(c => arg"${fieldTerm(c)} = ${fieldTerm(c)}")})
      """
    }

    val batchSQL = s"""
      sql"insert into $tableName(${notAiColumns.map(_.name).mkString(", ")}) values (${notAiColumns
      .map(c => s"{${fieldTerm(c).value}}")
      .mkString(", ")})"
    """.parse[Term].get

    // TODO: move formatter related stuffs to macros.formatter
    q"""
      ..$mods object $termName extends { ..$earlyStats } with ..${s"SQLSyntaxSupport[$typeName]"
      .parse[Ctor.Call]
      .get :: ctorcalls.toList} {
        $selfParam =>

        ..$stats;

        implicit class ${Type.Name(s"${modelName}PkSelectWhereImplicit")}[A](sqlBuilder: SelectSQLBuilder[A]) {
          def pkWhere(..$pkParams): ConditionSQLBuilder[A] = {
            ${pkWhereDef}
          }
        }

        implicit class ${Type.Name(s"${modelName}PkUpdateOrDeleteWhereImplicit")}(sqlBuilder: WhereSQLBuilder[UpdateOperation]) {
          def pkWhere(..$pkParams): ConditionSQLBuilder[UpdateOperation] = {
            ${pkWhereDef}
          }
        }

        override val tableName = $tableName

        override val columns = Seq(..${table.columns.map(c => arg"${c.name}")})

        val syn = $termName.syntax($tableName)

        val rn = syn.resultName

        def apply(resultSet: WrappedResultSet): $typeName =
          new ${modelName.parse[Ctor.Call].get}(..${table.columns.map { c =>
            arg"${fieldTerm(c)} = resultSet.get(rn.${fieldTerm(c)})"
          }})

        def opt(resultSet: WrappedResultSet): Option[$typeName] =
          resultSet.anyOpt(rn.${fieldTerm(pkColumns.head)}).map(_ => $termName(resultSet))

        override val autoSession = AutoSession

        def findQuery(..$pkParams): SQLBuilder[_] = select.from($termName as syn).pkWhere(..$pkArgs)

        def find(..$pkParams)(implicit session: DBSession = autoSession): Option[$typeName] =
          withSQL(findQuery(..$pkArgs)).map($termName.apply).single.apply()

        def findAllQuery: SQLBuilder[_] = select.from($termName as syn)

        def findAll()(implicit session: DBSession = autoSession): List[$typeName] =
          withSQL(findAllQuery).map($termName.apply).list.apply()

        def countAllQuery: SQLBuilder[_] = select(sqls.count).from($termName as syn)

        def countAll()(implicit session: DBSession = autoSession): Long = withSQL(countAllQuery).map(_.long(1)).single.apply().get

        def findByQuery(where: SQLSyntax): SQLBuilder[_] = select.from($termName as syn).where.append(where)

        def findBy(where: SQLSyntax)(implicit session: DBSession = autoSession): Option[$typeName] =
          withSQL(findByQuery(where)).map($termName.apply).single.apply()

        def findAllByQuery(where: SQLSyntax): SQLBuilder[_] = select.from($termName as syn).where.append(where)

        def findAllBy(where: SQLSyntax)(implicit session: DBSession = autoSession): List[$typeName] =
          withSQL(findAllByQuery(where)).map($termName.apply).list.apply()

        def countByQuery(where: SQLSyntax): SQLBuilder[_] = select(sqls.count).from($termName as syn).where.append(where)

        def countBy(where: SQLSyntax)(implicit session: DBSession = autoSession): Long =
          withSQL(countByQuery(where)).map(_.long(1)).single.apply().get

        def createQuery(..${columnsToParams(notAiColumns)}): SQLBuilder[_] =
          insert.into($termName).namedValues(..${notAiColumns.map(c => arg"column.${fieldTerm(c)} -> ${fieldTerm(c)}")})

        def create(..${columnsToParams(notAiColumns)})(implicit session: DBSession = autoSession): $typeName = $createDefBody

        def createByEntityQuery(entity: $typeName): SQLBuilder[_] = createQuery(..${notAiColumns.map(c => arg"entity.${fieldTerm(c)}")})

        def createByEntity(entity: $typeName)(implicit session: DBSession = autoSession): $typeName =
          create(..${notAiColumns.map(c => arg"entity.${fieldTerm(c)}")})

        def batchInsert(entities: Seq[$typeName])(implicit session: DBSession = autoSession): Seq[Int] = {
          val params: Seq[Seq[(Symbol, Any)]] = entities.map(entity =>
            Seq(..${notAiColumns.map(c => arg"${Term.Name(s"'${fieldTerm(c).value}")} -> entity.${fieldTerm(c)}")})
          )
          $batchSQL.batchByName(params: _*).apply()
        }

        def saveQuery(entity: $typeName): SQLBuilder[_] = update($termName).set(
          ..${table.columns.map(c => arg"column.${fieldTerm(c)} -> entity.${fieldTerm(c)}")}
        ).pkWhere(..${pkColumns.map(c => arg"entity.${fieldTerm(c)}")})

        def save(entity: $typeName)(implicit session: DBSession = autoSession): $typeName = {
          withSQL(saveQuery(entity)).update.apply()
          entity
        }

        def destroyQuery(entity: $typeName): SQLBuilder[_] =
          delete.from($termName).pkWhere(..${pkColumns.map(c => arg"entity.${fieldTerm(c)}")})

        def destroy(entity: $typeName)(implicit session: DBSession = autoSession): Unit = withSQL(destroyQuery(entity)).update.apply()
      }
    """
  }

}

object MacroImpl {

  def apply(defn: Stat, tableName: String): Stat = new MacroImpl(tableName)(defn)
}
