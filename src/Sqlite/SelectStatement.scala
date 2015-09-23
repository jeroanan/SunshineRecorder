import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

/**
 * Build and execute SQL Select statements.
 *
 * Currently select statements are pretty basic. A table name, a list of fields and a list of where clauses are
 * accepted. The where clauses are chained together using AND statements (so can't use OR, IN and such).
 *
 * The following fairly basic things are not yet supported by this set of functions:
 *
 *  + Table joins
 *  + No GROUP BY (and so no queries containing aggregate functions with > 1 column)
 *  + Nested subqueries
 *  + A tonne of other stuff.
 *
 * Why not? Because I don't need that functionality at the moment. In my view much of this can be worked around in
 * Sqlite by using a view, which this object can then query in a simpler manner. So I'd probably only make this extra
 * stuff if it turned out I would drown in views, or if I fancied the technical challenge.
 *
 * Queries made in this object are executed using a PreparedStatement, so that's at least the SQL injection thing kinda
 * covered off. 
 */
object SelectStatement {

  /**
   * Get a function that will build and execute SQL Select statements
   *
   * @param getConnection A function that will return a connection to Sqlite
   * @param toArray A function that will convert a ResultSet to a two-dimensional list of String
   * @return The select function
   */
  def getSelectStatement(getConnection: () => Connection,
      toArray: (ResultSet, List[String]) => List[List[String]]) = {

    /**
     * Build an excute a SQL Select statement
     *
     * @param tableName The table to select from
     * @param fields A list of field names to select
     * @param whereClauses a list of string tuples containing field name and value pairs. These are chained together
     *                     with AND clauses.
     * @return A two-dimensional list of String containing the resultset.
     */
    def select(tableName: String,
        fields: List[String],
        whereClauses: List[(String, String)] = List()) : List[List[String]] = {

      val connection = getConnection()
      val whereClause = buildWhereClause(whereClauses.map(x => x._1))
      val stmt = ("%s %s" format(buildSelectStatement(tableName, fields), whereClause))
	    val preparer = PrepareStatement.getPrepareStatement(connection)
      val prepStmt = preparer(stmt, whereClauses.map(x => x._2))

      val res = toArray(prepStmt.executeQuery(), fields)
      connection.close()
      res
    }

    select _
  }

  /**
   * Build the select/from portion of the select statement
   *
   * @param tableName The table to select from
   * @param fields The list of field names to select
   * @return The select/from portion of the select statement, i.e. SELECT field1, field2 FROM tableName
   */
  private def buildSelectStatement(tableName: String, fields: List[String]): String = {
		var fieldList = fields.reduce(_ + ", " + _)
    "SELECT %s FROM %s".format(fieldList, tableName)
  }

  /**
   * Build the where clause for the select statement ready for a prepared statement
   *
   * @param whereClauses The list of field names to include in the where clause
   * @return A where clause ready to be used as part of a prepared statement. e.g WHERE field1=? AND field2=?
   */
  private def buildWhereClause(whereClauses: List[String]) : String = {
		if (whereClauses.isEmpty) ""
		else "WHERE %s" format whereClauses.map("%s=?".format(_)).reduce(_ + " AND " + _)
    }
}
