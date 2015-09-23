import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import java.util.UUID

import scala.language.postfixOps

object SqliteStatements {

  def getConnection() = SqliteConnection.getConnection("test")

  /**
   * Execute a SQL statement
   *
   * @param sqlString The statement to be executed
   */
  def exec(sqlString: String) = {
      val connection = getConnection()
      val stmt = connection.createStatement()
      stmt.executeUpdate(sqlString)
      connection.close()
  }

  /**
   * Drop a table from the database if it exists
   *
   * @param tableName The table to be dropped
   */
  def dropTableIfExists(tableName: String) = {
      exec("DROP TABLE IF EXISTS %s".format(tableName))
  }

  /**
   * Create a new table in the database
   *
   * @param tableName The name of the table to create
   * @param fields The fields to include in the table. A list of tuples.
   *        Each tuple contains the name of a field and its data type.
   */
  def createTable(tableName: String, fields: List[(String, String)]) = {
	 val fieldsString = fields.map(x=> x._1 + " " + x._2).reduce(_ + ", " + _)
    val stmt = "CREATE TABLE %s (%s)".format(tableName, fieldsString)
    exec(stmt)
  }

  /**
   * Insert a row into a database table
   *
   * Currently only a simple insertion of values into all fields of a database is supported.
   *
   * The following are not currently supported:
   *    1. Inserting into just a subset of fields
   *    2. Inserting values based on a sub-query
   *
   * Integer values are saved as integers to the database. Other data types are saved as strings.
   *
   * Prepared statements are used to help mitigate the risk of SQL injection.
   *
   * @param tableName The table to insert the row into
   * @param fieldValues A list of values to be inserted into the new row.
   */
  def Insert(tableName: String, fieldValues: List[String]) = {

    val connection = getConnection()

    def buildInsertStatement() : String = {
		val placeholders = "(%s)" format(fieldValues).map(x => "?").reduce(_ + ", " + _)
      "INSERT INTO %s values %s".format(tableName, placeholders)
    }

    val preparer = PrepareStatement.getPrepareStatement(connection)
	 val prepStmt = preparer(buildInsertStatement(), fieldValues)
    prepStmt.executeUpdate()

    connection.close()
  }

  /**
   * Select rows from a database table.
   *
   * For more information see SelectStatement.scala
   *
   * @param tableName The name of the table to select values from
   * @param fields A list of the field names to select
   * @param whereClauses A list of tuples[String, String]. Each tuple contains the name of a field and its value.
	*                     The list of where clauses that will be applied to the select statement
   * @return A two-dimensional list of String. Each row contains a row from the result set
   */
  def select(tableName: String,
      fields: List[String],
      whereClauses: List[(String, String)] = List()) : List[List[String]] = {

    val selectStmt = SelectStatement.getSelectStatement(getConnection, resultSetToArray)
    selectStmt(tableName, fields, whereClauses)
  }

  /**
   * Get the highest value of the id field for a table
   *
   * @param tableName The table to get the highest id for
   * @return The highest value of the id field. If there are no rows then 0 is returned.
   */
  def getMaxId(tableName: String) : Integer = {
    val result = select(tableName, List[String]("MAX(id)"))
	 if (result.head.head == null) 0
    else result.head.head.toInt
  }

  /**
	* Get a new id for the given table
	*
	* @param tableName The name of the table to get a new id for
	* @return The new id. This is the current maximum id plus one
	*/
  def getNewId(tableName: String) = {
	 getMaxId(tableName) + 1
  }

  /**
   * Find whether the given table exists in the database
   *
   * @param tableName The table to check for existence
   * @return True if the table exists; false otherwise
   */
  def tableExists(tableName: String) : Boolean = {
    getAllTableNames contains tableName
  }

  /*
	* Get the names of all tables in the database
	*
	* @return a list of string. Each element is a table name.
	*/
  def getAllTableNames() : List[String] = {
	 val masterTable = "sqlite_master"
    val selectFields = List[String]("name")
    val whereClause = List[(String, String)](("type", "table"))
	 val resultSet = select(masterTable, selectFields, whereClause)
	 resultSet.map(x => x(0))
  }

  /**
   * Delete a row from a database table based on its id field
   *
   * @param tableName The name of the table to delete the row from
   * @param id The id of the row to be deleted
   */
  def deleteById(tableName: String, id: Integer) = {
    val statement = "DELETE FROM %s WHERE id=%d" format(tableName, id)
    val conn = getConnection()
    val s = conn.createStatement()
    s.executeUpdate(statement)
    conn.close()
  }

  /**
   * Create a database table if it does not already exist
   *
   * @param tableName The name of the table to create
   * @param fields The fields to be added included in the table. An array of tuples.
   *               Each tuple contains the name and data type of a field.
   * @return true if the table was created, false otherwise
   */
  def createTableIfDoesNotExist(tableName: String, fields: List[(String, String)]) : Boolean = {
    if (!tableExists(tableName)) {
      createTable(tableName, fields)
      true
    } else {
      false
    }
  }

  /**
   * Check whether the given column exists in the given table
   *
   * @param tableName The name of the table to check
   * @param columnName The name of the column to check
   * @return true if a column with this name exists in the table; false otherwise
   */
  def columnExists(tableName: String, columnName: String) : Boolean = {

    val tableInfo = getTableInfo(tableName)
    val columns = getTableColumnNames(tableInfo)
    columns.filter(_ == columnName).length > 0
  }

  /**
   * Get the names of all columns from a table given the result of PRAGMA table_info
   *
   * @param connection The connection to the SQL database
   * @param tableInfo A two-dimensional list containing the result of PRAGMA table_info. Each row contains an entry.
   * @return A list of String containing the column names
   */
  def getTableColumnNames(tableInfo: List[List[String]]) : List[String] = {
    getTableColumnNamesAndTypes(tableInfo).map(x => x._1)
  }

  /**
	* Get the column names and types from the given table info
	*
	* @param tableInfo A result set from getTableInfo
	* @returns a list of tuple[String, String]. Each entry contains the name and type of a column
	*/
  def getTableColumnNamesAndTypes(tableInfo: List[List[String]]): List[(String, String)] = {
    tableInfo.map(x => (x(1), x(2)))
  }

  /**
   * Get information about a table
   *
   * @param connection The connection to the SQL database
   * @param tableName The name of the table to get information about
   * @return A two-dimensional list containing the table records. Each row contains a record.
   */
  def getTableInfo(tableName: String) : List[List[String]] = {
    val sql = "PRAGMA table_info(%s)" format(tableName)
    val connection = getConnection()
    val stmt = connection.createStatement()
    val result = stmt.executeQuery(sql)

    val fields = List("cid", "name", "type", "notnull", "dflt_value", "pk")
    val a = resultSetToArray(result, fields)
    connection.close()
    a
  }

  /**
   * Add a column with the given name and type to a database table
   *
   * @param connection The connection to the SQL database
   * @param tableName The name of the table to add the column to
   * @param columnName The name of the column to be added
   * @param columnType The data type of the column to be added
   */
  def addColumn(tableName: String, columnName: String, columnType: String) = {
    val sql = "ALTER TABLE %s ADD %s %s" format(tableName, columnName, columnType)
    exec(sql)
  }

  /**
   * Add a column to a database column if it doesn't already exist
   *
   * @param connection The connection to the SQL database
   * @param tableName The name of the table to add the column to
   * @param columnName The name of the column to be added
   * @param columnType The data type of the column to be added
   */
  def addColumnIfDoesntExist(tableName: String, columnName: String, columnType: String) = {
    if (!columnExists(tableName, columnName)) addColumn(tableName, columnName, columnType)
  }

  /**
   * Get columns that exist in the given table that aren't in the given list of column names
   *
   * @param tableName The name of the table to check
   * @param columns The list of columns that are known to exist
   * @return A list of columns that appear in the table that aren't in columns.
   */
  def getExtraColumns(tableName: String, columns: List[String]) : List[String] = {
    val tableColumns = getTableColumnNames(getTableInfo(tableName))
    tableColumns.filter(!columns.contains(_))
  }

  /**
	* Get tables that exist in the databse that aren't in the given list of table names
	*
	* @param tables A list of table names
	* @return A list of table names that exist in the database but aren't in tables
	*/
  def getExtraTables(tables: List[String]) : List[String] = {
	 val allTables = getAllTableNames()
	 allTables.filter(!tables.contains(_))
  }

  /**
  * Makes a ResultSet into a two-dimensional array of string representing the tabular data.
  *
  * @param resultSet The ResultSet to transform
  * @param columnNames The list of column names in the ResultSet
  * @return A two-dimensional list representing the data. Each row contains a record
  */
  def resultSetToArray(resultSet: ResultSet, columnNames: List[String]) : List[List[String]] = {

     @annotation.tailrec
     def iter(table : List[List[String]]) : List[List[String]] = {
       if (!resultSet.next()) table
       else {
          val row = columnNames.map(resultSet.getString(_))
          iter(table :+ row)
       }
     }

     iter(List(List())).filter(_.length > 0)
   }

  /**
   * Drop the given columns from the given database table
   *
   * Because Sqlite has no direct support for dropping columns this has to be done by making a temporary table that is
   * the old one without the columns to be dropped, copying data from the old table to the new one, dropping the old
   * table,recreating it without the dropped columns, copying the data to it from the temp table and then drpping the
   * temp table.
   *
   * @param tableName The table name to drop columns from
   * @param columns A list of column names to drop. If the list is empty then nothing happens.
   */
  def dropColumns(tableName: String, columns: List[String]) = {

     if (!columns.isEmpty)
     {
       val existingColumns = getTableColumnNamesAndTypes(getTableInfo(tableName))
       val columnsToKeep = existingColumns.filter(x => !columns.contains(x._1))

       val tmpTableName = "%s%s" format(tableName, UUID.randomUUID().toString().replace("-", ""))
       createTableIfDoesNotExist(tmpTableName, columnsToKeep)

       val columnNames = columnsToKeep.map(_._1)

       val existingData = select(tableName, columnNames)
       existingData.foreach(Insert(tmpTableName, _))

       dropTableIfExists(tableName)

       createTableIfDoesNotExist(tableName, columnsToKeep)
       existingData.foreach(Insert(tableName, _))

       dropTableIfExists(tmpTableName)
     }
  }

  /**
   * Update the fields in a table.
   *
   * @param tableName The name of the table to update
   * @param columns A list of tuple of String containing the column names and values to update.
   * @param whereClauses A list of tuple of String containing the column names and values to use in the where clause
   */
  def updateTable(tableName: String, columns: List[(String, String)], whereClauses: List[(String, String)]) = {

	 def buildWhereClause(whereClauses: List[(String, String)]) = {
		if (whereClauses.isEmpty) ""
		else "WHERE %s" format(whereClauses.map(x => "%s=?" format(x._1)).reduce(_ + " AND " + _))
	 }
	 val columnsPlaceholders = columns.map(x => "%s=?" format(x._1)).reduce(_ + ", " + _)

	 val stmt = "UPDATE %s SET %s %s" format(tableName, columnsPlaceholders, buildWhereClause(whereClauses))

	 val connection = getConnection()
	 val preparer = PrepareStatement.getPrepareStatement(connection)
	 val prepStmt = preparer(stmt, columns.map(x => x._2) ++ whereClauses.map(x => x._2))
   prepStmt.executeUpdate()

   connection.close()
  }

  /**
   * Count the number of occurrences in a table where a where clause applies
   *
   * @param tableName The name of the table to count from
   * @param whereClauses A list of tuple of String containing the column names and values to use in the where clause
   * @return The number of rows from the count   
   */
  def countWhere(tableName: String, whereClauses: List[(String, String)]) = {
	 select(tableName, List("COUNT(*)"), whereClauses).head.head.toInt
  }
}
