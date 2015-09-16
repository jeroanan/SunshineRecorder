import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement

import Sqlite.SelectStatement

object SqliteStatements {  
  
  /**
   * Execute a SQL statement
   *  
   * @param connection The connection to the SQL database
   * @param sqlString The statement to be executed
   */
  def exec(connection: Connection, sqlString: String) = {
      val stmt = connection.createStatement()    
      stmt.executeUpdate(sqlString)
  }
   
  /**
   * Drop a table from the database if it exists
   *   
   * @param connection The connection to the SQL database
   * @param tableName The database to be dropped
   */
  def dropTableIfExists(connection: Connection, tableName: String) = {
      exec(connection, "DROP TABLE IF EXISTS %s".format(tableName))
  }  
  
  /**
   * Create a new table in the database
   * 
   * @param connection The connection to the SQL database
   * @param tableName The name of the table to create
   * @param fields The fields to include in the table. A list of tuples. 
   *        Each tuple contains the name of a field and its data type.
   */
  def createTable(connection: Connection, tableName: String, fields: Array[(String, String)]) = {
    
    def buildCreateTableStatement(fields: Array[(String, String)]) : String = {
      
        def makeFieldListString(fields: Array[(String, String)]) : String = {
          
          @annotation.tailrec
          def makeFieldListIter(fieldListString: String, fields: Array[(String, String)]) : String = {        
            if (fields.length == 0) fieldListString
            else {
             val thisField = fields.head
             val comma = if (fieldListString == "") "" else ", "
             val s = "%s%s%s %s".format(fieldListString, comma, thisField._1, thisField._2)
             
             makeFieldListIter(s, fields.tail) 
            }
          }
          
          makeFieldListIter("", fields)
        }
        
        "CREATE TABLE %s (%s)".format(tableName, makeFieldListString(fields))
    }
    
    exec(connection, buildCreateTableStatement(fields))
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
   * @param connection The connection to the SQL database
   * @param tableName The table to insert the row into
   * @param fieldValues A list of values to be inserted into the new row.
   */
  def Insert(connection: Connection, tableName: String, fieldValues: Array[Any]) = {
  
    def buildInsertStatement() : String = {
    
      def buildParamPlaceholders(fieldValues: Array[Any]) : String = {
        val paramPlaceholders = "?, " * fieldValues.length
        "(%s)".format(paramPlaceholders.stripSuffix(", "))
      }
      
      "INSERT INTO %s values %s".format(tableName, buildParamPlaceholders(fieldValues))        
    }
    
    def doPrepare(statement: String, fieldValues: Array[Any]) : PreparedStatement = {
      
      @annotation.tailrec
      def doPrepareIter(counter: Int, statement: PreparedStatement, fieldValues: Array[Any]) : PreparedStatement = {
        if (fieldValues.length == 0) statement
        else {
          val value = fieldValues.head
          
          value match {
            case _: Int => statement.setInt(counter, value.toString.toInt)
            case _ => statement.setString(counter, value.toString)
          }
          
          doPrepareIter(counter + 1, statement, fieldValues.tail)
        }
      }
      
      val prepStmt = connection.prepareStatement(statement)
      doPrepareIter(1, prepStmt, fieldValues)
    }
    
    val prepStmt = doPrepare(buildInsertStatement(), fieldValues)
    prepStmt.executeUpdate()
  }  
    
  /**
   * Select rows from a database table.
   * 
   * For more information see SelectStatement.scala
   * 
   * @param connection
   * @param tableName
   * @param fields
   * @param whereClauses
   * @return
   */
  def select(connection: Connection, 
      tableName: String, 
      fields: Array[String], 
      whereClauses: Array[(String, String)] = Array()) : ResultSet = 
        
    SelectStatement.select(connection, tableName, fields, whereClauses)
  
  /**
   * Get the highest value of the id field for a table
   * 
   * @param connection The connection to the SQL database
   * @param tableName The table to get the highest id for
   * @return The highest value of the id field. If there are no rows then 0 is returned. 
   */
  def getMaxId(connection: Connection, tableName: String) : Integer = {
    
    val resultSet = select(connection, tableName, Array[String]("MAX(id) as id"))    
    resultSet.getInt("id")    
  }
  
  def getNewId(connection: Connection, tableName: String) = getMaxId(connection, tableName) + 1
    
  /**
   * Find whether the given table exists in the database
   * 
   * @param connection The connection to the SQL database
   * @param tableName The table to check for existence
   * @return True if the table exists; false otherwise
   */
  def tableExists(connection: Connection, tableName: String) : Boolean = {
    val masterTable = "sqlite_master"
    val whereClause = Array[(String, String)](("name",tableName), ("type", "table"))
    val selectFields = Array[String]("COUNT(*)")
    val resultSet = select(connection, masterTable, selectFields, whereClause)
    val count = resultSet.getInt(1)
        
    count > 0
  }
    
  /**
   * Delete a row from a database table based on its id field
   * 
   * @param connection The connection to the SQL database
   * @param tableName The name of the table to delete the row from
   * @param id The id of the row to be deleted
   */
  def deleteById(connection: Connection, tableName: String, id: Integer) = {
    val statement = "DELETE FROM %s WHERE id=%d" format(tableName, id)
    val s = connection.createStatement()
    s.executeUpdate(statement)
  }
  
  /**
   * Create a database table if it does not already exist
   * 
   * @param connection The connection to the SQL database
   * @param tableName The name of the table to create
   * @param fields The fields to be added included in the table. An array of tuples. 
   *               Each tuple contains the name and data type of a field.
   * @return True if the table was created, false otherwise 
   */
  def createTableIfDoesNotExist(connection: Connection, tableName: String, fields: Array[(String, String)]) = {    
    if (!tableExists(connection, tableName)) {
      createTable(connection, tableName, fields)
      true
    } else {
      false
    }
  }
  
  /**
   * Does the given column exist in the given table?
   * 
   * @param connection The connection to the SQL database
   * @param tableName The name of the table to check
   * @param columnName The name of the column to check
   * @return true if a column with this name exists in the table; false otherwise
   */
  def columExists(connection: Connection, tableName: String, columnName: String) : Boolean = {
    
    val tableInfo = getTableInfo(connection, tableName)
    val columns = getTableColumnNames(connection, tableInfo)
    columns.filter(_ == columnName).length > 0
  }  
  
  /**
   * Get the names of all columns from a table given the result of PRAGMA table_info
   * 
   * @param connection The connection to the SQL database
   * @param tableInfo The result of a PRAGMA table_info query
   * @return An array containing the names of 
   */
  def getTableColumnNames(connection: Connection, tableInfo: ResultSet) : Array[String] = {
    
    @annotation.tailrec
    def getTableColumnNamesIter(columns: Array[String], tableInfo: ResultSet) : Array[String] = {
      if (!tableInfo.next()) columns
      else getTableColumnNamesIter(columns :+ tableInfo.getString("name"), tableInfo)
    }
    
    getTableColumnNamesIter(Array(), tableInfo)
  }
  
  /**
   * Get information about a table
   * 
   * @param connection The connection to the SQL database
   * @param tableName The name of the table to get information about
   * @return A ResultSet containing the table information
   */
  def getTableInfo(connection: Connection, tableName: String) : ResultSet = {
    val sql = "PRAGMA table_info(%s)" format(tableName)
    val stmt = connection.createStatement()
    stmt.executeQuery(sql)    
  }
  
  /**
   * Add a column with the given name and type to a database table
   * 
   * @param connection The connection to the SQL database
   * @param tableName The name of the table to add the column to
   * @param columnName The name of the column to be added
   * @param columnType The data type of the column to be added
  */
  def addColumn(connection: Connection, tableName: String, columnName: String, columnType: String) = {
    val sql = "ALTER TABLE %s ADD %s %s" format(tableName, columnName, columnType)
    exec(connection, sql)
  }
}