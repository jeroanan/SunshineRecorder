import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import java.util.UUID

import Sqlite.SelectStatement

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
   * @param tableName The database to be dropped
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
  def createTable(tableName: String, fields: Array[(String, String)]) = {
    
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
    
    exec(buildCreateTableStatement(fields))
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
  def Insert(tableName: String, fieldValues: Array[String]) = {
  
    val connection = getConnection()
    
    def buildInsertStatement() : String = {
    
      def buildParamPlaceholders(fieldValues: Array[String]) : String = {
        val paramPlaceholders = "?, " * fieldValues.length
        "(%s)".format(paramPlaceholders.stripSuffix(", "))
      }
      
      "INSERT INTO %s values %s".format(tableName, buildParamPlaceholders(fieldValues))        
    }
    
    def doPrepare(statement: String, fieldValues: Array[String]) : PreparedStatement = {
      
      @annotation.tailrec
      def doPrepareIter(counter: Int, statement: PreparedStatement, fieldValues: Array[String]) : PreparedStatement = {
        if (fieldValues.length == 0) statement
        else {
          val value = fieldValues.head          
          statement.setString(counter, value.toString)
          doPrepareIter(counter + 1, statement, fieldValues.tail)
        }
      }
      
      val prepStmt = connection.prepareStatement(statement)
      doPrepareIter(1, prepStmt, fieldValues)
    }
    
    val prepStmt = doPrepare(buildInsertStatement(), fieldValues)
    prepStmt.executeUpdate()
    
    connection.close()
  }  
    
  /**
   * Select rows from a database table.
   * 
   * For more information see SelectStatement.scala
   * 
   * @param tableName
   * @param fields
   * @param whereClauses
   * @return
   */
  def select(tableName: String, 
      fields: Array[String], 
      whereClauses: Array[(String, String)] = Array()) : Array[Array[String]] = {
       
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
    
    val result = select(tableName, Array[String]("MAX(id) as id"))
    result.head(0).toInt
  }
  
  def getNewId(tableName: String) = getMaxId(tableName) + 1
    
  /**
   * Find whether the given table exists in the database
   * 
   * @param tableName The table to check for existence
   * @return True if the table exists; false otherwise
   */
  def tableExists(tableName: String) : Boolean = {
    val masterTable = "sqlite_master"
    val whereClause = Array[(String, String)](("name",tableName), ("type", "table"))
    val selectFields = Array[String]("COUNT(*)")
    val resultSet = select(masterTable, selectFields, whereClause)
    val count = resultSet.head(0).toInt
    count > 0
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
   * @return True if the table was created, false otherwise 
   */
  def createTableIfDoesNotExist(tableName: String, fields: Array[(String, String)]) = {    
    if (!tableExists(tableName)) {
      createTable(tableName, fields)
      true
    } else {
      false
    }
  }
  
  /**
   * Does the given column exist in the given table?
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
   * @param tableInfo A two-dimensional array containing the result of PRAGMA table_info
   * @return An array containing the names of the column names
   */
  def getTableColumnNames(tableInfo: Array[Array[String]]) : Array[String] = {    
    getTableColumnNamesAndTypes(tableInfo).map(x => x._1)
  }
  
  def getTableColumnNamesAndTypes(tableInfo: Array[Array[String]]): Array[(String, String)] = {
    
    @annotation.tailrec
    def nameTypeIter(columns: Array[(String, String)], tableInfo: Array[Array[String]]) : Array[(String, String)] = {
      
      if (tableInfo isEmpty) columns
      else nameTypeIter(columns :+ (tableInfo.head(1), tableInfo.head(2)), tableInfo.tail)
    }
    nameTypeIter(Array(), tableInfo)
  }
  
  /**
   * Get information about a table
   * 
   * @param connection The connection to the SQL database
   * @param tableName The name of the table to get information about
   * @return A two-dimensional array containing the table records
   */
  def getTableInfo(tableName: String) : Array[Array[String]] = {
    val sql = "PRAGMA table_info(%s)" format(tableName)
    val connection = getConnection()
    val stmt = connection.createStatement()
    val result = stmt.executeQuery(sql)
    
    val fields = Array("cid", "name", "type", "notnull", "dflt_value", "pk")
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
  
  def getExtraColumns(tableName: String, columns: Array[String]) : Array[String] = {
    val tableColumns = getTableColumnNames(getTableInfo(tableName))    
    tableColumns.filter(!columns.contains(_))
  }
  
  def resultSetToArray(resultSet: ResultSet, columnNames: Array[String]) : Array[Array[String]] = {
       
     @annotation.tailrec
     def iter(table : Array[Array[String]]) : Array[Array[String]] = {
       if (!resultSet.next()) table
       else {
          val row = columnNames.map(resultSet.getString(_))            
          iter(table :+ row)
       }
     }
     
     iter(Array(Array())).filter(_.length > 0)
   }
  
  def dropColumns(tableName: String, columns: Array[String]) = {
    
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
  
}