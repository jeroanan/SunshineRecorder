import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement

import Sqlite.SelectStatement

object SqliteStatements {
  
  def exec(connection: Connection, sqlString: String) = {
    val stmt = connection.createStatement()    
    stmt.executeUpdate(sqlString)    
  }
  
  def dropTableIfExists(connection: Connection, tableName: String) = {
    def buildDropTableIfExistsStatement() = {
      "DROP TABLE IF EXISTS %s".format(tableName)
    }
    
    exec(connection, buildDropTableIfExistsStatement())
  }
  
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
  
  def select(connection: Connection, 
      tableName: String, 
      fields: Array[String], 
      whereClauses: Array[(String, String)] = Array()) : ResultSet = {
    
    SelectStatement.select(connection, tableName, fields, whereClauses)
  }
  
  def getMaxId(connection: Connection, tableName: String) : Integer = {
    
    val resultSet = select(connection, tableName, Array[String]("MAX(id) as id"))    
    resultSet.getInt("id")    
  }
  
  def getNewId(connection: Connection, tableName: String) = getMaxId(connection, tableName) + 1
  
  def tableExists(connection: Connection, tableName: String) : Boolean = {
    true
  }
  
  def deleteById(connection: Connection, tableName: String, id: Integer) = {
    val statement = "DELETE FROM %s WHERE id=%d" format(tableName, id)
    val s = connection.createStatement()
    s.executeUpdate(statement)
  }
}