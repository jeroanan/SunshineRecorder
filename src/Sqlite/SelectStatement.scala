package Sqlite

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

object SelectStatement {
  
  def getSelectStatement(getConnection: () => Connection,
      toArray: (ResultSet, Array[String]) => Array[Array[String]]) = {
  
    def select(tableName: String, 
        fields: Array[String], 
        whereClauses: Array[(String, String)] = Array()) : Array[Array[String]] = {
      
      val connection = getConnection()
      val whereFields = buildWhereClause(whereClauses)
      val whereClause = if (whereFields == "") "" else " WHERE %s" format whereFields
      val stmt = ("%s%s" format(buildSelectStatement(tableName, fields), whereClause))
      val prepStmt = doPrepare(connection, stmt, whereClauses)
      
      val res = toArray(prepStmt.executeQuery(), fields)
      connection.close()
      res
    }
    
    select _
  }
  
  private def buildSelectStatement(tableName: String, fields: Array[String]): String = {    
      
      def buildFieldList(fields: Array[String]) : String = {
        
        @annotation.tailrec
        def buildFieldListIter(fieldList: String, fields: Array[String]) : String = {
          if (fields.length == 0) fieldList
          else {
            val comma = if (fieldList == "") "" else ", "
            buildFieldListIter("%s%s%s".format(fieldList, comma, fields.head), fields.tail)
          } 
        }
        
        buildFieldListIter("", fields)
      }
    
      "SELECT %s FROM %s".format(buildFieldList(fields), tableName)
  }
  
  private def buildWhereClause(whereClauses: Array[(String, String)]) : String = {
      
      @annotation.tailrec
      def buildWhereClauseIter(whereSoFar: String, whereClauses: Array[(String, String)]) : String = {
        if (whereClauses.length == 0) whereSoFar
        else {
          val thisClause = whereClauses.head._1
          val comma = if (whereSoFar == "") "" else " AND "
            
          val newSoFar = "%s%s%s=?" format(whereSoFar, comma, thisClause)
          buildWhereClauseIter(newSoFar, whereClauses.tail)
        }
      }
      
      buildWhereClauseIter("", whereClauses)
    }
  
  private def doPrepare(connection: Connection, statement: String, whereClauses: Array[(String, String)]) : PreparedStatement = {
    
    def doPrepareIter(counter: Int, statement: PreparedStatement, whereClauses: Array[(String, String)]) : PreparedStatement = {
      if (whereClauses.length == 0) statement
      else {
        statement.setString(counter, whereClauses.head._2)
        doPrepareIter(counter +1, statement, whereClauses.tail)
      }
      statement
    }
    
    val prepStmt = connection.prepareStatement(statement)
    doPrepareIter(1, prepStmt, whereClauses)
  }    
}