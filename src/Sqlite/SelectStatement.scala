import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

object SelectStatement {
  
  def getSelectStatement(getConnection: () => Connection,
      toArray: (ResultSet, List[String]) => List[List[String]]) = {
  
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
  
  private def buildSelectStatement(tableName: String, fields: List[String]): String = {    
		var fieldList = fields.reduce(_ + ", " + _)
      "SELECT %s FROM %s".format(fieldList, tableName)
  }
  
  private def buildWhereClause(whereClauses: List[String]) : String = {
		if (whereClauses.isEmpty) ""
		else "WHERE %s" format whereClauses.map("%s=?".format(_)).reduce(_ + " AND " + _)
    }
}
