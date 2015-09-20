import java.sql.Connection
import java.sql.PreparedStatement

object PrepareStatement {
  
  def getPrepareStatement(connection: Connection) = {
	 def doPrepare(statement: String, fieldValues: List[String]) : PreparedStatement = {
		
      @annotation.tailrec
      def doPrepareIter(counter: Int, statement: PreparedStatement, fieldValues: List[String]) : PreparedStatement = {
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

	 doPrepare _
  }
}
