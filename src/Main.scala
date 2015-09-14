

object Main extends App {
    
  val conn = SqliteConnection.getConnection("test")
 
  SqliteStatements.dropTableIfExists(conn, "test")
    
  val fields = Array(("id", "INTEGER"), ("field1", "INTEGER"), ("field2", "INTEGER"))
  SqliteStatements.createTable(conn, "test", fields)  
 
  val fieldValues = Array[Any](1, 4, 5)
  SqliteStatements.Insert(conn, "test", fieldValues)
}