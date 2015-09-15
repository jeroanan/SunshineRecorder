

object Main extends App {
    
  val conn = SqliteConnection.getConnection("test")
 
  SqliteStatements.dropTableIfExists(conn, "test")
    
  val fields = Array(("id", "INTEGER"), ("field1", "INTEGER"), ("field2", "INTEGER"))
  SqliteStatements.createTable(conn, "test", fields)  
 
  val fieldValues = Array[Any](SqliteStatements.getNewId(conn, "test"), 4, 8)
  SqliteStatements.Insert(conn, "test", fieldValues)
  
  val fieldValues2 = Array[Any](SqliteStatements.getNewId(conn, "test"), 49, 8)
  SqliteStatements.Insert(conn, "test", fieldValues2)
  
  //SqliteStatements.deleteById(conn, "test", SqliteStatements.getMaxId(conn, "test"))
  
}  