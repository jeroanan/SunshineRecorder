

object Main extends App {
    
  val conn = SqliteConnection.getConnection("test")
 
  SqliteStatements.dropTableIfExists(conn, "test")
    
  val fields = Array(("id", "INTEGER"), ("field1", "INTEGER"), ("field2", "INTEGER"))
  SqliteStatements.createTable(conn, "test", fields)  
 
  val fieldValues = Array[Any](1, 4, 8)
  SqliteStatements.Insert(conn, "test", fieldValues)
  
  val res = SqliteStatements.select(conn, "test", Array("id as poo", "field2"))
  
  while (res.next()) {
    println(res.getString("poo"))
  }
}