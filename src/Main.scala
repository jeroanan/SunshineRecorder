

object Main extends App {
    
    
  Database.initDatabase()
   
  
//  SqliteStatements.dropTableIfExists(getConnection(), "test")
//  

//  val fields = Array(("id", "INTEGER"), ("field1", "INTEGER"), ("field2", "INTEGER"))
//  SqliteStatements.createTableIfDoesNotExist(conn, "test", fields)  
// 
//  val fieldValues = Array[Any](SqliteStatements.getNewId(conn, "test"), 4, 8)
//  SqliteStatements.Insert(conn, "test", fieldValues)
//  
//  val fieldValues2 = Array[Any](SqliteStatements.getNewId(conn, "test"), 49, 8)
//  SqliteStatements.Insert(conn, "test", fieldValues2)
//  
//  SqliteStatements.addColumn(conn, "test", "field21", "TEXT")  
}  