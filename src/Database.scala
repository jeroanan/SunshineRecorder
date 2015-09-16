import java.sql.Connection

object Database {
  
  def initDatabase(connection: Connection) {
    
    val tableDefinitions = Array(
        ("folder", Array(
                      ("id", "INTEGER"),
                      ("name", "TEXT")
                   )
        )
    )
    
    for (t <- tableDefinitions) SqliteStatements.createTableIfDoesNotExist(connection, t._1, t._2)
    
    connection.close()
    
  }
}