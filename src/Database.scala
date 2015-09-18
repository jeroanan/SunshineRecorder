import java.sql.Connection

object Database {
  
  def initDatabase() {
    
    val tableDefinitions = Array(
        ("folder", Array(
                      ("id", "INTEGER"),
                      ("name", "TEXT")
                   )
        )
    )
    
    for (t <- tableDefinitions)
      if (!SqliteStatements.createTableIfDoesNotExist(t._1, t._2))
      {
        t._2.foreach(f => SqliteStatements.addColumnIfDoesntExist(t._1, f._1, f._2))
        val fieldNames = t._2.map(f => f._1)
        
        val columnsToDrop = SqliteStatements.getExtraColumns(t._1, fieldNames)
        SqliteStatements.dropColumns(t._1, columnsToDrop)
        
      }    
  }
}