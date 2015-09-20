import java.sql.Connection

object Database {

  def initDatabase() {
    val tableDefinitions = List(
        ("folder", List(
                      ("id", "INTEGER PRIMARY KEY"),
                      ("name", "TEXT")
                   )
        ),
        ("bookmark", List(
							 ("id", "INTEGER PRIMARY KEY"),
							 ("url", "TEXT"),
							 ("description", "TEXT"),
							 ("folder", "INTEGER REFERENCES folder(id)")
						  )
		  )
    )

	 val existNotExist = tableDefinitions.partition(x => SqliteStatements.tableExists(x._1))
	 val existingTables = existNotExist._1
	 val newTables = existNotExist._2

	 newTables.foreach(x => SqliteStatements.createTableIfDoesNotExist(x._1, x._2))

	 existingTables.foreach(x => {
		val newColumns = x._2.filter(y => !SqliteStatements.columnExists(x._1, y._1))
		newColumns.foreach(y => SqliteStatements.addColumnIfDoesntExist(x._1, y._1, y._2))

		val extraColumns = SqliteStatements.getExtraColumns(x._1, x._2.map(y => y._1))
		SqliteStatements.dropColumns(x._1, extraColumns)
	 })
	 
	 val tableNames = tableDefinitions.map(x => x._1)
	 SqliteStatements.getExtraTables(tableNames).foreach(SqliteStatements.dropTableIfExists(_))
  }
}
