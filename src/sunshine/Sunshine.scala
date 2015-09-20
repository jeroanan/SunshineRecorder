object Sunshine {

  def init() = {
	 Database.initDatabase()
  } 

  def addFolder(folderName: String) {
	 if (folderName.trim.isEmpty) throw new ValidationException
	 if (folderExists(folderName)) throw new FolderExistsException
	 SqliteStatements.Insert("folder", List(SqliteStatements.getNewId("folder").toString, folderName))
  }

  def deleteFolder(folderName: String) {
	 if (folderName.trim.isEmpty) throw new ValidationException
	 if (!folderExists(folderName)) throw new FolderNotFoundException
	 val id = getAllFolders.filter(_(1) == folderName).head.head.toInt
	 SqliteStatements.deleteById("folder", id)
  }

  private def folderExists(folderName: String) : Boolean = {
	 SqliteStatements.select("folder", List("COUNT(*)"), List(("name", folderName))).head.head.toInt > 0
  }

  private def getAllFolders() = {
	 SqliteStatements.select("folder", List("id", "name"))  
  }
}
