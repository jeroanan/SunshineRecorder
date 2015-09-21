/**
 * Contains the logic that comprises the application
 */
object Sunshine {

  /**
	* Initialise the application
	*/
  def init() = {
	 Database.initDatabase() 
  }

  /**
	* Add a folder
	*
	* @param folderName The name of the new folder
	* @throws ValidationException if folderName is empty or whitespace
	* @throws FolderExistsException if a folder with the same name exists
	*/
  def addFolder(folderName: String) {
	 val validations = List((folderName.trim.isEmpty, new ValidationException),
									(folderExists(folderName), new FolderExistsException))
	 validate(validations)
	 SqliteStatements.Insert("folder", List(SqliteStatements.getNewId("folder").toString, folderName))
  }

  /**
	* Delete a folder
	*
	* @param folderName the name of the folder to be deleted
	* @throws ValidationException if folderName is empty or whitespace
	* @throws FolderNotFoundException if the folder to be deleted doesn't exist
	*/
  def deleteFolder(folderName: String) {
	 val validations = List((folderName.trim.isEmpty, new ValidationException),
									(!folderExists(folderName), new FolderNotFoundException))
	 validate(validations)

	 val id = getAllFolders.filter(_(1) == folderName).head.head.toInt
	 SqliteStatements.deleteById("folder", id)
  }

  /**
	* Rename a folder
	*
	* @param oldName the current name of the folder
	* @param newName the name to rename the folder to
	* @throws ValidationException if oldName or newName are empty or whitespace
	* @throws FolderNotFoundException if a folder with name oldName does not exist
	* @throws FolderExistsException if a folder with name newName already exists
	*/
  def renameFolder(oldName: String, newName: String) {
	 val validations = List((List(oldName, newName).filter(_.trim.isEmpty).length > 0, new ValidationException),
									(!folderExists(oldName), new FolderNotFoundException),
									(folderExists(newName), new FolderExistsException))

	 validate(validations)

	 def nameTuple(x: String) = List(("name", x))

	 SqliteStatements.updateTable("folder", nameTuple(newName), nameTuple(oldName))
  }

  /**
	* Check whether a folder with the given name exists
	*
	* @param folderName the name of the folder to check
	* @return true if a folder with the given name exists, otherwise false
	*/
  private def folderExists(folderName: String) : Boolean = {
	 SqliteStatements.countWhere("folder", List(("name", folderName))) > 0
  }

  /**
	* Check whether a folder with the given id exists
	*
	* @param folderId the id of the folder to check
	* @return true if a folder with the given folderId exists, otherwise false
	*/
  private def folderExists(folderId: Integer) : Boolean = {
	 SqliteStatements.countWhere("folder", List(("id", folderId.toString))) > 0
  }

  /**
	* Get all folders from the database
	*
	* @return A two-dimensional list. Each row is a tuple of String containing the id and name of the folder
	*/
  def getAllFolders() : List[List[String]] = {
	 SqliteStatements.select("folder", List("id", "name")) 
  }

  /**
	* Add a bookmark
	*
	* @param url The url of the bookmark
	* @param description A description of the bookmark
	* @param folderId The id of the folder to add the bookmark to
	* @throws ValidationException if url is empty or whitespace
	* @throws BookmarkExists if a bookmark with the given url already exists
	* @throws FolderNotFoundException if a folder with the given folderId does not exist
	*/
  def addBookmark(url : String, description : String, folderId: Integer) = {
	 val validations = List((url.trim.isEmpty, new ValidationException),
								   (bookmarkExists(url), new BookmarkExistsException),
								   (!folderExists(folderId), new FolderNotFoundException))

	 validate(validations)

	 SqliteStatements.Insert("bookmark", List(SqliteStatements.getNewId("bookmark").toString,
															url,
															description,
															folderId.toString))															
  }
  
  /**
	* Throw an exception if a predicate is true
	* 
	* @param validations A list containing a tuple of Boolean and an object deriving from Exception.
	*                    If any of the Boolean values are true then the Exception of the first such
	*                    entry in this list is thrown
	* @throws Excpetion if one or more of the predicates in validation are true
	*/
  private def validate(validations: List[(Boolean, Exception)]) = {
		val exceptions = validations.filter(x => x._1)
		if (!exceptions.isEmpty) throw exceptions.head._2
  }

  /**
	* Check if a bookmark with the given url exists
	*
	* @param url The url to check
	* @return true if the bookmark exists, otherwise false
	*/
  def bookmarkExists(url: String) : Boolean = {
	 SqliteStatements.countWhere("bookmark", List(("url", url))) > 0
  }

  /**
	* Get all bookmarks
	*
	* @return A two-dimensional list of String. Each row contains the id, url, description and folder of a bookmark
	*/
  def getAllBookmarks() : List[List[String]] = {
	 SqliteStatements.select("bookmark", List("id", "url", "description", "folder"))
  }
}
