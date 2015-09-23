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
	* Delete a folder. Delete all bookmarks in the folder.
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

   val bookmarks = getAllBookmarks.filter(_(3) == id.toString).map(_(1))

   bookmarks.foreach(deleteBookmark(_))

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
   * Delete a bookmark
   *
   * @param url The url of the bookmark to be deleted
   * @throws ValidationException if url is empty or whitespace
   * @throws BookmarkNotFoundException if no bookmark with the given url exists
   */
  def deleteBookmark(url: String) = {
    val validations = List((url.trim.isEmpty, new ValidationException),
                      (!bookmarkExists(url), new BookmarkNotFoundException))
    validate(validations)

    val id = getBookmarkId(url)

    SqliteStatements.deleteById("bookmark", id)
  }

  /**
   * Update a bookmark
   *
   * @param oldUrl The current url of the bookmark
   * @param newUrl The new url of the bookmark
   * @param newDescription The new description of the bookmark
   * @throws ValidationException if oldUrl or newUrl are empty or whitespace
   * @throws BookmarkNotFoundException if no bookmark exists with url of oldUrl
   * @throws BookmarkExistsException if oldUrl does not equal newUrl and a bookmark already exists with url of newUrl
   */
  def updateBookmark(oldUrl: String, newUrl: String, newDescription: String) = {

    def updatingUrl() : Boolean = {
      oldUrl != newUrl
    }

    val validations = List( (!List(oldUrl, newUrl).filter(_.trim.isEmpty).isEmpty, new ValidationException),
                      (!bookmarkExists(oldUrl), new BookmarkNotFoundException),
                      (updatingUrl && bookmarkExists(newUrl), new BookmarkExistsException))

    validate(validations)

    val id = getBookmarkId(oldUrl)

    val updateColumns = List(("url", newUrl),
                             ("description", newDescription))

    val whereClause = List(("id", id.toString))

    SqliteStatements.updateTable("bookmark", updateColumns, whereClause)
  }

  /**
   * Move a bookmark to a different folder
   *
   * @param url The url of the bookmark to move
   * @param newFolder The name of the folder to move to
   * @throws ValidationException if url or newFolder are empty or whitespace
   * @throws BookmarkNotFoundException if no bookmark exists with the given url
   * @throws FolderNotFoundException if no folder exists with a name of newFolder
   */
  def moveBookmark(url: String, newFolder: String) = {
    val validations = List( (!List(url, newFolder).filter(_.trim.isEmpty).isEmpty, new ValidationException),
                      (!bookmarkExists(url), new BookmarkNotFoundException),
                      (!folderExists(newFolder), new FolderNotFoundException))
    validate(validations)

    val folderId = getAllFolders().filter(_(1) == newFolder).head.head

    val updateColumns = List(("folder", folderId))
    val whereClause = List(("url", url))

    SqliteStatements.updateTable("bookmark", updateColumns, whereClause)
  }

  /**
   * Get a bookmark's id
   *
   * @param url The id of the bookmark
   * @returns The id of the bookmark
   * @throws ValidationException if the url is empty or whitespace
   * @throws BookmarkNotFoundException if no bookmark exists with the given url
   */
  private def getBookmarkId(url: String) : Integer = {
    val validations = List((url.trim.isEmpty, new ValidationException),
                      (!bookmarkExists(url), new BookmarkNotFoundException))
    validate(validations)
    getAllBookmarks.filter(x => x(1) == url).head.head.toInt
  }


  /**
	* Check if a bookmark with the given url exists
	*
	* @param url The url to check
	* @return true if the bookmark exists, otherwise false
	*/
  private def bookmarkExists(url: String) : Boolean = {
	 SqliteStatements.countWhere("bookmark", List(("url", url))) > 0
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
	* Get all bookmarks
	*
	* @return A two-dimensional list of String. Each row contains the id, url, description and folder of a bookmark
	*/
  def getAllBookmarks() : List[List[String]] = {
	 SqliteStatements.select("bookmark", List("id", "url", "description", "folder"))
  }
}
