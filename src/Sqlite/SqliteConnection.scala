import java.sql.Connection
import java.sql.DriverManager
import org.sqlite.JDBC

object SqliteConnection {
  def getConnection(dbName: String) = {
    DriverManager.getConnection("jdbc:sqlite:%s.db".format(dbName))    
  }
}