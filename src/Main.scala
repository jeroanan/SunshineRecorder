import swing._
import scala.swing.event._

import java.util.UUID

object Main extends SimpleSwingApplication {

  /*
	* So this is a very noddy frame to show how it's done.
	*
	* Problem is that now the main class for this whole application is now derived from SimpleSwingApplication!!
	* So much for keeping the UI decoupled from the rest of the program.
	*
	* What do we do? I reckon we do the following:
	*
	* 1. Create a new package called SunshineRecorder
	* 2. It contains a SunshineRecorder object, which abstracts the database functions and exposes functions like
	*    "add folder", "delete bookmark" and such. It essentially forms the entry point for whichever UI we slap on
	*     top of it. It is the application. Whatever's in this file just delegates to it to provide the functionality.
	*
	* Still, it's a little unsettling that in essence we have to surrender Main to a UI toolkit. It almost seems like
	* something written in Java should deal with this crap so we can keep our nice code in Scala that is the actual
	* application.
	*
	*/
  def top = new MainFrame {
	 title = "Boo"
	 contents = new GridPanel(2, 2) {
      hGap = 3
      vGap = 3
      contents += new Button {
        text = "Press Me!"
        reactions += {
          case ButtonClicked(_) => Sunshine.addFolder(UUID.randomUUID().toString())
        }
      }
    }
  }

  Sunshine.init()
}
