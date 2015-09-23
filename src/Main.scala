import swing._
import scala.swing.event._

import java.util.UUID

object Main extends SimpleSwingApplication {

  def top = new MainFrame {
	 title = "Bookmark Manager"
	 contents = new GridPanel(2, 2) {
      hGap = 3
      vGap = 3
      contents += new Button {
        text = "Press Me!"
        reactions += {
          case ButtonClicked(_) => sayHi
        }
      }
    }
  }

  def sayHi = {
    println("Hello there")
  }

  Sunshine.init()
}
