package model

/**
 * Created by hayssams on 20/10/14.
 */
object Model {

  case class Rating(val index : Int, val userid: Int, itemid: Int, rating: Int, timestamp: Long) {
    def toMap(): Map[String, Any] = Map("index" -> index, "userid" -> userid, "itemid" -> itemid, "rating" -> rating, "timestamp" -> timestamp)
  }

  case class Item(val itemid: Int, name: String, timestamp: Long, unknown: Boolean, action: Boolean, adventure: Boolean, animation: Boolean,
                  children: Boolean, comedy: Boolean, crime: Boolean, documentary: Boolean, drama: Boolean, fantasy: Boolean, filmnoir: Boolean, horror: Boolean,
                  musical: Boolean, mystery: Boolean, romance: Boolean, scifi: Boolean, thriller: Boolean, war: Boolean, western: Boolean) {
    def toMap(): Map[String, Any] = Map("itemid" -> itemid, "name" -> name, "timestamp" -> timestamp, "unknown" -> unknown,
      "action" -> action, "adventure" -> adventure, "animation" -> animation, "children" -> children, "comedy" -> comedy, "crime" -> crime, "documentary" -> documentary,
      "drama" -> drama, "fantasy" -> fantasy, "filmnoir" -> filmnoir, "horror" -> horror, "musical" -> musical, "mystery" -> mystery, "romance" -> romance, "scifi" -> scifi,
      "thriller" -> thriller, "war" -> war, "western" -> western)
  }


  case class User(val userid:Int, itemids:Array[Int]) {
    def toMap(): Map[String, Any] = Map("userid" -> userid, "itemids" -> itemids.map(_.toString).mkString(","))
  }

}
