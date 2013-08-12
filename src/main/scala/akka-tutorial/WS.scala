package akkaTutorial

import akka.actor._
import akka.pattern._
import akka.util.Timeout

object MyImplicits {
  implicit val timeout = Timeout(1)
}
import MyImplicits._;
 

sealed trait WSMessage
case class Lookup(address:Int) extends WSMessage
case class Read(address:Int) extends WSMessage
case class Found(content:String) extends WSMessage
case class Redirect(next:ActorRef) extends WSMessage
case class LoveMe(senderAsNode:Node) extends WSMessage
case class Write(key:Int, value:String) extends WSMessage

class Node(val address:Int, var neighbors:List[Node], var
    data:collection.mutable.Map[Int,String]) extends Actor {
  def receive = {
    case LoveMe(senderAsNode) =>
      neighbors = senderAsNode :: neighbors
    case Write(key, value) =>
      data(key) = value
    case Read(soughtAddress) =>
      if (data contains soughtAddress)
        sender ! Found(data(soughtAddress))
      else {
        val distances = (address :: (neighbors map {_ address})) map
          {(a:Int) => math.abs(a - soughtAddress)} zip (self ::
            (neighbors map {_ self}))
        val nearest = (distances reduce
          {(e1:Tuple2[Int,ActorRef], e2:Tuple2[Int,ActorRef]) =>
          if (e1._1 < e2._1) e1 else e2})._2
        sender ! Redirect(nearest)
      }
  }
}

object WattsStrogats extends App {
  val system = ActorSystem("WSSystem")
  var data = new collection.mutable.HashMap[Int,String]
  data(1) = "Hello world"
  val node = system.actorOf(Props(new Node(1, Nil, data)), name = "node")
  node ? Read(1) onSuccess {case message =>
      println(message)
      system.shutdown()
    }
}
