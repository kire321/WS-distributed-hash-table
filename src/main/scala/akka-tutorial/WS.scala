//if neighbors maps addresses to actorRefs, then everything else will
//fall into place

package akkaTutorial

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import scala.collection.mutable

object MyImplicits {
  implicit val timeout = Timeout(1)
}
import MyImplicits._;
 

sealed trait WSMessage
case class Lookup(address:Int) extends WSMessage
case class Read(address:Int) extends WSMessage
case class Found(content:String) extends WSMessage
case class Redirect(next:ActorRef) extends WSMessage
case class LoveMe(returnAddress:Int) extends WSMessage
case class Write(key:Int, value:String) extends WSMessage
case class GetDegree extends WSMessage

class Node(
  val address:Int,
  var neighbors:collection.mutable.Map[Int,ActorRef],
  var data:collection.mutable.Map[Int,String]
      ) extends Actor {
  def receive = {
    case GetDegree =>
      sender ! neighbors.size
    case LoveMe(returnAddress) =>
      neighbors(returnAddress) = sender
    case Write(key, value) =>
      data(key) = value
    case Read(soughtAddress) =>
      if (data contains soughtAddress)
        sender ! Found(data(soughtAddress))
      else {
        val distances = (address :: neighbors.keys.toList) map
          {(a:Int) => math.abs(a - soughtAddress)} zip (self ::
            (neighbors.keys.toList map neighbors))
        val nearest = (distances reduce
          {(e1:Tuple2[Int,ActorRef], e2:Tuple2[Int,ActorRef]) =>
          if (e1._1 < e2._1) e1 else e2})._2
        sender ! Redirect(nearest)
      }
  }
}

object Tests {
  def readSingleNode = {
    val system = ActorSystem("WSSystem")
    var data = new collection.mutable.HashMap[Int,String]
    data(1) = "Hello world"
    val node = system.actorOf(Props(new Node(1, new mutable.HashMap[Int, ActorRef], data)), name = "node")
    node ? Read(1) onSuccess {case message =>
        println(message)
        system.shutdown()
      }
  }

  def readMultipleNodes = {
    val system = ActorSystem("WSSystem")
    val addresses = 0 until 10
    val nodes = for(i <- addresses) yield system.actorOf(Props(new
      Node(i, new mutable.HashMap[Int, ActorRef], new collection.mutable.HashMap[Int,String])), name = "node" + i.toString)
    val pairs = (nodes.takeRight(1) ++ nodes zip addresses) ++ (nodes zip (addresses.takeRight(1) ++ addresses))
    pairs foreach {
      t => t._1 !  LoveMe(t._2)
    }
    nodes foreach {
      node:ActorRef => node ? GetDegree onSuccess {
        case degree => assert(degree == 2)
      }
    }
    system.shutdown()
  }
}

object WattsStrogatz extends App {
  println("different text")
  //assert(0 == 1)
  Tests.readSingleNode
  Tests.readMultipleNodes
}


