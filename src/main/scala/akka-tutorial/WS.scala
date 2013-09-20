package akkaTutorial

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

import org.scalatest.FlatSpec

object Constants {
  implicit val timeout = Timeout(1 second)
  val KeyNotFound = new Exception("Key not found")
}
import Constants._;
 

sealed trait WSMessage
case class Lookup(address:Int) extends WSMessage
case class Read(address:Int) extends WSMessage
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
        sender ! data(soughtAddress)
      else {
        val distances = (address :: neighbors.keys.toList) map
          {(a:Int) => math.abs(a - soughtAddress)} zip (self :: (neighbors.keys.toList map neighbors))
        val nearest = (distances reduce
          {(e1:Tuple2[Int,ActorRef], e2:Tuple2[Int,ActorRef]) => if (e1._1 < e2._1) e1 else e2}
          )._2
        if(nearest == self)
          sender ! Status.Failure(KeyNotFound)
        sender ! Redirect(nearest)
      }
  }
}

object WSSpec extends FlatSpec {
  def SingleNode = new {
    val system = ActorSystem("WSSystem")
    var data = new collection.mutable.HashMap[Int,String]
    data(1) = "foobar"
    val node = system.actorOf(Props(new Node(1, new mutable.HashMap[Int, ActorRef], data)), name = "node")
  }

  "A single node" should "read items" in {
    val sn = SingleNode
    assert(Await.result(sn.node ? Read(1), 1 second).asInstanceOf[String] == "foobar")
    sn.system.shutdown()
  }

  it should "throw an exception when asked to read nonexistent items" in {
    val sn = SingleNode
    val thrown = intercept[Exception] {
      sn.node ? Read(2)
    }
    assert(thrown === KeyNotFound)
  }

//  def readMultipleNodes = {
//    val system = ActorSystem("WSSystem")
//    val addresses = 0 until 10
//    val nodes = for(i <- addresses) yield system.actorOf(Props(new
//      Node(i, new mutable.HashMap[Int, ActorRef], new collection.mutable.HashMap[Int,String])), name = "node" + i.toString)
//    val pairs = (nodes.takeRight(1) ++ nodes zip addresses) ++ (nodes zip (addresses.takeRight(1) ++ addresses))
//    pairs foreach {
//      t => t._1 !  LoveMe(t._2)
//    }
//    nodes foreach {
//      node:ActorRef => node ? GetDegree onSuccess {
//        case degree => assert(degree == 2)
//      }
//    }
//    //nodes take(5) last ! Write(5, "foobar")
//    //nodes head ? Read(5) onSuccess
//    system.shutdown()
//  }
}

object WattsStrogatz extends App {
  WSSpec.execute()
}


