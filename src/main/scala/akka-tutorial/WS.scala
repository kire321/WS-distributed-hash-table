package akkaTutorial

import akka.actor._
import akka.pattern._
import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._
import ExecutionContext.Implicits.global

object Constants {
  implicit val timeout = Timeout(1 second)
  val KeyNotFound = new Exception("Key not found")
  def actor(system:ActorSystem, name:String, address:Int) = system.actorOf(Props(new Node(address, new collection.mutable.HashMap[Int, ActorRef],
    new collection.mutable.HashMap[Int,String])), name = name)
  def SingleNode = new {
    val system = ActorSystem("WSSystem")
    val node = actor(system, "node", 1)
  }
}
import Constants._;
 

sealed trait WSMessage
case class Read(address:Int) extends WSMessage
case class AddNeighbor(newNeighbor:ActorRef) extends WSMessage
case class Write(key:Int, value:String) extends WSMessage
case class GetDegree extends WSMessage
case class GetAddress extends WSMessage
case class Finished extends WSMessage

class Node(
  val address:Int,
  var neighbors:collection.mutable.Map[Int,ActorRef],
  var data:collection.mutable.Map[Int,String]
      ) extends Actor {

  def nearestNeighbor(soughtAddress:Int) = {
    val distances = (address :: neighbors.keys.toList) map
      {(a:Int) => math.abs(a - soughtAddress)} zip (self :: (neighbors.keys.toList map neighbors))
    (distances reduce {(e1:Tuple2[Int,ActorRef], e2:Tuple2[Int,ActorRef]) => if (e1._1 < e2._1) e1 else e2})._2
  }

  def receive = {
    case GetDegree =>
      sender ! neighbors.size
    case GetAddress =>
      sender ! address
    case AddNeighbor(newNeighbor) =>
      val senderCopy = sender
      newNeighbor ? GetAddress onSuccess {
        case newAddress:Int => neighbors(newAddress) = newNeighbor
        senderCopy ! Finished
      }
    case Write(key, value) =>
      val nearest = nearestNeighbor(key)
      if(nearest == self) {
        data(key) = value
        sender ! Finished
      }
      else
        nearest forward Write(key, value)
    case Read(soughtAddress) =>
      if (data contains soughtAddress)
        sender ! data(soughtAddress)
      else {
        val nearest = nearestNeighbor(soughtAddress)
        if(nearest == self)
          sender ! Status.Failure(KeyNotFound)
        else {
          nearest forward Read(soughtAddress)
        }
      }
  }
}

object Main {
  def main(args:Array[String]) {
    val sn = SingleNode
    if (args.length > 0) {
      val read = for {
        remote <- sn.system.actorSelection("akka.tcp://WSSystem@127.0.1.1:" + args(0) + "/user/node").resolveOne(1 second)
        written <- remote ? Write(1, "foobar")
        read <- remote ? Read(1)
      } yield read
      assert(Await.result(read, 1 second).asInstanceOf[String] == "foobar")
      println("Looks like everything worked.")
      sn.system.shutdown()
    }
  }
}


