package akkaTutorial

import akka.actor._
import akka.pattern._
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration._
import ExecutionContext.Implicits.global

import org.scalatest.FlatSpec

object Constants {
  implicit val timeout = Timeout(1 second)
  val KeyNotFound = new Exception("Key not found")
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

object WSSpec extends FlatSpec {

  def actor(system:ActorSystem, name:String, address:Int) = system.actorOf(Props(new Node(address, new mutable.HashMap[Int, ActorRef],
    new collection.mutable.HashMap[Int,String])), name = name)

  def SingleNode = new {
    val system = ActorSystem("WSSystem")
    val node = actor(system, "node", 1)
  }

  "A single node" should "store items" in {
    val sn = SingleNode
    val read = for {
      write <- sn.node ? Write(1, "foobar")
      read <- sn.node ? Read(1)
    } yield read
    assert(Await.result(read, 1 second).asInstanceOf[String] === "foobar")
    sn.system.shutdown()
  }

  it should "throw an exception when asked to read nonexistent items" in {
    val sn = SingleNode
    val thrown = intercept[Exception] {
      println(Await.result(sn.node ? Read(1), 1 second).asInstanceOf[String])
    }
    assert(thrown === KeyNotFound)
  }

  def SinglyLinkedRing = new {
    val system = ActorSystem("WSSystem")
    val nodes = for(i <- 0 until 10) yield actor(system, "node" + i.toString, i)
    val pairs = (nodes.takeRight(1) ++ nodes zip nodes) ++ (nodes zip (nodes.takeRight(1) ++ nodes))
    Await.ready(Future.traverse(pairs)({t => t._1 ? AddNeighbor(t._2)}), 1 second)
  }

  "A singly linked ring" should "have degree 2 everywhere" in {
    val slr = SinglyLinkedRing
    slr.nodes foreach {
      node:ActorRef => node ? GetDegree onSuccess {
        case degree => {
          assert(degree === 2)
        }
      }
    }
    slr.system.shutdown()
  }

  it should "read data from across the ring" in {
    val slr = SinglyLinkedRing
    val read = for {
      write <- slr.nodes.take(6).last ? Write(5, "foobar")
      read <- slr.nodes.head ? Read(5)
    } yield read
    assert(Await.result(read, 1 second).asInstanceOf[String] === "foobar")
    slr.system.shutdown()
  }

  it should "get exceptions from across the ring" in {
    val slr = SinglyLinkedRing
    val thrown = intercept[Exception] {
      println(Await.result(slr.nodes.head ? Read(5), 1 second).asInstanceOf[String])
    }
    assert(thrown === KeyNotFound)
  }

  it should "write data to across the ring" in {
    val slr = SinglyLinkedRing
    val read = for {
      write <- slr.nodes.head ? Write(0, "foobar")
      read <-  slr.nodes.take(6).last ? Read(0)
    } yield read
    assert(Await.result(read, 1 second).asInstanceOf[String] === "foobar")
    slr.system.shutdown()
  }
}

object WattsStrogatz extends App {
  WSSpec.execute()
}


