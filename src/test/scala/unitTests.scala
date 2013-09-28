package akkaTutorial

import akka.actor._
import akka.pattern._

import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration._
import ExecutionContext.Implicits.global

import org.scalatest.FlatSpec
import Constants._;

class WSSpec extends FlatSpec {

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
    //Await.ready(Future.traverse(slr.nodes)(_ ? GetDegree andThen {case degree => assert(degree === 2)}), 1 second)
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