package part3_ono

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Address, Props, ReceiveTimeout}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.dispatch.{PriorityGenerator, UnboundedPriorityMailbox}
import akka.util.Timeout

import scala.concurrent.duration._
import akka.pattern.pipe
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Random
import scala.util.{Failure, Success}



object OnoClusteringDomain {
  case class ProcessFile(filePath: String)
  case class ProcessLine(line: String, aggregator:ActorRef)
  case class ProcessLineResult(count: Int)
  case class RegisterWorker(address:Address, ref:ActorRef)
}

class OnoClusteringMailBox (settings: ActorSystem.Settings, config:Config) extends UnboundedPriorityMailbox (
  PriorityGenerator {
    case _: MemberEvent => 0
    case _ => 4
  }
)


class OnoMaster extends Actor with ActorLogging {

  import OnoClusteringDomain._
  import context.dispatcher
  implicit val timeout =  Timeout(3 seconds)

  val cluster = Cluster(context.system)

  var workers: Map[Address, ActorRef] = Map()
  var pendingRemoval: Map[Address, ActorRef] = Map()

    override def preStart(): Unit = {
      cluster.subscribe(
        self,
        initialStateMode = InitialStateAsEvents,
        classOf[MemberEvent],
        classOf[UnreachableMember]
      )
    }

    override def postStop(): Unit = {
      cluster.unsubscribe(self)
    }

    override def receive: Receive = 
      handleClusterEvents
      .orElse(handleWorkerRegistration)
      .orElse(handleJob)


    def handleClusterEvents: Receive = {
      case MemberUp(member) if member.hasRole("master") => 
          println("master:memberup:master_address", member.address)

      case MemberUp(member) if member.hasRole("worker") => 
        println("master:handleClusterEvents:", member)
        if (pendingRemoval.contains(member.address)) {
          pendingRemoval = pendingRemoval - member.address
        } else {
          println("master:handleClusterEvents::newWorker", member)
          val addr = s"${member.address}/user/worker}"
          println("master:handleClusterEvents::newWorker:addr", addr)
          val newActorSelection = context.actorSelection(addr)
          //println("master:handleClusterEvents::newWorker:newActorSelection", newActorSelection)
          context.actorSelection(addr).resolveOne(3 seconds)
            .onComplete {
            case Success(ref) => self ! RegisterWorker(member.address, ref)  
            case Failure(ex) => 
              println(s"master:resolve:failed:self:path ${self.path}")
              println(s"master:resolve:failed: $addr")
              println(s"master:resolve:failed:ex: $ex")
            }
          /*newActorSelection.resolveOne(3 seconds).map{ref => 
            println(s"master:newActorSelection.resolveOne: ${member.address}")
            RegisterWorker(member.address, ref)
          }.pipeTo(self)*/
        }
      case UnreachableMember(member) =>
        val workerOption = workers.get(member.address)
        workerOption.foreach{ actorRef =>
          pendingRemoval = pendingRemoval + (member.address -> actorRef)
        }

      case MemberRemoved(member, previousStatus) =>
        workers = workers - member.address


      case m: MemberEvent =>
        log.info(s"new member event: $m")
    }

    def handleWorkerRegistration: Receive = {
      case  rw: RegisterWorker =>
        println("master:handleWorkerRegistration:", rw.address, rw.ref)
        workers = workers ++ Map(rw.address -> rw.ref)
    }
    def handleJob: Receive = {
      case ProcessFile(filePath) =>
        println("master:looping text file")
        val aggregator = context.actorOf(Props[OnoAggregator], "aggregator")
        scala.io.Source.fromFile(filePath).getLines.foreach{ line =>
          self ! ProcessLine(line, aggregator)
        }
      case ProcessLine(line, aggregator) =>
        val workerIndex = Random.nextInt((workers -- pendingRemoval.keys).size)
        val worker: ActorRef = (workers -- pendingRemoval.keys).values.toSeq(workerIndex)
        println("master:passing to worker")
        worker ! ProcessLine(line, aggregator)
    }

}

class  OnoAggregator extends Actor with ActorLogging {
  import OnoClusteringDomain._
  context.setReceiveTimeout(3 seconds)

  override def receive: Receive = online(0)

  def online(totalCount: Int): Receive = {
    case ProcessLineResult(count) =>
      context.become(online(totalCount + count))
    case ReceiveTimeout =>
      log.info(s"TOTAL COUNT:$totalCount")
      context.setReceiveTimeout(Duration.Undefined)
  }


}


class OnoWorker extends Actor with ActorLogging {
   import OnoClusteringDomain._

  override def receive(): Receive = {
    case ProcessLine(line, aggregator) =>
      aggregator ! ProcessLineResult(line.split(" ").length)
  }
}


object OnoSeedNodes extends App {
    import OnoClusteringDomain._ 

  
    def createNode(port: Int, role: String, props: Props, actorName: String): Unit = {
      val config = ConfigFactory.parseString(
        s"""
        | akka.cluster.roles = ["$role"]
        | akka.remote.artery.canonical.port = $port
        """.stripMargin
        ).withFallback(ConfigFactory.load("part3_clustering/clusteringExample.conf"))

      val system = ActorSystem("RTJVMCluster", config)
      import system.dispatcher
      val actor = system.actorOf(props, actorName)
      if (actorName=="master" ){
        system.scheduler.scheduleOnce(20 seconds ) {
          println("waiting finished")
          println(s"master: $actor")
          system.scheduler.scheduleOnce(2 seconds) {
            actor ! ProcessFile("src/main/resources/txt/lipsum.txt")
          }
        }

      }
    }


    createNode(12553, "worker", Props[OnoWorker], "worker")
    createNode(12552, "worker", Props[OnoWorker], "worker")
    createNode(12551, "master", Props[OnoMaster], "master")

    //Thread.sleep(10000)



}
