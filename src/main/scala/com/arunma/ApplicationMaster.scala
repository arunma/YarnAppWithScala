package com.arunma

import java.nio.ByteBuffer
import java.util

import com.arunma.YarnAppUtils._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records

import scala.collection.JavaConverters._

/**
  * @author Arun Manivannan
  */
class ApplicationMaster extends AMRMClientAsync.CallbackHandler {
  private var rmClient: AMRMClientAsync[ContainerRequest] = _
  private var nmClient: NMClientAsync = _
  private implicit val conf = new YarnConfiguration()

  def runContainer(): Unit = {
    rmClient = AMRMClientAsync.createAMRMClientAsync[ContainerRequest](1000, this)
    rmClient.init(conf)
    rmClient.start()

    println("###################### RM Client started")

    nmClient = NMClientAsync.createNMClientAsync(new NMClientCallback())
    nmClient.init(conf)
    nmClient.start()

    println("###################### NM Client started")


    val regResponse = rmClient.registerApplicationMaster("", 0, "")
    val maxMemory = Math.min(regResponse.getMaximumResourceCapability.getMemory, 1024)
    val maxCpu = Math.min(regResponse.getMaximumResourceCapability.getVirtualCores, 1)


    val resource = Resource.newInstance(maxMemory, maxCpu)
    val priority = Records.newRecord(classOf[Priority])
    priority.setPriority(1)

    println("###################### Raising container request")
    rmClient.addContainerRequest(new ContainerRequest(resource, null, null, priority))
    println("###################### Container request raised")


  }


  //Callback methods of AMRMClientAsync
  override def onContainersAllocated(containers: util.List[Container]): Unit = {

    println(s" ########### Containers allocated ${containers.size()} ")

    val commands = List(
      "$JAVA_HOME/bin/java " +
        " -Xmx256m " +
        s" com.arunma.DummyApplication " +
        " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
    )


    val localResources = Map(
      "SampleYarnApp-assembly-0.1.jar" -> setUpLocalResourceFromPath(FileSystem.get(conf).makeQualified(new Path(sys.env("ARUN_JAR_PATH"))))
    )

    val containerLaunchContext = createContainerContext(commands, localResources, buildEnvironment(Map()))

    containers.asScala.foreach { container =>
      nmClient.startContainerAsync(container, containerLaunchContext)
    }

  }

  def stopApplication(status: FinalApplicationStatus, message: String): Unit = {
    rmClient.unregisterApplicationMaster(status, message, null)
    rmClient.stop()
    nmClient.stop()
  }

  override def onContainersCompleted(statuses: util.List[ContainerStatus]): Unit = {
    println("################## Container task completed ")
    statuses.asScala.foreach{ status =>
      rmClient.releaseAssignedContainer(status.getContainerId)
    }
    stopApplication(FinalApplicationStatus.SUCCEEDED, "SUCCESS")
  }

  override def onError(e: Throwable): Unit = {
    println(s"################### ERROR in container ################ ${e.getMessage}")
    e.printStackTrace()
    stopApplication(FinalApplicationStatus.FAILED, e.getMessage)
  }

  override def onShutdownRequest(): Unit = {
    println("################# onShutdownRequest")
    stopApplication(FinalApplicationStatus.FAILED, "KILLED")
  }

  override def onNodesUpdated(updatedNodes: util.List[NodeReport]): Unit = println("################# onNodesUpdated")

  override def getProgress: Float = 100

}

object ApplicationMaster extends App {
    println("###################### Application master STARTUP !!!")
    new ApplicationMaster().runContainer()
}

class NMClientCallback extends NMClientAsync.CallbackHandler {
  override def onContainerStarted(containerId: ContainerId, allServiceResponse: util.Map[String, ByteBuffer]): Unit = println(s"onContainerStarted $containerId")

  override def onContainerStatusReceived(containerId: ContainerId, containerStatus: ContainerStatus): Unit = println(s"onContainerStatusReceived $containerId")

  override def onContainerStopped(containerId: ContainerId): Unit = println(s"onContainerStopped $containerId")

  override def onStartContainerError(containerId: ContainerId, t: Throwable): Unit = println(s"onStartContainerError started $containerId ${t.getMessage}")

  override def onGetContainerStatusError(containerId: ContainerId, t: Throwable): Unit = println(s"onGetContainerStatusError started $containerId ${t.getMessage}")

  override def onStopContainerError(containerId: ContainerId, t: Throwable): Unit = println(s"onStopContainerError started $containerId ${t.getMessage}")
}


