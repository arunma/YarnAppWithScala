package com.arunma

import java.io.File

import com.arunma.YarnAppUtils._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records

/**
  * @author Arun Manivannan
  */
object SampleYarnClient {

  def main(args: Array[String]): Unit = {

    val yarnClient = YarnClient.createYarnClient()
    implicit val conf = new YarnConfiguration()
    implicit val fs = FileSystem.get(conf)

    yarnClient.init(conf)
    yarnClient.start()
    val application = yarnClient.createApplication()

    //Copy bundle to HDFS
    val localJarPath = new Path(args(0))
    val yarnPath = fs.makeQualified(Path.mergePaths(new Path("/apps/"), localJarPath))
    fs.copyFromLocalFile(false, true, localJarPath, yarnPath)


    val commands = List(
      "$JAVA_HOME/bin/java " +
        " -Xmx256m " +
        s" com.arunma.ApplicationMaster " +
        " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
    )

    println(commands.last)

    val localResources = Map(
      "SampleYarnApp-assembly-0.1.jar" -> setUpLocalResourceFromPath(yarnPath) //Give the yarn path as local resource so that the container can download this and use it locally
    )

    val addlEnvVars = Map (
      "ARUN_JAR_PATH" -> yarnPath.toUri.getRawPath
    )

    val environment = buildEnvironment(addlEnvVars)

    val amContainer = createContainerContext(commands, localResources, environment)

    //Resource
    val resource = Resource.newInstance(1024, 2)

    //Priority
    val priority = Records.newRecord(classOf[Priority])
    priority.setPriority(1)

    val context = application.getApplicationSubmissionContext
    context.setAMContainerSpec(amContainer)
    context.setApplicationName("Scala Yarn App")
    context.setResource(resource)
    context.setPriority(priority)

    val appId = context.getApplicationId
    println("############ Submitting application with id : " + appId)

    yarnClient.submitApplication(context)

  }
}
