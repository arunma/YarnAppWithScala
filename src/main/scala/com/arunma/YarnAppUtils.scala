package com.arunma

import java.io.File
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records.{ContainerLaunchContext, LocalResource, LocalResourceType, LocalResourceVisibility}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier
import org.apache.hadoop.yarn.util.{Apps, ConverterUtils, Records}

import scala.collection.JavaConverters._

/**
  * @author Arun Manivannan
  */
object YarnAppUtils {

  def setUpLocalResourceFromPath(jarPath: Path)(implicit conf: Configuration): LocalResource = {
    val resource = Records.newRecord(classOf[LocalResource])
    val jarStatus = FileSystem.get(conf).getFileStatus(jarPath)
    resource.setResource(ConverterUtils.getYarnUrlFromPath(jarPath))
    resource.setSize(jarStatus.getLen())
    resource.setTimestamp(jarStatus.getModificationTime())
    resource.setType(LocalResourceType.FILE)
    resource.setVisibility(LocalResourceVisibility.PUBLIC)

    resource

  }

  def buildEnvironment(addlEnv: Map[String, String])(implicit conf: YarnConfiguration): Map[String, String] = {
    val classpath = conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH: _*)
    val envMap = new java.util.HashMap[String, String]()
    classpath.foreach(c => Apps.addToEnvironment(envMap, Environment.CLASSPATH.name(), c.trim, File.pathSeparator))

    Apps.addToEnvironment(envMap, Environment.CLASSPATH.name(), Environment.PWD.$() + File.pathSeparator + "*", File.pathSeparator)

    addlEnv.foreach { case (key, value) =>
      Apps.addToEnvironment(envMap, key, value, File.pathSeparator)
    }

    envMap.asScala.toMap
  }

  def createContainerContext(commands: List[String], resources: Map[String, LocalResource], environment: Map[String, String]): ContainerLaunchContext = {
    val launchContext = Records.newRecord(classOf[ContainerLaunchContext])
    launchContext.setCommands(commands.asJava)
    launchContext.setTokens(allTokens)
    launchContext.setLocalResources(resources.asJava)
    launchContext.setEnvironment(environment.asJava)
    launchContext
  }

  private def allTokens: ByteBuffer = {
    // creating the credentials for container execution
    val credentials = UserGroupInformation.getCurrentUser.getCredentials
    val dob = new DataOutputBuffer
    credentials.writeTokenStorageToStream(dob)
    ByteBuffer.wrap(dob.getData, 0, dob.getLength)
  }

}
