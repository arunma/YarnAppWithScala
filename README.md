This is an attempt on a minimal example of a YARN application on Scala.

There are three core classes in this App :

1. `SampleYarnClient`
2. `ApplicationMaster`
3. `DummyApplication` (the business logic) 

### 1. SampleYarnClient

This is the entry-point of the program. Does the following :
 
1. Instantiates `YarnClient`
2. Negotiates resources for the ApplicationMaster container with the help of the `YarnClient`.  The way it does it is to initiate an `ApplicationSubmissionContext` which is just a wrapper around the `Resource`, `Priority` and `ContainerLaunchContext` (among others).  Let's quickly look at the code and we'll go over in detail on these three components of the SubmissionContext.

```scala
val yarnClient = YarnClient.createYarnClient()
...
val application = yarnClient.createApplication()
...
val context = application.getApplicationSubmissionContext
context.setAMContainerSpec(amContainer)
context.setApplicationName("Scala Yarn App")
context.setResource(resource)
context.setPriority(priority)

yarnClient.submitApplication(context)
```

####a. Resource

Resources is a simple wrapper around CPU and Memory

```scala
val resource = Resource.newInstance(1024, 2) //1 GB memory and 2 cores
```

####b. Priority

Priority is just an Integer - the higher the number, the higher the priority

```scala
val priority = Records.newRecord(classOf[Priority])
priority.setPriority(1)
```

####c. ContainerLaunchContext

The ContainerLaunchRequest has three primary parameters in this simple example :
 
1. Commands (`List[String]`): The bootstrap command (ideally `java <MainClass>`)
2. LocalResources (`Map[String,LocalResource]`) : The jars and the other artifacts (properties, libraries etc) that's essential for running your command
3. Environment (`Map[String,String]`): The environment variables essential for the program 

(the other important one is the Security Tokens which is not used here because my local cluster isn't kerberized)

```scala
  def createContainerContext(commands: List[String], resources: Map[String, LocalResource], environment: Map[String, String]): ContainerLaunchContext = {
    val launchContext = Records.newRecord(classOf[ContainerLaunchContext])
    launchContext.setCommands(commands.asJava)
    launchContext.setLocalResources(resources.asJava)
    launchContext.setEnvironment(environment.asJava)
    launchContext
  }
```

######Commands 
Like I said, the commands are just a sequence of instructions you would like to execute to run the ApplicationMaster from the shell.

```scala
val commands = List(
      "$JAVA_HOME/bin/java " +
        " -Xmx256m " +
        s" com.arunma.ApplicationMaster " +
        " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
    )
```
######LocalResources

As for this program, you don't need any properties or configuration files. All you need is just the jar binary alone. 

**Note :**
The way that you make any binary or resource available for YARN is to place the binaries in a HDFS location. This particular step involving the setting up of local resources just means that we are telling YARN to download the binaries from the HDFS location and place in in the local path of the container when launched. 

```scala
val localResources = Map(
      "SampleYarnApp-assembly-0.1.jar" -> setUpLocalResourceFromPath(yarnPath) 
    )
```

######Environment
These are custom environment variables or just the classpath that needs to be set for your bundle to run.

### 2. ApplicationMaster

Now that we have discussed about the `SampleYarnClient`, let's discuss the second. This class, as the name indicates, is your `ApplicationMaster` (Duh!). It's responsible for launching the containers that is expected to run your "business logic".  The steps are : 

1. The AppMaster uses the ResourceManager client (the `AMRMClient`) to raise a request for a container - `ContainerRequest`. This example uses the async version of the client - `AMRMClientAsync` that implements a series of callbacks - `onContainersAllocated`, `onContainersCompleted`, `onError` etc .  
2. When the RM allocates a container for the application, the `onContainersAllocated` callback gets invoked.
3. Within the `onContainersAllocated` (now that we have the handle to the container), the AppMaster then uses the `NMClientAsync` to launch the "business" container (`DummyApplication`).  This is achieved by constructing another `ContainerLaunchContext` (the one that wraps commands, local resources and environment variables).

### 3. DummyApplication

This is the "business logic".  Not `Scala` in the purest sense but it helps us see the logs. Note that because this is an infinite loop, we'll have to forcefully kill the application.

```scala
object DummyApplication extends App {
   
     while(true) {
       println("Niceeeeeeeeee !!! This is the core application that is running within the container that got negotiated by from Application Master !!!")
       Thread.sleep(1000)
     }
   }
```


##Usage:


```bash
$ hadoop jar /Users/arun/IdeaProjects/SampleYarnApp/target/scala-2.11/SampleYarnApp-assembly-0.1.jar com.arunma.SampleYarnClient /Users/arun/IdeaProjects/SampleYarnApp/target/scala-2.11/SampleYarnApp-assembly-0.1.jar
```
  
  
Alternatively, you could just do an 

```sbtshell
sbt assembly
```

and run the `SampleYarnClient` from your IDE with the absolute path of the assembly jar as the first argument.

