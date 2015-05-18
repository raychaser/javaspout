package com.sumologic.docker

import java.io.{InputStreamReader, BufferedReader}

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.command.{StatsCallback, EventCallback}
import com.github.dockerjava.api.model.{Statistics, Event}
import com.github.dockerjava.core.{DockerClientBuilder, DockerClientConfig}
import org.slf4j.{LoggerFactory, Logger}

object App {
  private val LOGGER: Logger = LoggerFactory.getLogger(classOf[App])

  def main(args: Array[String]) {
    val config = DockerClientConfig.createDefaultConfigBuilder()
      .withVersion("1.17")
      .withUri("unix:///var/run/docker.sock")
      .withMaxPerRouteConnections(256) // We will make long standing connections to the same route over and over again for stats and logs streaming
      .withMaxTotalConnections(256) // We are going to hit only one route so the limit should be the same here from all I can tell
      .build()
    val docker = DockerClientBuilder.getInstance(config).build()
    val eventCallback = new EventCallbackHandler(docker)
    val executorService = docker.eventsCmd(eventCallback).exec()
    LOGGER.info("Listening for events...")
    Thread.sleep(100000)
    executorService.shutdown()
  }
}

class EventCallbackHandler(val docker: DockerClient) extends EventCallback {
  private val LOGGER = LoggerFactory.getLogger(classOf[EventCallbackHandler])

  val logPumpById = scala.collection.mutable.HashMap.empty[String, LogPump]
  val statsPumpById = scala.collection.mutable.HashMap.empty[String, StatsPump]

  def addLogPump(id: String) {
    val pump = new LogPump(docker, id)
    logPumpById += (id -> pump)
  }

  def removeLogPump(id: String) {
    logPumpById -= id
  }

  def addStatsPump(id: String) {
    val pump = new StatsPump(docker, id)
    statsPumpById += (id -> pump)
  }

  def removeStatsPump(id: String) {
    statsPumpById -= id
  }

  override def onEvent(event: Event) {
    LOGGER.debug("event: " + event)
    val status = event.getStatus
    if (status == "start" || status == "restart") {
      LOGGER.info("start/restart, id: " + event.getId)
      addLogPump(event.getId)
      addStatsPump(event.getId)
    } else if (status == "die") {
      LOGGER.info("die, id: " + event.getId)
      removeLogPump(event.getId)
      removeStatsPump(event.getId)
    }
  }

  override def onException(t: Throwable) {
    t.printStackTrace()
  }

  override def onCompletion(n: Int) {
    LOGGER.debug("events received: " + n)
  }

  override def isReceiving(): Boolean = {
    true
  }
}

class LogPump(val docker: DockerClient, val id: String) {
  private val LOGGER = LoggerFactory.getLogger(classOf[LogPump])

  new Thread {
    override def run(): Unit = {
      LOGGER.info("starting, id: " + id)
      val response = docker.logContainerCmd(id)
        .withStdErr()
        .withStdOut()
        .withTailAll()
        .withTimestamps(true)
        .withFollowStream(true).exec()
      val reader = new BufferedReader(new InputStreamReader(response, "UTF-8"))
      var done = false
      while (!done) {
        val line = reader.readLine()
        if (line == null) {
          done = true
        } else {
          LOGGER.debug("id: " + id + ", " + line)
        }
      }
    }
  }.start()
}

object LogPump {

  def apply(docker: DockerClient, id: String): LogPump = {
    new LogPump(docker, id)
  }
}

class StatsPump(val docker: DockerClient, val id: String) {
  private val LOGGER = LoggerFactory.getLogger(classOf[StatsPump])

  new Thread {
    override def run(): Unit = {
      LOGGER.info("starting, id: " + id)
      val statsCallback = new StatsCallbackHandler(id)
      val executorService = docker.statsCmd(statsCallback).withContainerId(id).exec()
    }
  }.start()
}

class StatsCallbackHandler(val id: String) extends StatsCallback {
  private val LOGGER = LoggerFactory.getLogger(classOf[StatsPump])

  override def onStats(stats: Statistics) {
    LOGGER.debug("id: " + id + ", " + stats)
  }

  override def onException(t: Throwable) {
    t.printStackTrace()
  }

  override def onCompletion(n: Int) {
    LOGGER.debug("id: " + id + ", received: " + n)
  }

  override def isReceiving: Boolean = {
    true
  }
}

object StatsPump {

  def apply(docker: DockerClient, id: String): StatsPump = {
    new StatsPump(docker, id)
  }
}
