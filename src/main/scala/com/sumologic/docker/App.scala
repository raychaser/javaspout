package com.sumologic.docker

import java.io.{InputStreamReader, BufferedReader}

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.command.EventCallback
import com.github.dockerjava.api.model.Event
import com.github.dockerjava.core.{DockerClientBuilder, DockerClientConfig}

object App {

  def main(args: Array[String]) {
    val config = DockerClientConfig.createDefaultConfigBuilder()
      .withVersion("1.16")
      .withUri("unix:///var/run/docker.sock")
      .build()
    val docker = DockerClientBuilder.getInstance(config).build()
    val eventCallback = new EventCallbackHandler(docker)
    val executorService = docker.eventsCmd(eventCallback).exec()
    Thread.sleep(100000)
    executorService.shutdown()
  }

  class EventCallbackHandler(val docker: DockerClient) extends EventCallback {

    val logPumpById = scala.collection.mutable.HashMap.empty[String, LogPump]

    def addLogPump(id: String) {
      val pump = new LogPump(docker, id)
      logPumpById += (id -> pump)
    }

    def removeLogPump(id: String): Unit = {
      logPumpById -= id
    }

    override def onEvent(event: Event) {
      println("Event: " + event)
      val status = event.getStatus
      if (status == "start" || status == "restart") {
        println("START: " + event)
        addLogPump(event.getId)
      } else if (status == "die") {
        println("DIE: " + event)
        removeLogPump(event.getId)
      }
    }

    override def onException(t: Throwable) {
      t.printStackTrace()
    }

    override def onCompletion(n: Int) {
      println("Events received: " + n)
    }

    override def isReceiving(): Boolean = {
      return true
    }
  }

}

class LogPump(val docker: DockerClient, val id: String) {
  new Thread {
    override def run(): Unit = {
      println("PUMP: starting: " + id)
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
          done != true
        } else {
          println("Pump: " + id + ": " + line)
        }
      }
    }
  }.run()
}

object LogPump {

  def apply(docker: DockerClient, id: String): LogPump = {
    new LogPump(docker, id)
  }
}
