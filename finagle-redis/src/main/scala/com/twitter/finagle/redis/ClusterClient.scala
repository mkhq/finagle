package com.twitter.finagle.redis

import com.twitter.finagle.{ClientConnection, Redis, Service, ServiceFactory, ServiceFactoryProxy}
import com.twitter.finagle.redis.protocol._
import com.twitter.util.{Closable, Future, Time}

object ClusterClient {
  /**
   * Construct a cluster client from a single bootstrap host.
   * @param host a String of host:port combination.
   */
  def apply(host: String): ClusterClient =
    ClusterClient(Seq(host))

  /**
   * Construct a cluster client from a list of bootstrap hosts.
   * @param hosts a sequence of String of host:port combination.
   */
  def apply(hosts: Seq[String]): ClusterClient = {
    new ClusterClient(hosts)
  }
}

class ClusterClient(hosts: Seq[String])
    extends BaseClient
    with NormalCommands
    with ClusterCommands
    with Closable {

  private val Moved = "MOVED"
  private val MaxRedirects = 10

  private var conns = Map[String, ServiceFactory[Command, Reply]]()

  private def clientFor(host: String): ServiceFactory[Command, Reply] = {
    conns.getOrElse(host, {
      val factory = Redis.newClient(host)
      conns += (host -> factory)
      factory
    })
  }

  private def redirectRequest[T](
    cmd: Command,
    client: ServiceFactory[Command, Reply],
    remainingRedirects: Int = MaxRedirects
  )(handler: PartialFunction[Reply, Future[T]]): Future[T] = {
    client
      .toService
      .apply(cmd)
      .flatMap(handler orElse {
        case ErrorReply(message) if message.startsWith(Moved) =>
          val Array(_, slotId, host) = message.split(" ")
          redirectRequest[T](cmd, clientFor(host), remainingRedirects - 1)(handler)

        case ErrorReply(message) => Future.exception(new ServerError(message))
        case StatusReply("QUEUED") => Future.Done.asInstanceOf[Future[Nothing]]
        case _ => Future.exception(new IllegalStateException)
      })
  }

  private[redis] override def doRequest[T](
    cmd: Command
  )(handler: PartialFunction[Reply, Future[T]]): Future[T] = {
    val client = clientFor(hosts.head)

    redirectRequest[T](cmd, client)(handler)
  }

  def close(deadline: Time): Future[Unit] = {
    Future.join(conns.values.map(_.close(deadline)).toSeq)
  }

  override def select(index: Int): Future[Unit] =
    Future.exception(Exception("Not supported by Redis Cluster"))
}
