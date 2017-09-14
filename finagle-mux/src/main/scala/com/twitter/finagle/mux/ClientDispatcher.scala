package com.twitter.finagle.mux

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.mux.transport.{Message, MuxFailure}
import com.twitter.finagle.mux.util.TagMap
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Dtab, Filter, Failure, Service, Status}
import com.twitter.util.{Future, Promise, Return, Throw, Time, Try, Updatable}
import scala.util.control.NoStackTrace

/**
 * Indicates that the server failed to interpret or act on the request. This
 * could mean that the client sent a [[com.twitter.finagle.mux]] message type
 * that the server is unable to process.
 */
case class ServerError(what: String) extends Exception(what) with NoStackTrace

/**
 * Indicates that the server encountered an error whilst processing the client's
 * request. In contrast to [[com.twitter.finagle.mux.ServerError]], a
 * ServerApplicationError relates to server application failure rather than
 * failure to interpret the request.
 */
case class ServerApplicationError(what: String) extends Exception(what) with NoStackTrace

/**
 * Implements a dispatcher for a mux client. The dispatcher implements the bookkeeping
 * and transactions for outstanding messages and as such exposes an interface where
 * tag assignment can be deferred (i.e. Int => Message).
 */
private[finagle] class ClientDispatcher(trans: Transport[Message, Message])
    extends Service[Int => Message, Message] {
  import ClientDispatcher._

  // outstanding messages, each tagged with a unique int
  // between `MinTag` and `MaxTag`.
  private[this] val messages = TagMap[Updatable[Try[Message]]](TagRange, InitialTagMapSize)

  private[this] val processAndRead: Message => Future[Unit] =
    msg => {
      process(msg)
      readLoop()
    }

  /**
   * Reads indefinitely from the transport, handing successfully
   * decoding messages to `processAndRead`.
   */
  private[this] def readLoop(): Future[Unit] =
    trans.read().flatMap(processAndRead)

  readLoop().onFailure {
    case exc: Throwable =>
      trans.close()
      val result = Throw(exc)
      for (u <- messages.synchronized(messages.unmapAll())) {
        u() = result
      }
  }

  private[this] def process(msg: Message): Unit = msg match {
    case Message.Rerr(_, err) =>
      for (u <- messages.synchronized(messages.unmap(msg.tag)))
        u() = Throw(ServerError(err))

    case Message.Rmessage(_) =>
      for (u <- messages.synchronized(messages.unmap(msg.tag)))
        u() = Return(msg)

    case _ => // do nothing.
  }

  /**
   * Dispatch the message resulting from applying `f`
   * with a fresh tag.
   */
  def apply(f: Int => Message): Future[Message] = {
    val p = new Promise[Message]
    messages.synchronized(messages.map(p)) match {
      case None => FutureExhaustedTagsException
      case Some(tag) =>
        val msg = f(tag)
        trans.write(msg).transform {
          case Return(_) =>
            p.setInterruptHandler {
              case cause =>
                // We have to ensure that the Updatable in the TagMap is the same one that
                // this dispatch created. If it is, we need to replace it with a stand-in
                // to reserve the tag of discarded requests until Tdiscarded is
                // acknowledged by the peer.
                val interrupted = messages.synchronized {
                  messages.maybeRemap(msg.tag, Updatable.Empty) match {
                    case Some(u) if u eq p =>
                      // We have to send the Tdiscarded from within the lock to ensure
                      // that we don't race with receiving the response, unmapping the
                      // Empty thus freeing the tag, and sending a new dispatch which
                      // reuses the tag, then sending this Tdiscarded which will
                      // mistakenly interrupt the wrong dispatch on the server side.
                      // This noise could be avoided if we didn't reuse tags.
                      trans.write(Message.Tdiscarded(msg.tag, cause.toString))
                      true

                    case Some(u) =>
                      // Not our dispatch, so remap it.
                      messages.maybeRemap(msg.tag, u)
                      false

                    case None =>
                      false
                  }
                }

                if (interrupted) {
                  p.updateIfEmpty(Throw(cause))
                }
            }
            p
          case t @ Throw(_) =>
            messages.synchronized(messages.unmap(tag))
            Future.const(t.cast[Message])
        }
    }
  }

  override def status: Status = trans.status
  override def close(when: Time): Future[Unit] = trans.close(when)
}

private[finagle] object ClientDispatcher {

  val TagRange: Range = Message.Tags.MinTag to Message.Tags.MaxTag

  val InitialTagMapSize: Int = 256

  val FutureExhaustedTagsException = Future.exception(Failure.rejected("Exhausted tags"))

  /**
   * Creates a mux client dispatcher that can handle mux Request/Responses.
   */
  def newRequestResponse(trans: Transport[Message, Message]): Service[Request, Response] =
    new ReqRepFilter andThen new ClientDispatcher(trans)
}

/**
 * Bridges a mux ClientDispatcher to mux.Requests/mux.Responses. This includes support
 * for `Tdispatch` while downgrading to `Treq` if neccessary.
 */
private class ReqRepFilter extends Filter[Request, Response, Int => Message, Message] {
  import ReqRepFilter._

  // We currently attempt a Tdispatch and if it fails set canDispatch to No.
  // When Tinits/Rinits arrive, we should use them to signal this capability
  // instead.
  @volatile private[this] var canDispatch: CanDispatch.State = CanDispatch.Unknown

  private[this] def reply(msg: Try[Message]): Future[Response] = msg match {
    case Return(Message.RreqOk(_, rep)) =>
      Future.value(Response(rep))

    case Return(Message.RreqError(_, error)) =>
      Future.exception(ServerApplicationError(error))

    case Return(Message.RdispatchOk(_, _, rep)) =>
      Future.value(Response(rep))

    case Return(Message.RdispatchError(_, contexts, error)) =>
      val appError = ServerApplicationError(error)
      val exn = MuxFailure.fromContexts(contexts) match {
        case Some(f) => Failure(appError, f.finagleFlags)
        case None => appError
      }
      Future.exception(exn)

    case Return(Message.RdispatchNack(_, contexts)) =>
      val exn = MuxFailure.fromContexts(contexts) match {
        case Some(f) => Failure(Failure.RetryableNackFailure.why, f.finagleFlags)
        case None => Failure.RetryableNackFailure
      }
      Future.exception(exn)

    case Return(Message.RreqNack(_)) =>
      Failure.FutureRetryableNackFailure

    case t @ Throw(_) => Future.const(t.cast[Response])
    case Return(m) => Future.exception(Failure(s"unexpected response: $m"))
  }

  def apply(req: Request, svc: Service[Int => Message, Message]): Future[Response] = {
    val couldDispatch = canDispatch

    val msg = couldDispatch match {
      case CanDispatch.No => { tag: Int =>
        Message.Treq(tag, Some(Trace.id), req.body)
      }

      case CanDispatch.Yes | CanDispatch.Unknown => { tag: Int =>
        val contexts = Contexts.broadcast.marshal()
        Message.Tdispatch(tag, contexts.toSeq, req.destination, Dtab.local, req.body)
      }
    }

    if (couldDispatch != CanDispatch.Unknown) svc(msg).transform(reply)
    else
      svc(msg).transform {
        case Throw(ServerError(_)) =>
          // We've determined that the server cannot handle Tdispatch messages,
          // so we fall back to a Treq.
          canDispatch = CanDispatch.No
          apply(req, svc)

        case r @ Return(_) =>
          canDispatch = CanDispatch.Yes
          reply(r)

        case t @ Throw(_) => reply(t)
      }
  }
}

private object ReqRepFilter {

  /** Indicates if our peer can accept `Tdispatch` messages. */
  object CanDispatch extends Enumeration {
    type State = Value
    val Unknown, Yes, No = Value
  }
}
