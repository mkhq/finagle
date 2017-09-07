package com.twitter.finagle

import com.twitter.finagle.param.Stats
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.stats._
import com.twitter.finagle.thrift._
import com.twitter.finagle.util.Showable
import java.lang.reflect.Constructor
import java.net.SocketAddress
import org.apache.thrift.protocol.TProtocolFactory
import scala.reflect.ClassTag

private[twitter] object ThriftUtil {
  private type BinaryService = Service[Array[Byte], Array[Byte]]

  private def findClass1(name: String): Option[Class[_]] =
    try Some(Class.forName(name))
    catch {
      case _: ClassNotFoundException => None
    }

  private def findClass[A](name: String): Option[Class[A]] =
    for {
      cls <- findClass1(name)
    } yield cls.asInstanceOf[Class[A]]

  private def findConstructor[A](clz: Class[A], paramTypes: Class[_]*): Option[Constructor[A]] =
    try {
      Some(clz.getConstructor(paramTypes: _*))
    } catch {
      case _: NoSuchMethodException => None
    }

  private def findRootWithSuffix(str: String, suffix: String): Option[String] =
    if (str.endsWith(suffix))
      Some(str.stripSuffix(suffix))
    else
      None

  /**
   * Construct an `Iface` based on an underlying [[com.twitter.finagle.Service]]
   * using whichever Thrift code-generation toolchain is available.
   */
  private[finagle] def constructIface[Iface](
    underlying: Service[ThriftClientRequest, Array[Byte]],
    cls: Class[_],
    clientParam: RichClientParam
  ): Iface = {
    val clsName = cls.getName

    // This is used with Scrooge's Java generated code.
    // The class name passed in should be ServiceName$ServiceIface.
    // Will try to create a ServiceName$ServiceToClient instance.
    def tryJavaServiceNameDotServiceIface: Option[Iface] =
      for {
        baseName <- findRootWithSuffix(clsName, "$ServiceIface")
        clientCls <- findClass[Iface](baseName + "$ServiceToClient")
        cons <- findConstructor(
          clientCls,
          classOf[Service[_, _]],
          classOf[RichClientParam]
        )
      } yield {
        cons.newInstance(underlying, clientParam)
      }

    // This is used with Scrooge's Scala generated code.
    // The class name passed in should be ServiceName$FutureIface
    // or the higher-kinded version, ServiceName[Future].
    // Will try to create a ServiceName$FinagledClient instance.
    def tryScalaServiceNameIface: Option[Iface] =
      for {
        baseName <- findRootWithSuffix(clsName, "$FutureIface")
          .orElse(Some(clsName))
        clientCls <- findClass[Iface](baseName + "$FinagledClient")
        cons <- findConstructor(
          clientCls,
          classOf[Service[_, _]],
          classOf[RichClientParam]
        )
      } yield cons.newInstance(underlying, clientParam)

    val iface =
      tryJavaServiceNameDotServiceIface
        .orElse(tryScalaServiceNameIface)

    iface.getOrElse {
      throw new IllegalArgumentException(
        s"Iface $clsName is not a valid thrift iface. For Scala generated code, " +
          "try `YourServiceName$FutureIface` or `YourServiceName[Future]. " +
          "For Java generated code, try `YourServiceName$ServiceIface`."
      )
    }
  }

  /**
   * Construct a binary [[com.twitter.finagle.Service]] for a given Thrift
   * interface using whichever Thrift code-generation toolchain is available.
   */
  def serverFromIface(
    impl: AnyRef,
    serverParam: RichServerParam
  ): BinaryService = {
    // This is used with Scrooge's Java generated code.
    // The class passed in should be ServiceName$ServiceIface.
    // Will try to create a ServiceName$Service instance.
    def tryThriftFinagleService(iface: Class[_]): Option[BinaryService] =
      for {
        baseName <- findRootWithSuffix(iface.getName, "$ServiceIface")
        serviceCls <- findClass[BinaryService](baseName + s"$$Service")
        cons <- findConstructor(serviceCls, iface, classOf[RichServerParam])
      } yield {
        cons.newInstance(impl, serverParam)
      }

    // This is used with Scrooge's Scala generated code.
    // The class passed in should be ServiceName$FutureIface,
    // ServiceName$FutureIface, or ServiceName.
    // Will try to create a ServiceName$FinagleService.
    def tryScroogeFinagleService(iface: Class[_]): Option[BinaryService] =
      (for {
        baseName <- findRootWithSuffix(iface.getName, "$FutureIface")
        // handles ServiceB extends ServiceA, then using ServiceB$MethodIface
          .orElse(findRootWithSuffix(iface.getName, "$MethodIface"))
          .orElse(Some(iface.getName))
        serviceCls <- findClass[BinaryService](baseName + "$FinagleService")
        baseClass <- findClass1(baseName)
      } yield {
        findConstructor(
          serviceCls,
          baseClass,
          classOf[RichServerParam]
        ).map { cons =>
          cons.newInstance(impl, serverParam)
        }
      }).flatten

    def tryClass(cls: Class[_]): Option[BinaryService] =
      tryThriftFinagleService(cls)
        .orElse(tryScroogeFinagleService(cls))
        .orElse {
          (Option(cls.getSuperclass) ++ cls.getInterfaces).view.flatMap(tryClass).headOption
        }

    tryClass(impl.getClass).getOrElse {
      throw new IllegalArgumentException(
        s"$impl implements no candidate ifaces. For Scala generated code, " +
          "try `YourServiceName$FutureIface`, `YourServiceName$MethodIface` or `YourServiceName`. " +
          "For Java generated code, try `YourServiceName$ServiceIface`."
      )
    }
  }

  @deprecated("Use com.twitter.finagle.RichServerParam", "2017-08-16")
  def serverFromIface(
    impl: AnyRef,
    protocolFactory: TProtocolFactory,
    stats: StatsReceiver = NullStatsReceiver,
    maxThriftBufferSize: Int = Thrift.param.maxThriftBufferSize,
    label: String
  ): BinaryService = {
    serverFromIface(impl, RichServerParam(protocolFactory, label, maxThriftBufferSize, stats))
  }

  /**
   * Construct a multiplexed binary [[com.twitter.finagle.Service]].
   */
  def serverFromIfaces(
    ifaces: Map[String, AnyRef],
    defaultService: Option[String],
    serverParam: RichServerParam
  ): BinaryService = {
    val services = ifaces.map {
      case (serviceName, impl) =>
        serviceName -> serverFromIface(impl, serverParam)
    }
    new MultiplexedFinagleService(
      services,
      defaultService,
      serverParam.protocolFactory,
      serverParam.maxThriftBufferSize
    )
  }

  @deprecated("Use com.twitter.finagle.RichServerParam", "2017-08-16")
  def serverFromIfaces(
    ifaces: Map[String, AnyRef],
    defaultService: Option[String],
    protocolFactory: TProtocolFactory,
    stats: StatsReceiver,
    maxThriftBufferSize: Int,
    label: String
  ): BinaryService = {
    serverFromIfaces(
      ifaces,
      defaultService,
      RichServerParam(
        protocolFactory,
        label,
        maxThriftBufferSize,
        stats
      )
    )
  }

  /**
   * Construct a binary [[com.twitter.finagle.Service]] for a given Thrift
   * interface using whichever Thrift code-generation toolchain is available.
   * (Legacy version for backward-compatibility).
   */
  @deprecated("Use com.twitter.finagle.RichServerParam", "2017-08-16")
  def serverFromIface(
    impl: AnyRef,
    protocolFactory: TProtocolFactory,
    serviceName: String
  ): BinaryService = {
    serverFromIface(impl, RichServerParam(protocolFactory, serviceName))
  }
}

/**
 * A mixin trait to provide a rich Thrift client API.
 *
 * @define clientUse
 *
 * Create a new client of type `Iface`, which must be generated
 * by [[https://github.com/twitter/scrooge Scrooge]].
 *
 * For Scala generated code, the `Class` passed in should be
 * either `ServiceName$FutureIface` or `ServiceName[Future]`.
 *
 * For Java generated code, the `Class` passed in should be
 * `ServiceName$ServiceIface`.
 *
 * @define serviceIface
 *
 * Construct a Finagle `Service` interface for a Scrooge-generated Thrift object.
 *
 * E.g. given a Thrift service
 * {{{
 *   service Logger {
 *     string log(1: string message, 2: i32 logLevel);
 *     i32 getLogSize();
 *   }
 * }}}
 *
 * you can construct a client interface with a Finagle Service per Thrift method:
 *
 * {{{
 *   val loggerService = Thrift.client.newServiceIface[Logger.ServiceIface]("localhost:8000", "client_label")
 *   val response = loggerService.log(Logger.Log.Args("log message", 1))
 * }}}
 *
 * @define buildMultiplexClient
 *
 * Build client interfaces for multiplexed thrift serivces.
 *
 * E.g.
 * {{{
 *   val client = Thrift.client.multiplex(address, "client") { client =>
 *     new {
 *       val echo = client.newIface[Echo.FutureIface]("echo")
 *       val extendedEcho = client.newServiceIface[ExtendedEcho.ServiceIface]("extendedEcho")
 *     }
 *   }
 *
 *   client.echo.echo("hello")
 *   client.extendedEcho.getStatus(ExtendedEcho.GetStatus.Args())
 * }}}
 */
trait ThriftRichClient { self: Client[ThriftClientRequest, Array[Byte]] =>
  import ThriftUtil._

  protected val clientParam: RichClientParam

  protected def protocolFactory: TProtocolFactory

  /** The client name used when group isn't named. */
  protected val defaultClientName: String
  protected def stats: StatsReceiver = ClientStatsReceiver

  /**
   * The `Stack.Params` to be used by this client.
   *
   * Both [[defaultClientName]] and [[stats]] predate [[Stack.Params]]
   * and as such are implemented separately.
   */
  protected def params: Stack.Params

  /**
   * $clientUse
   */
  def newIface[Iface](dest: String, cls: Class[_]): Iface = {
    val (n, l) = Resolver.evalLabeled(dest)
    newIface(n, l, cls)
  }

  /**
   * $clientUse
   */
  def newIface[Iface](dest: String, label: String, cls: Class[_]): Iface =
    newIface(Resolver.eval(dest), label, cls)

  /**
   * $clientUse
   */
  def newIface[Iface: ClassTag](dest: String): Iface = {
    val (n, l) = Resolver.evalLabeled(dest)
    newIface[Iface](n, l)
  }

  /**
   * $clientUse
   */
  def newIface[Iface: ClassTag](dest: String, label: String): Iface = {
    val cls = implicitly[ClassTag[Iface]].runtimeClass
    newIface[Iface](Resolver.eval(dest), label, cls)
  }

  /**
   * $clientUse
   */
  def newIface[Iface: ClassTag](dest: Name, label: String): Iface = {
    val cls = implicitly[ClassTag[Iface]].runtimeClass
    newIface[Iface](dest, label, cls)
  }

  /**
   * $clientUse
   */
  def newIface[Iface](name: Name, label: String, cls: Class[_]): Iface = {
    newIface(name, label, cls, clientParam, newService(name, label))
  }

  /**
   * $clientUse
   */
  def newIface[Iface](
    name: Name,
    label: String,
    cls: Class[_],
    clientParam: RichClientParam,
    service: Service[ThriftClientRequest, Array[Byte]]
  ): Iface = {
    val clientLabel = (label, defaultClientName) match {
      case ("", "") => Showable.show(name)
      case ("", l1) => l1
      case (l0, l1) => l0
    }

    val clientConfigScoped =
      clientParam.copy(clientStats = clientParam.clientStats.scope(clientLabel))
    constructIface(service, cls, clientConfigScoped)
  }

  @deprecated("Use com.twitter.finagle.RichClientParam", "2017-8-16")
  def newIface[Iface](
    name: Name,
    label: String,
    cls: Class[_],
    protocolFactory: TProtocolFactory,
    service: Service[ThriftClientRequest, Array[Byte]]
  ): Iface = {
    val clientLabel = (label, defaultClientName) match {
      case ("", "") => Showable.show(name)
      case ("", l1) => l1
      case (l0, l1) => l0
    }

    val clientConfigScoped = clientParam.copy(
      protocolFactory = protocolFactory,
      clientStats = clientParam.clientStats.scope(clientLabel)
    )
    constructIface(service, cls, clientConfigScoped)
  }

  /**
   * $serviceIface
   *
   * @param builder The builder type is generated by Scrooge for a thrift service.
   * @param dest Address of the service to connect to, in the format accepted by `Resolver.eval`.
   * @param label Assign a label for scoped stats.
   */
  def newServiceIface[ServiceIface <: ThriftServiceIface.Filterable[ServiceIface]](
    dest: String,
    label: String
  )(
    implicit builder: ServiceIfaceBuilder[ServiceIface]
  ): ServiceIface =
    newServiceIface(Resolver.eval(dest), label)

  /**
   * $serviceIface
   *
   * @param builder The builder type is generated by Scrooge for a thrift service.
   * @param dest Address of the service to connect to.
   * @param label Assign a label for scoped stats.
   */
  def newServiceIface[ServiceIface <: ThriftServiceIface.Filterable[ServiceIface]](
    dest: Name,
    label: String
  )(
    implicit builder: ServiceIfaceBuilder[ServiceIface]
  ): ServiceIface = {
    val thriftService = newService(dest, label)
    newServiceIface(thriftService, label)
  }

  /**
   * $serviceIface
   *
   * @param service The Finagle [[Service]] to be used.
   * @param label Assign a label for scoped stats.
   * @param builder The builder type is generated by Scrooge for a thrift service.
   */
  def newServiceIface[ServiceIface <: ThriftServiceIface.Filterable[ServiceIface]](
    service: Service[ThriftClientRequest, Array[Byte]],
    label: String
  )(
    implicit builder: ServiceIfaceBuilder[ServiceIface]
  ): ServiceIface = {
    val statsLabel = if (label.isEmpty) defaultClientName else label
    val clientConfigScoped =
      clientParam.copy(clientStats = clientParam.clientStats.scope(statsLabel))
    builder.newServiceIface(service, clientConfigScoped)
  }

  /**
   * Converts from a Service interface (`ServiceIface`) to the
   * method interface (`newIface`).
   */
  def newMethodIface[ServiceIface, FutureIface](serviceIface: ServiceIface)(
    implicit builder: MethodIfaceBuilder[ServiceIface, FutureIface]
  ): FutureIface = builder.newMethodIface(serviceIface)

  /**
   * $buildMultiplexClient
   */
  def multiplex[T](dest: Name, label: String)(build: MultiplexedThriftClient => T): T = {
    build(new MultiplexedThriftClient(dest, label))
  }

  /**
   * $buildMultiplexClient
   */
  def multiplex[T](dest: String, label: String)(build: MultiplexedThriftClient => T): T = {
    multiplex(Resolver.eval(dest), label)(build)
  }

  class MultiplexedThriftClient(dest: Name, label: String) {

    private[this] val service = newService(dest, label)

    def newIface[Iface: ClassTag](serviceName: String): Iface = {
      val cls = implicitly[ClassTag[Iface]].runtimeClass
      newIface[Iface](serviceName, cls)
    }

    def newIface[Iface](serviceName: String, cls: Class[_]): Iface = {
      val multiplexedProtocol = Protocols.multiplex(serviceName, clientParam.protocolFactory)
      val clientConfigMultiplexed = clientParam.copy(protocolFactory = multiplexedProtocol)
      ThriftRichClient.this.newIface(dest, label, cls, clientConfigMultiplexed, service)
    }

    def newServiceIface[ServiceIface <: ThriftServiceIface.Filterable[ServiceIface]](
      serviceName: String
    )(
      implicit builder: ServiceIfaceBuilder[ServiceIface]
    ): ServiceIface = {
      val multiplexedProtocol = Protocols.multiplex(serviceName, clientParam.protocolFactory)
      val statsLabel = if (label.isEmpty) defaultClientName else label
      val clientConfigMultiplexedScoped = clientParam.copy(
        protocolFactory = multiplexedProtocol,
        clientStats = clientParam.clientStats.scope(statsLabel)
      )
      builder.newServiceIface(service, clientConfigMultiplexedScoped)
    }
  }
}

/**
 * Produce a client with params wrapped in RichClientParam
 *
 * @param protocolFactory A `TProtocolFactory` creates protocol objects from transports
 * @param serviceName For client stats, (default: empty string)
 * @param maxThriftBufferSize The max size of a reusable buffer for the thrift response
 * @param responseClassifier  See [[com.twitter.finagle.service.ResponseClassifier]]
 * @param clientStats StatsReceiver for recording metrics
 */
case class RichClientParam(
  protocolFactory: TProtocolFactory = Thrift.param.protocolFactory,
  serviceName: String = "",
  maxThriftBufferSize: Int = Thrift.param.maxThriftBufferSize,
  responseClassifier: ResponseClassifier = ResponseClassifier.Default,
  clientStats: StatsReceiver = ClientStatsReceiver
) {

  def this(
    protocolFactory: TProtocolFactory,
    maxThriftBufferSize: Int,
    responseClassifier: ResponseClassifier
  ) = this(protocolFactory, "", maxThriftBufferSize, responseClassifier, ClientStatsReceiver)

  def this(
    protocolFactory: TProtocolFactory,
    responseClassifier: ResponseClassifier
  ) = this(protocolFactory, Thrift.param.maxThriftBufferSize, responseClassifier)

  def this(
    protocolFactory: TProtocolFactory
  ) = this(protocolFactory, ResponseClassifier.Default)

  def this() = this(Thrift.param.protocolFactory)
}

/**
 * A mixin trait to provide a rich Thrift server API.
 *
 * @define serveIface
 *
 * Serve the interface implementation `iface`, which must be generated
 * by either [[https://github.com/twitter/scrooge Scrooge]] or
 * [[https://github.com/mariusaeriksen/thrift-0.5.0-finagle thrift-finagle]].
 *
 * Given the IDL:
 *
 * {{{
 * service TestService {
 *   string query(1: string x)
 * }
 * }}}
 *
 * Scrooge will generate an interface, `TestService.FutureIface`,
 * implementing the above IDL.
 *
 * $serverExample
 *
 * Note that this interface is discovered by reflection. Passing an
 * invalid interface implementation will result in a runtime error.
 *
 * @define serverExample
 *
 * `TestService.FutureIface` must be implemented and passed
 * into `serveIface`:
 *
 * {{{
 * $serverExampleObject.serveIface(":*", new TestService.FutureIface {
 *   def query(x: String) = Future.value(x)  // (echo service)
 * })
 * }}}
 *
 * @define serverExampleObject ThriftMuxRichServer
 *
 * @define serveIfaces
 *
 * Serve multiple interfaces:
 *
 * {{{
 * val serviceMap = Map(
 * "echo" -> new EchoService(),
 * "extendedEcho" -> new ExtendedEchoService()
 * )
 *
 * val server = Thrift.server.serveIfaces(address, serviceMap)
 * }}}
 *
 * A default service name can be specified, so we can upgrade an
 * existing non-multiplexed server to a multiplexed one without
 * breaking the old clients:
 *
 * {{{
 * val server = Thrift.server.serveIfaces(
 *   address, serviceMap, defaultService = Some("extendedEcho"))
 * }}}
 */
trait ThriftRichServer { self: Server[Array[Byte], Array[Byte]] =>
  import ThriftUtil._

  protected def serverParam: RichServerParam

  protected def protocolFactory: TProtocolFactory

  protected def maxThriftBufferSize: Int = Thrift.param.maxThriftBufferSize

  protected def serverLabel: String = "thrift"

  protected def params: Stack.Params

  protected def serverStats: StatsReceiver = params[Stats].statsReceiver

  /**
   * $serveIface
   */
  def serveIface(addr: String, iface: AnyRef): ListeningServer =
    serve(addr, serverFromIface(iface, serverParam))

  /**
   * $serveIface
   */
  def serveIface(addr: SocketAddress, iface: AnyRef): ListeningServer =
    serve(addr, serverFromIface(iface, serverParam))

  /**
   * $serveIfaces
   */
  def serveIfaces(
    addr: String,
    ifaces: Map[String, AnyRef],
    defaultService: Option[String] = None
  ): ListeningServer =
    serve(addr, serverFromIfaces(ifaces, defaultService, serverParam))

  /**
   * $serveIfaces
   */
  def serveIfaces(addr: SocketAddress, ifaces: Map[String, AnyRef]): ListeningServer =
    serve(addr, serverFromIfaces(ifaces, None, serverParam))

  /**
   * $serveIfaces
   */
  def serveIfaces(
    addr: SocketAddress,
    ifaces: Map[String, AnyRef],
    defaultService: Option[String]
  ): ListeningServer =
    serve(addr, serverFromIfaces(ifaces, defaultService, serverParam))
}

/**
 * Produce a server with params wrapped in RichServerParam
 *
 * @param protocolFactory A `TProtocolFactory` creates protocol objects from transports
 * @param serviceName For server stats, (default: "thrift")
 * @param maxThriftBufferSize The max size of a reusable buffer for the thrift response
 * @param serverStats StatsReceiver for recording metrics
 */
case class RichServerParam(
  protocolFactory: TProtocolFactory = Thrift.param.protocolFactory,
  serviceName: String = "thrift",
  maxThriftBufferSize: Int = Thrift.param.maxThriftBufferSize,
  serverStats: StatsReceiver = LoadedStatsReceiver
) {

  def this(
    protocolFactory: TProtocolFactory,
    maxThriftBufferSize: Int
  ) = this(protocolFactory, "thrift", maxThriftBufferSize, LoadedStatsReceiver)

  def this(
    protocolFactory: TProtocolFactory
  ) = this(protocolFactory, Thrift.param.maxThriftBufferSize)

  def this() = this(Thrift.param.protocolFactory)
}
