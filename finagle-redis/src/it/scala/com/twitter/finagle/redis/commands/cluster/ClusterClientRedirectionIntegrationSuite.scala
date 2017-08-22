package com.twitter.finagle.redis.integration

import com.twitter.conversions.time._
import com.twitter.finagle.redis.{ClusterClient, ClusterClientTest, ServerError,
  ClusterAskError, ClusterMovedError, ClusterTooManyRedirectsError}
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis.util.{BufToString, RedisCluster}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util.{Await, Awaitable, Future, Time}
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class ClusterClientRedirectionIntegrationSuite extends ClusterClientTest {

  // Generate hash keys/slots for testing:
  // while true; do r=`openssl rand -hex 4`;
  // s=`echo "cluster keyslot $r" | redis-cli | cut -f2`; echo "$s,$r"; done

  val primaryCount: Int = 3

  // Resharding is depending on a correctly started/configured cluster
  test("Cluster is configured and started correctly", RedisTest, ClientTest) {
    startCluster()
  }

  test("Correctly throw a ClusterMovedError on access to non-owned slot", RedisTest, ClientTest) {
    withClusterClients(0, 1, 2) { case Seq(a, b, c) =>
      val key = Buf.Utf8("foo")
      val value = Buf.Utf8("bar")

      // test the that the responsible server is not throwing an error
      Await.result(a.set(key, value))
      assert(Await.result(a.get(key)) == Some(value))

      // retrieve the value from another server (MOVED redirect)
      intercept[ClusterMovedError](Await.result(b.get(key)))
    } 
  }

  test("Correctly throw a ClusterMovedError after resharding", RedisTest, ClientTest) {
    withClusterClients(0, 1, 2) { case Seq(a, b, c) =>
      val key = Buf.Utf8("6ff70029") // slot 42
      val value = Buf.Utf8("bar")

      // test the server that is responsible
      Await.result(a.set(key, value))
      assert(Await.result(a.getKeysInSlot(42)) == Seq(key))
      assert(Await.result(b.getKeysInSlot(42)) == Seq())

      assertReshard(a, b, 42)

      assert(Await.result(b.getKeysInSlot(42)) == Seq(key))

      // retrieve the value from another server (MOVED error)
      intercept[ClusterMovedError](Await.result(a.get(key)))
    }
  }

  test("Correctly raise ClusterAskError when a slot is MIGRATING", RedisTest, ClientTest) {
    withClusterClients(0, 2) { case Seq(a, c) =>
      val slotId = 7
      val key = Buf.Utf8("2fed0338") // slot 7
      val value = Buf.Utf8("foo")

      // start migrating slot 7 to another node (C)
      val aId = Await.result(a.nodeId)
      val cId = Await.result(c.nodeId)
      assert(aId.nonEmpty)
      assert(cId.nonEmpty)

      Await.result(c.setSlotImporting(slotId, aId.get))
      Await.result(a.setSlotMigrating(slotId, cId.get))

      // make sure that C returns moved since its not responsible
      intercept[ClusterMovedError](Await.result(c.get(key)))

      // retrieve a key at A that doesnt exist to trigger ask redirection
      intercept[ClusterAskError](Await.result(a.get(key)))

      // send asking to C before the get to see if it has the key
      val f = for { _ <- c.asking(); resp <- c.get(key) } yield resp
      assert(Await.result(f) == None)
    }
  }

  test("Redirecting client with 0 maxRedirects does not redirect", RedisTest, ClientTest) {
    withClusterRedirectingClients(maxRedirects = 0)(0, 1) { case Seq(a, b) =>
      val key = Buf.Utf8("baz")

      // test the that the responsible server is not throwing an error
      assert(Await.result(a.get(key)) == None)

      intercept[ClusterTooManyRedirectsError](Await.result(b.get(key)))
    }
  }

  test("Redirecting client follows a MOVED redirection", RedisTest, ClientTest) {
    withClusterRedirectingClients(maxRedirects = 5)(0, 1, 2) { case Seq(a, b, c) =>
      val key = Buf.Utf8("foo")
      val value = Buf.Utf8("bar")

      // test the that the responsible server is not throwing an error
      Await.result(a.set(key, value))
      assert(Await.result(a.get(key)) == Some(value))

      // retrieve the value from another server (MOVED redirect)
      assert(Await.result(b.get(key)) == Some(value))
    }
  }

  test("Correctly follow MOVED redirection after resharding", RedisTest, ClientTest) {
    withClusterRedirectingClients()(0, 1, 2) { case Seq(a, b, c) =>
      val slotId = 5
      val key = Buf.Utf8("7c3942ce") // slot 5
      val value = Buf.Utf8("bar")

      // test the server that is responsible
      Await.result(a.set(key, value))
      assert(Await.result(a.getKeysInSlot(slotId)) == Seq(key))
      assert(Await.result(c.getKeysInSlot(slotId)) == Seq())

      assertReshard(a, c, slotId)

      assert(Await.result(c.getKeysInSlot(slotId)) == Seq(key))

      // retrieve the value from another server (MOVED redirect)
      assert(Await.result(b.get(key)) == Some(value))
    } 
  }

  test("Correctly follow ASK redirection when a slot is MIGRATING", RedisTest, ClientTest) {
    withClusterRedirectingClients()(0, 2) { case Seq(a, c) =>
      val slotId = 3
      val key = Buf.Utf8("f80d8469") // slot 7
      val value = Buf.Utf8("foo")

      // start migrating slot 3 to another node (C)
      val aId = Await.result(a.nodeId)
      val cId = Await.result(c.nodeId)
      assert(aId.nonEmpty)
      assert(cId.nonEmpty)

      Await.result(c.setSlotImporting(slotId, aId.get))
      Await.result(a.setSlotMigrating(slotId, cId.get))

      // retrieve a key at A that doesnt exist to trigger
      // ask redirection
      assert(Await.result(a.get(key)) == None)
    }
  }

}

