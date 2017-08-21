package com.twitter.finagle.redis.integration

import com.twitter.conversions.time._
import com.twitter.finagle.redis.{ClusterClient, ClusterClientTest, ServerError}
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
final class ClusterClientCommandsIntegrationSuite extends ClusterClientTest {

  // Generate hash keys/slots for testing:
  // while true; do r=`openssl rand -hex 4`;
  // s=`echo "cluster keyslot $r" | redis-cli | cut -f2`; echo "$s,$r"; done

  val primaryCount: Int = 3

  private def reshardAssert(a: ClusterClient, b: ClusterClient, slotId: Int) = {
    val slotsA = Await.result(ownedSlots(a))
    val slotsB = Await.result(ownedSlots(b))

    Await.result(reshard(a, b, Seq(slotId)))

    // find the slot which will be removed and create new ranges
    val expectedSlotsA = slotsA.flatMap { s =>
      if(s.start <= slotId && slotId <= s.end) Seq((s.start, slotId-1), (slotId+1, s.end))
      else Seq((s.start, s.end))
    }.sorted
    
    val expectedSlotsB = slotsB
      .map(s => (s.start, s.end))
      .toList ++ List((slotId, slotId))
      .sorted
      .foldLeft[List[(Int, Int)]](List()) {
        // merge two ranges when end and start are adjacent
        case (Nil, (start, end)) => List((start, end))
        case (acc :+ ((s, e)), (start, end)) if e + 1 == start => acc :+ (s, end)
        case (acc, (start, end)) => acc :+ (start, end)
      }

    waitUntilAsserted(s"A is responsible for $expectedSlotsA and B for $expectedSlotsB") {
      println(expectedSlotsA)
      println(expectedSlotsB)
      assertSlots(a, expectedSlotsA)
      assertSlots(b, expectedSlotsB)
    }
  }

  // Resharding is depending on a correctly started/configured cluster
  test("Cluster is configured and started correctly", RedisTest, ClientTest) {
    startCluster()
  }

  test("Correctly set/get the value of a single key from different nodes", RedisTest, ClientTest) {
    withClusterClients(0, 1, 2) { case Seq(a, b, c) =>
      val key = Buf.Utf8("foo")
      val value = Buf.Utf8("bar")

      // test the server that is responsible
      Await.result(a.set(key, value))
      assert(Await.result(a.get(key)) == Some(value))

      // retrieve the value from another server (ASK redirect)
      assert(Await.result(b.get(key)) == Some(value))

      // set another key that also belongs to a (ASK redirect)
      val newKey = Buf.Utf8("fuzz")
      Await.result(c.set(newKey, value))
      assert(Await.result(a.get(newKey)) == Some(value))
    } 
  }

  test("Correctly set/get the value of a single key when resharding", RedisTest, ClientTest) {
    withClusterClients(0, 1, 2) { case Seq(a, b, c) =>
      val key = Buf.Utf8("6ff70029") // slot 42
      val value = Buf.Utf8("bar")

      // test the server that is responsible
      Await.result(a.set(key, value))
      assert(Await.result(a.getKeysInSlot(42)) == Seq(key))
      assert(Await.result(b.getKeysInSlot(42)) == Seq())

      reshardAssert(a, b, 42)

      assert(Await.result(b.getKeysInSlot(42)) == Seq(key))

      // retrieve the value from another server (ASK redirect)
      assert(Await.result(a.get(key)) == Some(value))
    } 
  }
}

