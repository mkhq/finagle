package com.twitter.finagle.loadbalancer.aperture

import com.twitter.util.{Event, Witness}
import java.util.concurrent.atomic.AtomicReference

/**
 * [[ProcessCoordinate]] exposes a mechanism that allows a process to be
 * furnished with a coordinate between [0.0, 1.0). The coordinate is calculated relative
 * to other nodes in the process' peer group such that each coordinate is uniformly spaced
 * from another. The result is that each process that is part of the respective group
 * is now part of a topology with a sense of order and proximity.
 */
object ProcessCoordinate {

  /**
   * An ADT which represents the process coordinate.
   */
  sealed trait Coord {
    /**
     * Returns a position between the range [0, 1.0) which represents this
     * process' offset on the shared coordinate space.
     */
    def offset: Double

    /**
     * Computes the width of this processes range given the number of `units`
     * to span. That is, [offset, offset + width(x)) represents the range of this
     * process.
     */
    def width(units: Int): Double
  }

  /**
   * Defines a coordinate from a process' instance id.
   *
   * The coordinate is calculated between the range [0, 1.0) and is primarily
   * a function of `instanceId` and `totalInstances`. The latter dictates the size of
   * each uniform slice in the coordinate space and the former dictates this process'
   * location on the coordinate space. An additional parameter, `offset`, is exposed
   * that allows clients of the same peer group to coordinate a rotation or offset of
   * the coordinate space in order to avoid alignment with other client groups of
   * the same size (i.e. to add some entropy to the system).
   *
   * @param peerOffset A parameter which allows clients of the same peer group to
   * apply a rotation or offset of the coordinate space in order to avoid alignment
   * with other client groups of the same size. A good value for this is the hashCode of
   * a shared notion of process identifier (e.g. "/s/my-group-of-process".hashCode).
   *
   * @param instanceId An instance identifier for this process w.r.t its peer
   * cluster.
   *
   * @param totalInstances The total number of instances in this process' peer
   * cluster.
   */
  private[aperture] case class FromInstanceId(peerOffset: Int, instanceId: Int, totalInstances: Int) extends Coord {
    require(totalInstances > 0, s"totalInstances expected to be > 0 but was $totalInstances")

    private[this] val unitWidth: Double = 1.0 / totalInstances

    val offset: Double = {
      // compute the offset for this process between [0.0, 1.0).
      val normalizedOffset: Double = peerOffset / Int.MaxValue.toDouble
      val coord = (instanceId * unitWidth + normalizedOffset) % 1.0
      if (coord < 0) coord + 1.0 else coord
    }

    def width(units: Int): Double = {
      if (units > totalInstances)
        throw new IllegalArgumentException(s"units must be <= $totalInstances: $units")
      // If each peer gets a slice of size `unitWidth`, then 2 slices
      // is 2 * unitWidth and so on.
      (unitWidth * units) % 1.0
    }
  }

  /**
   * An [[Event]] which tracks the current process coordinate.
   */
  private[this] val coordinate: Event[Option[Coord]] with Witness[Option[Coord]] = Event()
  private[this] val ref: AtomicReference[Option[Coord]] = new AtomicReference(None)
  coordinate.register(Witness(ref))

  /**
   * An [[Event]] which triggers every time the process coordinate changes. This exposes
   * a push based API for the coordinate.
   */
  private[aperture] val changes: Event[Option[Coord]] = coordinate.dedup

  /**
   * Returns the current coordinate, if there is one set.
   */
  def apply(): Option[Coord] = ref.get

  /**
   * Globally set the coordinate for this process from the respective instance
   * metadata.
   *
   * @see [[FromInstanceId]] for more details.
   */
  def setCoordinate(offset: Int, instanceId: Int, totalInstances: Int): Unit = {
    coordinate.notify(Some(FromInstanceId(offset, instanceId, totalInstances)))
  }

  /**
   * Disables the ordering for this process and forces each Finagle client that
   * uses [[Aperture]] to derive a random ordering.
   */
  def unsetCoordinate(): Unit = {
    coordinate.notify(None)
  }
}
