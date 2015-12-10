/**
  * Created by fabuzaid21 on 12/8/15.
  */
/* SimpleApp.scala */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable
import scala.collection.mutable.Map
import scala.collection.mutable.Set
import scala.util.Random

object DistributedGenericJoin {
  var relations: Array[Relation] = null
  var globalTotalOrder: Array[Int] = null

  /**
    * @param attributes The attributes (A_1, A_2,…) that belong
    *                   to this relation
    */
  case class Relation(val attributes: Array[Int]) {
    def sortAttributesOnOrder(order: Array[Int])  = {
      val temp = attributes.sortBy(x => order.indexOf(x))
      System.arraycopy(temp, 0, attributes, 0, temp.length)
    }

    override def toString: String = {
      s"attributes: ${attributes.mkString("(", ", ", ")")}"
    }
  }

  /**
    * @param values a 2-tuple: the first represents the
    *               attribute, and the second represents
    *               corresponding value
    */
  case class Tuple(val values: Array[(Int, Int)]) {

    // override hashCode and equals so they can be used
    // as keys in HashMap.
    // hashCode for an Array of Tuple2's is non-deterministic
    // so we can convert to a Set
    override def hashCode(): Int = values.toSet[(Int, Int)].hashCode

    override def equals(that: Any): Boolean = {
      if (that.isInstanceOf[Tuple]) {
        values.deep == that.asInstanceOf[Tuple].values.deep
      } else {
        false
      }
    }

    def project(attr: Int): Int = {
      var i = 0
      while (i < values.length) {
        if (values(i)._1 == attr) {
          return values(i)._2
        }
        i += 1
      }
      throw new IllegalArgumentException(s"Attribute ${attr} not found in this tuple!")
    }

    def isEmpty(): Boolean = values.isEmpty

    /**
      * Sort this Tuple's values by attribute, based on a given order.
      */
    private def sortValsOnOrder(order: Array[Int])  = {
      val temp = values.sortBy(x => order.indexOf(x._1))
      System.arraycopy(temp, 0, values, 0, temp.length)
      this
    }

    override def toString: String = {
      "Tuple" + values.map { case (attr, value) =>
        s"($attr, $value)"
      }.mkString("[", ",", "]")
    }

    def partialTuple(attrs: immutable.Set[Int]): Tuple = {
      val partialVals = values.filter { case(attr, value) => attrs.contains(attr) }
      Tuple(partialVals)
    }

    def apply(values: Array[(Int, Int)]) = new Tuple(values).sortValsOnOrder(globalTotalOrder)
  }

  /***
    * @param attribute  The attribute (A_1, A_2,…) we are operating on in this partition
    * @param attributeValues An array of columns: each represents a column
    *                     of values for the attribute we are indexing on.
    *                     If @attribute == A_1, then @attributeValues[0] will
    *                     be the column of A_1 values in @relations[0]. If
    *                     @relations[i] does not have A_j as part of its attribute
    *                     set, then @attributeValues[i].length == 0. Also, if A_j
    *                     is being indexed by another attribute (see @attributeIndexes)
    *                     then, @attributeValues[i].length == 0 will be true as well.
    * @param attributeIndexes An array of hash maps; like @attributeValues, the index
    *                         into this array also corresponds with the index into @relations.
    *                         The i'th Map[Tuple, Int] maps from a partial tuple in the relation i
    *                         to complementary A_j value for that particular tuple. For example,
    *                         if R_0(A_1, A_2) had the tuple (4, 7) and we were indexing from
    *                         A_2 to A_1, then attributeIndexes[0] would have {Tuple(A_2, 7) => 4}
    *                         in its map.
    */
  // Sample PartitionedAttribute for Triangle Query
  // totalOrder: B, A, C
  // R_0(A, B)   R_1(B, C)  R_2(A, C)
  // 0 | 1       0 | 1      0 | 1
  // 1 | 2       1 | 2      1 | 2
  // 2 | 0       2 | 0      2 | 0
  // PartitionedAttribute(A, Array(Set(), Set(), Set(0, 1, 2)),
  //                         Array(Map(Tuple(B, 1) -> Set(0), Tuple(B, 2) -> Set(1), Tuple(B, 0) -> Set(2)),
  //                               Map(), Map())
  // PartitionedAttribute(B, Array(Set(1, 2, 0), Set(0, 1, 2), Set()),
  //                         Array(Map(), Map(), Map()))
  // PartitionedAttribute(C, Array(Set(), Set(), Set()),
  //                         Array(Map(), Map(Tuple(B, 0) -> Set(1), Tuple(B, 1) -> Set(2), Tuple(B, 2) -> Set(0)),
  //                                      Map(Tuple(A, 0) -> Set(1), Tuple(A, 1) -> Set(2), Tuple(A, 2) -> Set(0)))

  case class PartitionedAttribute(attribute: Int, attributeValues: Array[Set[Int]],
                                  attributeIndexes: Array[Map[Tuple, Set[Int]]])

  def loadDataFile(sc: SparkContext, file: String): RDD[Array[Int]] = {
    sc.textFile(file).map(x => x.split(" ").map(x => x.toInt))
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Distributed Generic Join")
    val sc = new SparkContext(conf)
    val globalAttrSet = Set[Int]()
    var tuples: RDD[Array[Int]] = null
    args match {
      case Array(file, attrs) => {
        tuples = loadDataFile(sc, file)
        relations = attrs.split(":").map(x => Relation(x.split(",").map { x =>
          val attrVal = x.toInt
          globalAttrSet.add(attrVal)
          attrVal
        }
        ))
        globalTotalOrder = Random.shuffle(globalAttrSet.toSeq).toArray
      }
    }

    // NOTE: we assume that each relation has the same tuples and
    // only differs in terms of its attributes
    val attrTupleKV: RDD[(Int, (Int, Tuple))] = tuples.flatMap { arr: Array[Int] =>
      // for each relation in our relations
      relations.zipWithIndex.flatMap { case (relation, relIdx) =>
        // create a Tuple (NOTE: we assume that the relation's attrs
        // were initially sorted)
        val t: Tuple = Tuple(relation.attributes.sorted.zipWithIndex.map {
          case (attr, idx) => (attr, arr(idx))
        })
        // we create a key-value pair (attr, (relation Index, Tuple))
        // now, we can partition on the attribute
        relation.attributes.map { attr =>
          (attr, (relIdx, t))
        }
      }.toIterator
    }

    // group the (attr, attrVal) keys together on each node
    // This makes it easier to perform projections and intersections
    // for certain attributes
    val tuplesPartitionedByAttr = attrTupleKV.groupByKey()

    // sort each relation's attributes by the global total order, so we know how
    // to construct the indexes on each PartitionedAttribute
    relations.foreach(rel => rel.sortAttributesOnOrder(globalTotalOrder))

    // each (attr, attrVal), Iterator[(relIdx, Tuple)] now gets transformed into a
    // PartitionAttribute for that attribute
    val partitionedAttrs: RDD[PartitionedAttribute] = tuplesPartitionedByAttr.mapPartitions {
      iter: Iterator[(Int, Iterable[(Int, Tuple)])] =>
        iter.map { case (attr, relIdxTupleIterable) =>

          val attrVals: Array[Set[Int]] = Array.fill[Set[Int]](relations.length)(Set[Int]())
          val attrIndexes: Array[Map[Tuple, Set[Int]]] = Array.fill[Map[Tuple,
            Set[Int]]](relations.length)(Map[Tuple, Set[Int]]())


          relIdxTupleIterable.iterator.foreach { case(relIdx, tuple) =>
            val attrVal = tuple.project(attr)
            attrVals(relIdx).add(attrVal)
            val m: Map[Tuple, Set[Int]] = attrIndexes(relIdx)

            val attrsToKeep = relations(relIdx).attributes.takeWhile(x => x != attr)
            val projectedTuple = tuple.partialTuple(attrsToKeep.toSet)
            if (!projectedTuple.isEmpty) {
              val s: Set[Int] = m.getOrElseUpdate(projectedTuple, Set[Int]())
              s.add(attrVal)
            }
          }
          new PartitionedAttribute(attr, attrVals, attrIndexes)
        }
    }
    val firstAttr = globalTotalOrder(0)
    // first we compute the join for the very first attribute in our global order
    var result = partitionedAttrs.flatMap { partitionedAttribute =>
      // only look at partitionedAttribute that has the very first attribute
      if (partitionedAttribute.attribute == firstAttr) {
        val intersect = partitionedAttribute.attributeValues.filterNot(x => x.isEmpty).reduce((x, y) => x.intersect(y))
        .map { x =>
          val arr = Array((firstAttr, x))
          Tuple(arr)
        }
        intersect.toIterator
      } else {
        Iterator()
      }
    }

    var tuplesSoFar = sc.broadcast(result.collect)
    val attrsSeenSoFar = Set(firstAttr)

    // compute the join on the remaining attributes
    globalTotalOrder.drop(1).foreach { attr =>
      result = partitionedAttrs.flatMap { partitionedAttribute =>
        if (partitionedAttribute.attribute == attr) {
          var toReturn = Set[Tuple]()
          tuplesSoFar.value.foreach { t: Tuple =>
            toReturn = partitionedAttribute.attributeValues.indices.filterNot { relIdx =>
              partitionedAttribute.attributeValues(relIdx).isEmpty &&
                partitionedAttribute.attributeIndexes(relIdx).isEmpty
            }.map { relIdx =>
              val attrsInCommon = relations(relIdx).attributes.filter(attr => attrsSeenSoFar.contains(attr)).toSet
              if (attrsInCommon.isEmpty) {
                partitionedAttribute.attributeValues(relIdx)
              } else {
                val key = t.partialTuple(attrsInCommon)
                partitionedAttribute.attributeIndexes(relIdx).getOrElse(key, Set[Int]())
              }
            }.reduce((x, y) => x.intersect(y)).map { x =>
              val arr = t.values :+ (attr, x)
              Tuple(arr)
            }.union(toReturn)
          }
          toReturn.toIterator
        } else {
          Iterator()
        }
      }
      tuplesSoFar = sc.broadcast(result.collect)
      attrsSeenSoFar.add(attr)
    }
    println(s"Number of Triangles: ${tuplesSoFar.value.length}")
  }
}
