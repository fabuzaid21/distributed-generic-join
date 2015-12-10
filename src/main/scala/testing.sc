
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable
import scala.collection.mutable.{Map, Set}
import scala.util.Random

case class Relation(val attributes: Array[Int]) {
  def sortAttributesOnOrder(order: Array[Int])  = {
    val temp = attributes.sortBy(x => order.indexOf(x))
    System.arraycopy(temp, 0, attributes, 0, temp.length)
  }

  override def toString: String = {
    s"attributes: ${attributes.mkString("(", ", ", ")")}"
  }
}

def loadDataFile(sc: SparkContext, file: String): RDD[Array[Int]] = {
  sc.textFile(file).map(x => x.split(" ").map(x => x.toInt))
}

val globalAttrSet = Set[Int]()
val tuples = loadDataFile(sc, "single_triangle.txt")
val relations = "1,2:2,3:1,3".split(":").map(x => Relation(x.split(",").map { x =>
  val attrVal = x.toInt
  globalAttrSet.add(attrVal)
  attrVal
}
))

val globalTotalOrder = Array(2, 1, 3)

case class Tuple(val values: Array[(Int, Int)]) {

  def isEmpty(): Boolean = values.isEmpty

  // override hashCode and equals so they can be used
  // as keys in HashMap
  override def hashCode(): Int = {
    values.toSet[(Int, Int)].hashCode
  }
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

  override def toString: String = {
    "Tuple" + values.map { case (attr, value) =>
      s"($attr, $value)"
    }.mkString("[", ",", "]")
  }

  def apply(values: Array[(Int, Int)]) = new Tuple(values.sorted)

  def partialTuple(attrs: immutable.Set[Int]): Tuple = {
    val partialVals = values.filter { case(attr, value) => attrs.contains(attr) }
    Tuple(partialVals.sorted)
  }
}

// NOTE: we assume that each relation has the same tuples and
// only differs in terms of its attributes
//
val attrTupleKV: RDD[(Int, (Int, Tuple))] = tuples.flatMap { arr: Array[Int] =>
  // for each relation in our relations
  relations.zipWithIndex.flatMap { case (relation, relIdx) =>
    // we create a Tuple
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

case class PartitionedAttribute(attribute: Int, attributeValues: Array[Set[Int]],
                                attributeIndexes: Array[Map[Tuple, Set[Int]]])

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
  println("RESULT SO FAR")
  result.collect.foreach(x => println(x))
  tuplesSoFar = sc.broadcast(result.collect)
  attrsSeenSoFar.add(attr)
}
result.foreach(x => println(x))

var attr = globalTotalOrder(1)
val foo = partitionedAttrs.flatMap { partitionedAttribute =>
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


val partitionedAttribute = partitionedAttrs.first
val conf = new SparkConf().setAppName("Distributed Generic Join")
val sc = new SparkContext(conf)
val _globalTotalOrder = Random.shuffle(globalAttrSet.toSeq).toArray
partitionedAttrs.foreach { x =>
    if (x.attribute == 2) {
      x.attributeValues.foreach(y => println(y))
//      println(x.attributeValues.filterNot(x => x.isEmpty).reduce((x, y) => x.intersect(y)))
    }
  }
