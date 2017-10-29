package lshash

import scala.collection.mutable.{Set => MutableSet}
import breeze.linalg.{DenseMatrix, DenseVector, sum}
import breeze.numerics.{pow, abs}
import breeze.stats.distributions.Gaussian

sealed trait DistanceFunction
case object Euclidean extends DistanceFunction
case object Cosine extends DistanceFunction
case object Manhattan extends DistanceFunction

case class HashValue(inputPoint: DenseVector[Double], extraData: Option[Int])
case class QueryResult(neighbour: HashValue, distance: Double)

class LSHash(hashSize: Int,
             inputDim: Int,
             numHashTables: Int = 1,
             storageConfig: StorageConfig = StorageConfig(AssociativeMap, None)) {
  import Storage.storage

  private val uniformPlanes: List[DenseMatrix[Double]] = initializeUniformPlanes()
  private val hashTables: List[BaseStorage] = initializeHashTables()

  private def initializeUniformPlanes(): List[DenseMatrix[Double]] =
    (for { _ <- 0 until numHashTables } yield generateUniformPlanes()).toList

  private def generateUniformPlanes(): DenseMatrix[Double] =
    DenseMatrix.rand(hashSize, inputDim, Gaussian(mu = 0.0, sigma = 1.0))

  private def initializeHashTables(): List[BaseStorage] =
    (for { ix <- 0 until numHashTables } yield storage(storageConfig, ix)).toList

  private def hash(planes: DenseMatrix[Double], inputPoint: DenseVector[Double]): String = {
    val projections: DenseVector[Double] = planes * inputPoint
    projections.toScalaVector.map(x => if (x > 0) "1" else "0").mkString("")
  }

  def index(inputPoint: DenseVector[Double], extraData: Option[Int]): Unit = {
    for {
      (table, plane) <- (hashTables, uniformPlanes).zipped
      key: String = hash(plane, inputPoint)
    } table.addValue(key, HashValue(inputPoint, extraData))
  }

  def query(queryPoint: DenseVector[Double],
            numResults: Option[Int] = None,
            distanceFunc: DistanceFunction = Euclidean): List[QueryResult] = {

    val dFunc: (DenseVector[Double], DenseVector[Double]) => Double = distanceFunc match {
      case Euclidean => LSHash.euclideanDist
      case Cosine => LSHash.cosineDist
      case Manhattan => LSHash.manhattanDist
    }

    val candidates: MutableSet[HashValue] = MutableSet()
    for {
      (table, planes) <- (hashTables, uniformPlanes).zipped
      binaryHash: String = hash(planes, queryPoint)
    } candidates ++= table.getValues(binaryHash)

    val distancesFromCandidates: List[QueryResult] =
      candidates.map{ hashvalue => QueryResult(hashvalue, dFunc(queryPoint, hashvalue.inputPoint)) }.toList
    val sortedCandidates: List[QueryResult] = distancesFromCandidates.sortBy(_.distance)

    numResults match {
      case Some(k) => sortedCandidates.take(k)
      case None => sortedCandidates
    }
  }

}

object LSHash {

  private def euclideanDist(x: DenseVector[Double], y: DenseVector[Double]): Double = math.sqrt(sum(pow(x - y, 2.0)))

  private def cosineDist(x: DenseVector[Double], y: DenseVector[Double]): Double =
    1 - (x.dot(y) / math.sqrt(x.dot(x) * y.dot(y)))

  private def manhattanDist(x: DenseVector[Double], y: DenseVector[Double]): Double = sum(abs(x - y))

}
