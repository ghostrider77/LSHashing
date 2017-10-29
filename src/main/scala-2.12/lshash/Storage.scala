package lshash

sealed trait StorageType
case object AssociativeMap extends StorageType

case class StorageConfig(storageType: StorageType, details: Option[Map[String, String]])

object Storage {

  def storage(storageConfig: StorageConfig, index: Int): BaseStorage = storageConfig match {
    case StorageConfig(AssociativeMap, _) => new InMemoryStorage(storageConfig)
  }

}

sealed abstract class BaseStorage() {
  def addValue(key: String, value: HashValue): Unit

  def getValues(key: String): List[HashValue]
}

class InMemoryStorage(config: StorageConfig) extends BaseStorage {
  import scala.collection.mutable.{Map => MutableMap}

  private val storage: MutableMap[String, List[HashValue]] = MutableMap()

  def addValue(key: String, value: HashValue): Unit = storage.update(key, value :: storage.getOrElse(key, Nil))

  def getValues(key: String): List[HashValue] = storage.getOrElse(key, Nil)
}

