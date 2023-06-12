import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.testing.memory","2718592000")
      .config("spark.local.dir","target/tmp")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
  }
}