import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.Random
import scopt.OptionParser

import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._
import org.apache.spark.sql.hive._

object RandomData {

  def generateData(numRecords:Int, partitions:Int, lookupSize:Int, moneyScale: Double, out: String, orcOut: String, tableName: String) = {
    val conf = new SparkConf().setAppName("Random Data")
    val sc = new SparkContext(conf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val recordsPerPartition = numRecords / partitions
    val key = "K"

    //def formatMoney(x: Double):String = f"${x}%10.2f"

    def makeRecord():Seq[Any] = Seq.tabulate(4)(n => s"Attr${n}_${Random.nextInt(lookupSize)}") ++
      Seq.fill(16)(Random.nextInt(200)) ++
      Seq.fill(16)(math.round(Random.nextDouble * moneyScale * 100)/100.0)

    // schema for ORC
    val schema  = StructType(Seq(StructField(key, IntegerType, true)) ++
      Seq.tabulate(4)(n => StructField(s"Attr${n}", StringType, true)) ++
      Seq.tabulate(16)(n => StructField(s"Count${n}", IntegerType, true)) ++
      Seq.tabulate(16)(n => StructField(s"Amount${n}", DoubleType, true)))

    // create an empty RDD to force partitions
    val seedRdd = sc.parallelize(Seq.fill(partitions)(recordsPerPartition),partitions)
    // in each partition fill in the records
    val randoms = seedRdd.flatMap(records => Seq.fill(records)(makeRecord()))

    // add on the incrementing identifier - better to do this with mapPartition and offset
    val numbers = sc.parallelize(1 to numRecords, partitions)
    val numbered = numbers.zip(randoms)

    // cache only if we're doing to save text as well
    //randoms.cache()


    val schemaRDD = hiveContext.createDataFrame(numbered.map(x=>Row.fromSeq(Seq(x._1) ++ x._2)),schema)
    schemaRDD.saveAsOrcFile(orcOut)

    if (tableName != "") {
      def mapSqlTypes(dataType: DataType):String = dataType match  {
        case IntegerType => "int"
        case DoubleType => "double"
        case StringType => "string"
        case _ => "string"
      }

      hiveContext.sql(s"DROP TABLE IF EXISTS ${tableName}")
      hiveContext.sql(s"CREATE EXTERNAL TABLE ${tableName} (" + schema.map(x=>s"${x.name} ${mapSqlTypes(x.dataType)}").mkString(",") + s") stored as orc location '/user/hsbc/${orcOut}'")
    }

    sc.stop()
    //schemaRDD.registerTempTable("test")
    // and save as text for the hell of it
    //schemaRDD.rdd.saveAsTextFile(out)
  }

  def main(args: Array[String]) {
    case class Config(numRecords: Int = 1000000, partitions: Int = 100, lookupSize: Int = 1000,  moneyScale: Double = 1000000, out: String = "out", orcOut: String = "orc", tableName: String = "")
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("Random Data", "1.0")
      opt[Int]('n', "numRecords") action { (x, c) =>
        c.copy(numRecords = x) } text("Number of records to generate")
      opt[Int]('p', "partitions") action { (x, c) =>
        c.copy(partitions = x) } text("Partitions to use (increase for parallelism and avoiding memory constraints)")
      opt[Int]('l', "lookupSize") action { (x, c) =>
        c.copy(lookupSize = x) } text("Limit on key length for lookups")
      opt[String]('o', "out") valueName("<path>") action { (x, c) =>
        c.copy(out = x) } text("Output path")
      opt[String]('r', "orcOut") valueName("<path>") action { (x, c) =>
        c.copy(orcOut = x) } text("Output path (orc format)")
      opt[String]('t', "tableName") valueName("<name>") action { (x, c) =>
        c.copy(tableName = x) } text("Hive Table Name to create")
      opt[String]('s', "moneyScale") valueName("<scale>") action { (x, c) =>
        c.copy(out = x) } text("Scaling factor fro random amount fields")
      note("we are assuming here that numRecords is divisible by partitions, otherwise we need to compensate for the residual")
    }

    // parser.parse returns Option[C]
    parser.parse(args.toSeq, Config()) match {
      case Some(config) =>
        generateData(config.numRecords, config.partitions, config.lookupSize, config.moneyScale, config.out, config.orcOut, config.tableName)
        System.exit(0)
      case None =>
        println("Invalid arguments error")
    }
  }
}
