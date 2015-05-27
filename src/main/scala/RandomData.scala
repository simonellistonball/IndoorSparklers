import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.Random
import scopt.OptionParser

object RandomData {

  def generateData(numRecords:Int, partitions:Int, lookupSize:Int, moneyScale: Double, out: String) = {
    val conf = new SparkConf().setAppName("Random Data")
    val sc = new SparkContext(conf)

    val recordsPerPartition = numRecords / partitions
    def formatMoney(x: Double):String = f"${x}%10.2f"

    def makeRecord():String = (Seq.tabulate(4)(n => s"Attr${n}_${Random.nextInt(lookupSize)}") ++
      Seq.fill(16)(Random.nextInt(200)) ++
      Seq.fill(16)(formatMoney(Random.nextDouble * moneyScale))).map(_.toString).mkString(",")

    val seedRdd = sc.parallelize(Seq.fill(partitions)(recordsPerPartition),partitions)
    val randoms = seedRdd.flatMap(records => Seq.fill(records)(makeRecord()))
    randoms.saveAsTextFile(out)

  }

  def main(args: Array[String]) {
    case class Config(numRecords: Int = 1000000, partitions: Int = 100, lookupSize: Int = 1000,  moneyScale: Double = 1000000, out: String = "out")
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
      opt[String]('s', "moneyScale") valueName("<scale>") action { (x, c) =>
        c.copy(out = x) } text("Scaling factor fro random amount fields")
      note("we are assuming here that numRecords is divisible by partitions, otherwise we need to compensate for the residual")
    }

    // parser.parse returns Option[C]
    parser.parse(args.toSeq, Config()) match {
      case Some(config) =>
        generateData(config.numRecords, config.partitions, config.lookupSize, config.out)
      case None =>
        println("Invalid arguments error")
    }
  }
}
