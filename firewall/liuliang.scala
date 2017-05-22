import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd
import org.apache.spark.rdd._

object liuliang {

    // The Main Entery.
    def main(args: Array[String]): Unit = {

        val r1 = """((\d{1,3}\.){3}\d{1,3}\.?[\w\-]{0,}(\s>\s)?){2}""".r 
        val r2 = """(\d{1,3}\.){3}\d{1,3}\.?[\w\-]{0,}(\s>\s)""".r       
        val r3 = """(\s>\s)(\d{1,3}\.){3}\d{1,3}\.?[\w\-]{0,}""".r       
        val r4 = """(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.?[\w\-]{0,}""".r

        val spark = new SparkContext("spark://10.170.31.120:7077", "liuliang")
        val fs = spark.textFile("hdfs://10.170.31.120:9000/user/songling/item/liuliang.txt")
        val meth1 = default(fs)(_,_)
        val meth2 = direct(fs)(_)
        val meth3 = firewall(fs)(_,_,_)
        args(0) match {
            case "-o"  => meth1(r3, "out")        // Find Only Receiver
            case "-i"  => meth1(r2, "in")         // Find Only Sender
            case "-d"  => meth2(r1)               // Find the directioni
            case "-fo" => meth3(r3,r4, "fireout")
            case "-fi" => meth3(r2,r4, "firein")
            case _     => meth1(r1, "all")        // Find all IP and counting
            }
        spark.stop()
    }

    // Default Method.
    def default(fs: RDD[String])(r: scala.util.matching.Regex, name: String): Unit = {
        fs.flatMap(line => r.findAllIn(line)).flatMap(line => line.split(" > ")).map(word =>
            (word, 1)).reduceByKey(_+_).sortBy(x => x._2, false).saveAsTextFile("hdfs://10.170.31.120:9000/user/songling/item/ans-" + name)
    }

    // Directed Method.
    def direct(fs: RDD[String])(r: scala.util.matching.Regex): Unit = {
        fs.flatMap(line => r.findAllIn(line)).map(word => 
        (word, 1)).reduceByKey(_+_).sortBy(x => x._2, false).saveAsTextFile("hdfs://10.170.31.120:9000/user/songling/item/ans-default")
    }

    def firewall(fs: RDD[String])(r1: scala.util.matching.Regex, r2: scala.util.matching.Regex, name: String): Unit = {
        val predata = fs.flatMap(line => r1.findAllIn(line)).flatMap(line => line.split(" > ")).map(word =>
            (word, 1)).reduceByKey(_+_)
        val after_wall = for (word <- predata) yield {
            word match{
              case word if word._2>200 => {
                  val middle :String = r2.replaceAllIn(word._1,m => m.group(1)+".***.***.***")
                  (middle,word._2)}
              case word if word._2>50  => {
                  val middle :String = r2.replaceAllIn(word._1,m => m.group(1)+"."+m.group(2)+".***.***")
                  (middle,word._2)}
              case word if word._2>10  => {
                  val middle :String = r2.replaceAllIn(word._1,m => m.group(1)+"."+m.group(2)+"."+m.group(3)+".***")
                  (middle,word._2)}
              case _ =>  word
            }
        }
        after_wall.reduceByKey(_+_).sortBy(x => x._2, false).saveAsTextFile("hdfs://10.170.31.120:9000/user/songling/item/ans-" + name)
    }
} 
