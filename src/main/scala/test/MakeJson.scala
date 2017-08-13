package test
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import com.databricks.spark.csv
import com.mongodb.spark._
import java.math.BigInteger
import com.mongodb.spark._
import com.mongodb.spark.config._
import com.mongodb.spark.rdd.MongoRDD
import org.bson.Document
object MakeJson {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val emp = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("G:\\Edx\\emp.csv")

    val dep = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("G:\\Edx\\dep.csv")

    val out = dep.toJSON.map(k => (Document.parse(k).getDouble("eid").toString() + "~" + Document.parse(k).getString("comment"), Document.parse(k)))
    // println(out.first)

    val s = out.reduceByKey((x, y) => Document.parse(x.toJson() + ", " + y.toJson()))

    val ss = s.map(k => (k._1.split("~")(0), k._1.split("~")(1) + ":" + k._2.toJson))

    val sss = ss.reduceByKey(_ + "," + _).map(s => "{" + "_id:" + s._1 + "," + s._2 + "}")

    //sss.saveAsTextFile("resources/test")
    val read = sc.textFile("resources/test", 2).map(k => (Document.parse(k).getDouble("_id"), Document.parse(k)))

    val doc = sss.map(d => (Document.parse(d).getDouble("_id"), Document.parse(d)))

    val z = doc.leftOuterJoin(read)

    val zz = z.filter(k => k._2._1 != k._2._2.getOrElse("new")).map(s => s._2._1.toJson())

    zz.saveAsTextFile("resources/diff1")
    val call_writeConf = getWriteConf("test", "call", conf)

    // MongoSpark.save(doc, call_writeConf)

    // val d = new Document("bigInt", new BigInteger("10112345685623456487546454051011234568562345648754")).append("str", "This is a regular string")
    //  println(d)
  }

  def getWriteConf(dbase: String, collection: String, conf1: SparkConf) = {

    WriteConfig.create(dbase, collection, "mongodb://127.0.0.1/", 10, WriteConcernConfig.create(conf1).writeConcern)
  }
}