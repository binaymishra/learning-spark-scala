import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Application { 
	def main(args: Array[String]) {

		val dataFile = "C:\\big-data-file.txt"
		val conf = new SparkConf().setAppName("learningSparkScala").setMaster("local[4]")
		val sc = new SparkContext(conf)

		val textFile = sc.textFile(dataFile).cache()

		val words = textFile.flatMap(line => line.split("\\s+"))

		val lineCount = textFile.count

		val wordCount = words.count

		val wordStartWithA = words.map(w => w.toUpperCase).filter(w => w.startsWith("AN"))
		

		println("Number of line : " + lineCount)

		println("Number of words : " + wordCount)

		println("words start with A : " + wordStartWithA.count)

		//wordStartWithA.foreach(println)

		sc.stop()

	}
}