import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object NoInOutLink {
    def main(args: Array[String]) {
        val MASTER_ADDRESS = "ec2-35-160-94-160.us-west-2.compute.amazonaws.com"
        val SPARK_MASTER = "spark://" + MASTER_ADDRESS + ":7077"
        val HDFS_MASTER = "hdfs://" + MASTER_ADDRESS + ":9000"
        val input_dir = HDFS_MASTER + "/inputtxt"
        val links_file = input_dir + "/links-simple-sorted.txt"
        val titles_file = input_dir + "/titles-sorted.txt"
        val num_partitions = 10
        

        val conf = new SparkConf()
            .setAppName("NoInOutLink")
            .setMaster(SPARK_MASTER)
            

        val sc = new SparkContext(conf)

        val in_links = sc.textFile(links_file, num_partitions).map(in)
        val in_link_lists = in_links
        	.flatMap( line => line.split("\\s+") )
        	.filter( word => word != "" )
        val in_link_lists_long = in_link_lists.map(line => (line.toLong,"c"))

		val out_links = sc.textFile(links_file, num_partitions).map(out)
        val out_link_lists = out_links
        	.flatMap( line => line.split("\\s+") )
        	.filter( word => word != "" )
		val out_link_lists_long = out_link_lists.map(line => (line.toLong,1))
            .reduceByKey( (a, b) => (a + b) )

		val titles = sc.textFile(titles_file, num_partitions).zipWithIndex()
		val title_lists = titles
			.map(m => (m._2 + 1, m._1))
		
		val no_in_link = title_lists.subtractByKey(in_link_lists_long, num_partitions).sortByKey()
		val no_out_link = title_lists.subtractByKey(out_link_lists_long, num_partitions).sortByKey()
		println("[ NO OUTLINKS ]")

        no_out_link.takeOrdered(10)(Ordering[Long].on(x => x._1)).foreach(println)

		println("\n[ NO INLINKS ]")

        no_in_link.takeOrdered(10)(Ordering[Long].on(x => x._1)).foreach(println)
    
    }

    def in(line: String): String = {
		line.replaceAll("""([0-9]+)[\p{Punct}]""", "")
	}
	def out(line: String): String = {
		line.replaceAll("""[\p{Punct}].*""", "")

	}
}
