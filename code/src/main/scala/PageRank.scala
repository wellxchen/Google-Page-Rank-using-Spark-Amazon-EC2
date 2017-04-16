import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// Do NOT use different Spark libraries.

object PageRank {
    def main(args: Array[String]) {
        val MASTER_ADDRESS = "ec2-35-160-94-160.us-west-2.compute.amazonaws.com"
        val SPARK_MASTER = "spark://" + MASTER_ADDRESS + ":7077"
        val HDFS_MASTER = "hdfs://" + MASTER_ADDRESS + ":9000"
        val input_dir = HDFS_MASTER + "/inputtxt"
        val links_file = input_dir + "/links-simple-sorted.txt"
        val titles_file = input_dir + "/titles-sorted.txt"
        val num_partitions = 10
        val iters = 10

        val conf = new SparkConf()
            .setAppName("PageRank")
            .setMaster(SPARK_MASTER)

        val sc = new SparkContext(conf)


        //keep a map of [key, outlinks]

        val out_links = sc.textFile(links_file, num_partitions).map(punct_only)
        val out_links_list = out_links.map(line => (line.split("\\s+")(0), line.split("\\s+")))//
        val out_links_list_second = out_links_list
            .flatMap{ case (c, innerList) => innerList.map(_ -> c) }
            .filter(line => line._1 != line._2)
        val out_links_list_final = out_links_list_second.map(m => (m._2, m._1))
            .groupByKey(num_partitions)

        //keep a map of [key, page rank]

        val titles = sc.textFile(titles_file, num_partitions).zipWithIndex()
        val titles_count = titles.count().toDouble

        var ranks = titles.map(m => ((m._2 + 1).toString(), 100.0/titles_count))


        //in linlks

        val in_links = sc.textFile(links_file, num_partitions).map(punct_only)
        val in_links_list = in_links.map(line => (line.split("\\s+")(0), line.split("\\s+")))//
        val in_links_list_final = in_links_list
            .flatMap{ case (c, innerList) => innerList.map(_ -> c) }
            .filter(line => line._1 != line._2)
            .groupByKey(num_partitions)

        val no_in_links = ranks.subtractByKey(in_links_list_final)
        val no_in_links_second = no_in_links.map(m => (m._1, 15.0/titles_count.toDouble))

        
        /* PageRank */
       for (i <- 1 to iters) {


            var contribs = out_links_list_final.join(ranks).values.flatMap{ case (lists, rank) =>
                val size = lists.size
                lists.map(list => (list, rank / size))
            }

            ranks = contribs.reduceByKey(_ + _).mapValues(15.0/titles_count.toDouble + 0.85 * _)

            ranks = ranks.union(no_in_links_second)
           
        }


        val ranks_sum = ranks.values.sum

        ranks = ranks.map(m => (m._1, (m._2 * 100.0 / ranks_sum.toDouble)))

        //val titles_final = titles.map(m => ((m._2 + 1).toString(), m._1)
        //ranks = ranks.join(titles_final)
        
        println("[ PageRanks ]")

        ranks.takeOrdered(10)(Ordering[Double].reverse.on(x => x._2)).foreach(println)
        
    }

    def punct_only (line: String): String = {
        line.replaceAll("""[\p{Punct}]""", "")
    }

}
