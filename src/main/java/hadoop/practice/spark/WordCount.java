package hadoop.practice.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Spark map reduce example, based off the example from
 * http://www.robertomarchetto.com/spark_java_maven_example
 * 
 * To run this example:
 *   - Create a jar with this class as the main class
 * 		 - right click project - > export -> runnable jar
 *       - choose the correct run configuration for this class
 *       - extract required libraries (this is what I chose, not sure if the other options work)
 *   - pscp the jar from your local machine to the hortonworks virtual machine.
 * 		 - pscp C:\Users\James\Desktop\i.jar root@192.168.81.128:/james/i.jar
 *   - create an example test file
 *       - su spark
 *       - cd /james
 *       - echo "This is test 1. This is test 2. This is test 3." > spark-in.txt
 *   - run some example commands against hdfs to get familiar with it
 *       - hadoop fs -ls /
 *       - hadoop fs -du /
 *   - put an example input text file in hdfs
 *       - hadoop fs -mkdir /user/spark/data
 *       - hadoop fs -put spark-in.txt /user/spark/data
 *   - run the jar using spark-submit
 *       - cd /usr/hdp/current/spark-client
 *       - ./bin/spark-submit --class hadoop.practice.spark.WordCount --master local[2] /james/i.jar /user/spark/data/spark-in.txt /user/spark/data/spark-out
 *   - find result file of your job
 *       - hadoop fs -ls /user/spark/data/spark-out/
 *       - hadoop fs -cat /user/spark/data/spark-out/part-00000
 */
public class WordCount {
	/**
	 * An implementation of FlatMapFunction<T,R> which splits the input String on a space character, and outputs a List of Strings.
	 * A FlatMapFunction is just a generic function which takes an Object of one type and returns an Iterable of another type of object.
	 */
	private static final FlatMapFunction<String, String> WORDS_EXTRACTOR = new FlatMapFunction<String, String>() {
		@Override
		public Iterable<String> call(String s) throws Exception {
			return Arrays.asList(s.split(" "));
		}
	};

	/**
	 * A function that returns a Tuple2<in, 1> from the string in...the 1 just means, 1 word.
	 */
	private static final PairFunction<String, String, Integer> WORDS_MAPPER = new PairFunction<String, String, Integer>() {
		@Override
		public Tuple2<String, Integer> call(String s) throws Exception {
			return new Tuple2<String, Integer>(s, 1);
		}
	};

	/**
	 * A function that returns the sum of two integers.
	 */
	private static final Function2<Integer, Integer, Integer> WORDS_REDUCER = new Function2<Integer, Integer, Integer>() {
		@Override
		public Integer call(Integer a, Integer b) throws Exception {
			return a + b;
		}
	};

	public static void main(String[] args) {
		String fullHdfsFilePathOfInputFile = args[0];
		String fullHdfsFilePathOfOutputFile = args[1];
		SparkConf conf = new SparkConf().setAppName("hadoop.practice.spark.WordCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		// JavaRDD is a distributed collection of objects.
		// The main abstraction Spark provides is a resilient distributed dataset (RDD), 
		// which is a collection of elements partitioned across the nodes of the cluster 
		// that can be operated on in parallel.
		
		// Read each line of the text file into a JavaRDD
		// Hello my name is james what is your name
		JavaRDD<String> file = context.textFile(fullHdfsFilePathOfInputFile);
		
		// Read the words of the file
		// Hello, my, name, is, james, what, is, your, name
		JavaRDD<String> words = file.flatMap(WORDS_EXTRACTOR);
		
		// A distributed pair
		// At this point, you could end up with something like this:
		// Hello, 1
		// my, 1
		// name, 1
		// is, 1
		// james, 1
		// what, 1
		// is, 1
		// your, 1
		// name, 1
		//  
		JavaPairRDD<String, Integer> pairsOfSingleWordToTheNumber1 = words.mapToPair(WORDS_MAPPER);
		
		// Condense the pairs, by summing all the values for common keys. The resulting JavaPairRDD is a 
		// distributed pair where the string is the word and the integer is the number of times it was found
		// Hello, 1
		// my, 1
		// name, 2
		// is, 2
		// james, 1
		// what, 1
		// your, 1
		JavaPairRDD<String, Integer> groupedByKeyHash = pairsOfSingleWordToTheNumber1.reduceByKey(WORDS_REDUCER);

		// Save the results as 
		groupedByKeyHash.saveAsTextFile(fullHdfsFilePathOfOutputFile);
		context.close();
	}
}