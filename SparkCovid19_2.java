/* Java imports */
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.lang.Iterable;
import java.util.Iterator;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
/* Spark imports */
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;

public class SparkCovid19_2 {
	/**
	 * args[0]: Input file path on distributed file system
	 * args[1]: Output file path on distributed file system
	 */
	public static void main(String[] args){

		final String input = args[0];
		final String output = args[2];

		/* essential to run any spark code */
		SparkConf conf = new SparkConf().setAppName("SparkCovid19_2");
		JavaSparkContext sc = new JavaSparkContext(conf);

		final HashMap<String, Double> population = new HashMap<String, Double>();

		JavaRDD<String> rdd = sc.textFile(args[1]);

		List<String> Lines = rdd.collect();

		for (String line : Lines) {
			String []row = line.split(",");
			if (row == null || row[0] == null || row[1].trim().toLowerCase().equals("location")) continue; //location or null values
			try{
				double cnt = Double.parseDouble(row[4]);
				if (row[0] == null || cnt == 0 || population.containsKey(row[0].trim().toLowerCase())) continue;
				population.put(row[0].trim().toLowerCase(), cnt);
				if (row[1] == null || cnt == 0 || population.containsKey(row[1].trim().toLowerCase())) continue;
				population.put(row[1].trim().toLowerCase(), cnt);
			} catch(Exception e) {
				System.err.println("Adding population to map: " + e.getMessage());
			}
		}

		final Broadcast<HashMap<String,Double>> population_BV = sc.broadcast(population);
		JavaRDD<String> dataRDD = sc.textFile(args[0]);

		JavaPairRDD<String, Double> counts =
				dataRDD.flatMapToPair(new PairFlatMapFunction<String, String, Double>(){
					public Iterator<Tuple2<String, Double>> call(String value){
						String[] row = value.split(",");

						List<Tuple2<String, Double>> retWords =
								new ArrayList<Tuple2<String, Double>>();

						if (row[1] == null || row[2] == null || !population_BV.value().containsKey(row[1].trim().toLowerCase())) return retWords.iterator();

						double sum = Double.parseDouble(row[2]);
						sum = sum * 1000000L;
						sum /= population_BV.value().get(row[1].trim().toLowerCase());
						retWords.add(new Tuple2<String, Double>(row[1].trim().toLowerCase(), sum));

						return retWords.iterator();
					}
				}).reduceByKey(new Function2<Double, Double, Double>(){
					public Double call(Double x, Double y){
						return x+y;
					}
				});

		counts.saveAsTextFile(output);
	}

}