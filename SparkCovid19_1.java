/* Java imports */
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.lang.Iterable;
import java.util.Iterator;
import java.text.SimpleDateFormat;
import java.util.Date;
/* Spark imports */
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;

public class SparkCovid19_1 {
	/**
	 * args[0]: Input file path on distributed file system
	 * args[1]: Output file path on distributed file system
	 */
	public static void main(String[] args){

		final String input = args[0];
		final String output = args[3];

		try {
			final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			format.setLenient(false);
			Date data_start_date = format.parse("2019-12-31");
			Date data_end_date = format.parse("2020-04-08");
			final Date input_start_date = format.parse(args[1]);
			final Date input_end_date = format.parse(args[2]);

			if (input_start_date.after(input_end_date) ||
					(input_start_date.before(data_start_date)) ||
					(input_end_date.after(data_end_date))
			) {
				System.out.println("\nInvalid start and end dates, out of range\n");
				System.exit(1);
			}

			/* essential to run any spark code */
			SparkConf conf = new SparkConf().setAppName("SparkCovid19_1");
			JavaSparkContext sc = new JavaSparkContext(conf);

			/* load input data to RDD */
			JavaRDD<String> dataRDD = sc.textFile(args[0]);

			JavaPairRDD<String, Integer> counts =
					dataRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>(){
						public Iterator<Tuple2<String, Integer>> call(String value){
							String[] row = value.split(",");

							List<Tuple2<String, Integer>> retWords =
									new ArrayList<Tuple2<String, Integer>>();

							if (row == null || row[0] == null || row[3] == null || row[0].trim().toLowerCase().equals("date"))
								return retWords.iterator();

							Date data_date = null;

							try {
								data_date = format.parse(row[0].trim());
							} catch (Exception e) {
								return retWords.iterator();
							}

							if (data_date.before(input_start_date) || data_date.after(input_end_date)) return retWords.iterator();

							retWords.add(new Tuple2<String, Integer>(row[1].trim().toLowerCase(), Integer.parseInt(row[3])));

							return retWords.iterator(); }
					}).reduceByKey(new Function2<Integer, Integer, Integer>(){
						public Integer call(Integer x, Integer y){
							return x+y;
						}
					});

			counts.saveAsTextFile(output);
		} catch (Exception e) {
			System.out.println("\n\n\n\nInvalid start and end dates\n\n\n\n");
			System.exit(1);
		}
	}
}
