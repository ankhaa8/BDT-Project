package cs523.BDT.Spark;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;

public class SKConsumer {

	public static void main(String[] args) throws IOException {

		System.out.println("Spark Streaming started now .....");
		System.setProperty("hive.metastore.uris", "thrift://localhost:9083");
		SparkConf conf = new SparkConf().setAppName("kafka-spark-streaming")
				.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(
				20000));

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		Set<String> topics = Collections.singleton("nom");

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils
				.createDirectStream(ssc, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						topics);

		List<String> allRecord = new ArrayList<String>();
		final String COMMA = ",";

		directKafkaStream
				.foreachRDD(rdd -> {

					System.out.println("New data arrived  "
							+ rdd.partitions().size() + " Partitions and "
							+ rdd.count() + " Records");
					if (rdd.count() > 0) {
						rdd.collect()
								.forEach(
										rawRecord -> {

											System.out.println(rawRecord);
											System.out
													.println("***************************************");
											System.out.println(rawRecord._2);
											String record = rawRecord._2();
											StringTokenizer st = new StringTokenizer(
													record, ",");

											StringBuilder sb = new StringBuilder();
											while (st.hasMoreTokens()) {
												String bookID = st.nextToken();
												String title = st.nextToken();
												String authors = st.nextToken();
												String average_rating = st
														.nextToken();
												String isbn = st.nextToken();
												String isbn13 = st.nextToken();
												String language_code = st
														.nextToken();
												String num_pages = st
														.nextToken();
												String ratings_count = st
														.nextToken();
												String text_reviews_count = st
														.nextToken();
												String publication_date = st
														.nextToken();
												String publisher = st
														.nextToken();

												sb.append(bookID)
														.append(",")
														.append(title)
														.append(",")
														.append(authors)
														.append(",")
														.append(average_rating)
														.append(",")
														.append(isbn)
														.append(",")
														.append(isbn13)
														.append(",")
														.append(language_code)
														.append(",")
														.append(num_pages)
														.append(",")
														.append(ratings_count)
														.append(",")
														.append(text_reviews_count)
														.append(",")
														.append(publication_date)
														.append(",")
														.append(publisher);
												allRecord.add(sb.toString());
											}

										});
						System.out.println("All records OUTER MOST :"
								+ allRecord.size());
						FileWriter writer = new FileWriter("output.csv");
						for (String s : allRecord) {
							writer.write(s);
							writer.write("\n");
						}
						System.out.println("Output has been created : ");
						
						HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(sc);        
				        hiveContext.sql("LOAD DATA LOCAL INPATH '/home/cloudera/cs523/BDTproject/Spark/output.csv' INTO TABLE books");
				        Row[] results = hiveContext.sql("SELECT * FROM books").collect();
				        System.out.println(results.length);
						System.out.println("Data has been saved successfully");
						
						writer.close();
					}
				});

		
		ssc.start();
		ssc.awaitTermination();
	}

}