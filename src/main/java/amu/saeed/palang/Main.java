package amu.saeed.palang;

import amu.saeed.palang.types.PersistDataSet;
import com.google.common.base.Stopwatch;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;

/**
 * Created by saeed on 8/20/17.
 */
public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[1]");
        conf.setAppName("palang");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Stopwatch stopwatch = Stopwatch.createStarted();
        JavaRDD<String> x = sc.textFile("/mnt/b/data-omid/50m.csv", 8);
        x.persist(StorageLevel.DISK_ONLY());
        x.count();
        JavaRDD<String> y = x.filter(t -> t.toLowerCase().contains("saeed"));
        y.saveAsTextFile("/tmp/obj5");
        System.out.println(stopwatch);


//        val stopwatch = Stopwatch.createStarted()
////        RDD<String> rdd = sc.textFile("/tmp/pla", 1);
////        rdd.saveAsObjectFile("/tmp/obj");
////        System.out.println("DONE: " + stopwatch);
////
////        System.exit(0);
//
//
//        val strs = PersistDataSet.readFromTextFile("/mnt/b/data-omid/50m.csv");
//        val strs2 = strs.filter { it.toLowerCase().contains("saeed") }
//        strs2.saveAsTextFile("/tmp/saeed")
//        println("#: ${strs.size()}    SIZE:${strs.sizeOnDisk()}")
//        println(stopwatch)

    }
}
