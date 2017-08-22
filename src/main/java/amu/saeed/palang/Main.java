package amu.saeed.palang;

import amu.saeed.palang.types.KeyVal;
import amu.saeed.palang.types.PersistDataSet;
import com.google.common.base.Stopwatch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Created by saeed on 8/20/17.
 */
public class Main {
    public static void main(String[] args) throws IOException {
//        SparkConf conf = new SparkConf();
//        conf.setMaster("local[1]");
//        conf.setAppName("palang");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        Stopwatch stopwatch = Stopwatch.createStarted();
//        JavaRDD<String> x = sc.textFile("/mnt/b/data-omid/50m.csv", 8);
//        x.persist(StorageLevel.DISK_ONLY());
//        x.count();
//        JavaRDD<String> y = x.filter(t -> t.toLowerCase().contains("saeed"));
//        y.saveAsTextFile("/tmp/obj5");
//        System.out.println(stopwatch);

        Stopwatch stopwatch = Stopwatch.createStarted();
        Stream<String> lines = Files.lines(Paths.get("/mnt/b/data-omid/50m.csv"));
        PersistDataSet<KeyVal<String, Integer>> rds = PersistDataSet.newDataSet(lines.map(l -> new KeyVal(l, l.length())));
        System.out.println(rds.count());
        System.out.println(stopwatch);

    }
}
