package amu.saeed.palang;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DataSet<T> implements Iterable<T> {
    private String filePath;
    private FileOutputStream fostream;
    private BufferedOutputStream bostream;
    private Output output = new Output(new ByteArrayOutputStream());
    private Kryo kryo = new Kryo();
    private Class<T> type;

    private DataSet() {
        try {
            filePath = File.createTempFile("palang", ".seq").getPath();
            fostream = new FileOutputStream(filePath);
            bostream = new BufferedOutputStream(fostream, 4 * 1024 * 1024);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SafeVarargs
    public static <T> DataSet<T> newDataSet(T... elements) {
        return newDataSet(elements);
    }

    public static <T> DataSet<T> newDataSet(Iterable<T> elements) {
        Preconditions.checkNotNull(elements);
        DataSet<T> ds = new DataSet<>();
        for (T element : elements)
            ds.append(element);
        ds.closeFile();
        return ds;
    }

    public static <T> DataSet<T> newDataSet(Iterator<T> iter) {
        Preconditions.checkNotNull(iter);
        DataSet<T> ds = new DataSet<>();
        while (iter.hasNext())
            ds.append(iter.next());
        ds.closeFile();
        return ds;
    }


    public <R> DataSet<R> map(Function<? super T, ? extends R> fun) {
        DataSet<R> newDS = new DataSet<>();
        for (T t : this)
            newDS.append(fun.apply(t));
        return newDS;
    }

    public DataSet<T> filter(final Predicate<? super T> predicate) {
        DataSet<T> newDS = new DataSet<>();
        for (T t : this)
            if (predicate.test(t))
                newDS.append(t);
        return newDS;
    }

    public DataSet<T> clone() {
        DataSet<T> newDS = new DataSet<>();
        for (T t : this)
            newDS.append(t);
        return newDS;
    }


    private void closeFile() {
        try {
            bostream.close();
            fostream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void append(T t) {
        if (type == null)
            type = (Class<T>) t.getClass();
        output.clear();
        kryo.writeObject(output, t);
        try {
            bostream.write(output.getBuffer(), 0, output.position());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        output.clear();
    }


    @NotNull
    @Override
    public Iterator<T> iterator() {
        try {
            final Input input = new Input(new FileInputStream(filePath));
            Iterator<T> iter = new Iterator<T>() {
                @Override
                public boolean hasNext() {
                    return !input.eof();
                }

                @Override
                public T next() {
                    return kryo.readObject(input, type);
                }
            };
            return iter;

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void forEach(Consumer<? super T> consumer) {

    }

    @Override
    public Spliterator<T> spliterator() {
        return null;
    }

    public static void main(String[] args) throws IOException {
        int COUNT = 40_000_000;
        List<Integer> INTS = new ArrayList<>(COUNT);
        for (int i = 0; i < COUNT; i++)
            INTS.add(i);

        Stopwatch stopwatch = Stopwatch.createStarted();
        DataSet<Integer> ints = DataSet.newDataSet(INTS);
        System.out.println("DONE: " + stopwatch);

        DataSet<String> v = ints.map(x -> Integer.toString(x));
        System.out.println("DONE: " + stopwatch);

        FileWriter writer = new FileWriter("/tmp/pla");
        for (String s : v)
            writer.write(s + "\n");
        writer.close();
        System.out.println("DONE: " + stopwatch);

        DataSet<String> w = v.filter(t -> t.contains("99"));
        System.out.println("DONE: " + stopwatch);

        DataSet<String> z = v.clone();
        System.out.println("DONE: " + stopwatch);

    }

}
