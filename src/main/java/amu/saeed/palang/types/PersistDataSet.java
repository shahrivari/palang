package amu.saeed.palang.types;

import amu.saeed.palang.PalangRuntimeException;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by saeed on 8/20/17.
 */
public class PersistDataSet<T> extends DataSet<T> {
    private static boolean KEEP_TEMP_FILES = false;
    private String filePath;
    private FileOutputStream fostream;
    private BufferedOutputStream bostream;
    private Output output = new Output(new ByteArrayOutputStream());
    private Kryo kryo = new Kryo();
    private Class<T> type;
    private int size = 0;

    public PersistDataSet() {
        try {
            filePath = File.createTempFile("palang", ".dat").getPath();
            if (!KEEP_TEMP_FILES) {
                File file = new File(filePath);
                file.deleteOnExit();
            }
            fostream = new FileOutputStream(filePath);
            bostream = new BufferedOutputStream(fostream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SafeVarargs
    public static <T> PersistDataSet<T> newDataSet(T... elements) {
        return newDataSet(elements);
    }

    public static <T> PersistDataSet<T> newDataSet(Iterable<T> elements) {
        Preconditions.checkNotNull(elements);
        PersistDataSet<T> ds = new PersistDataSet<>();
        for (T element : elements)
            ds.append(element);
        ds.conclude();
        return ds;
    }

    public static <T> PersistDataSet<T> newDataSet(Iterator<T> iter) {
        Preconditions.checkNotNull(iter);
        PersistDataSet<T> ds = new PersistDataSet<>();
        while (iter.hasNext())
            ds.append(iter.next());
        ds.conclude();
        return ds;
    }

    @Override
    protected void conclude() {
        try {
            output.close();
            bostream.close();
            fostream.close();
        } catch (IOException e) {
            throw new PalangRuntimeException(e);
        }
    }

    @Override
    protected void append(T t) {
        size++;
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

    public long sizeOnDisk() { return new File(filePath).length();}

    @Override
    public int size() {
        return size;
    }

    @NotNull
    @Override
    public Iterator<T> iterator() {
        try {
            FileInputStream ifstream = new FileInputStream(filePath);
            final Input input = new Input(ifstream);
            Iterator<T> iter = new Iterator<T>() {
                @Override
                public boolean hasNext() {
                    boolean eof = input.eof();
                    if (eof)
                        try {
                            ifstream.close();
                        } catch (IOException e) {
                            throw new PalangRuntimeException(e);
                        }
                    return !eof;
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

    public static PersistDataSet readFromBinaryFile(String path) throws IOException {
        PersistDataSet persistDataSet = new PersistDataSet();
        FileInputStream fileInputStream = new FileInputStream(path);
        Input input = new Input(fileInputStream);
        Kryo kryo = new Kryo();

        if (!input.eof()) {
            Object obj = kryo.readClassAndObject(input);
            persistDataSet.append(obj);
            while (!input.eof())
                persistDataSet.append(kryo.readObject(input, obj.getClass()));
        }
        persistDataSet.conclude();
        return persistDataSet;
    }

    public static PersistDataSet<String> readFromTextFile(String path) throws IOException {
        return newDataSet(Files.lines(Paths.get(path)).iterator());
    }

}
