package amu.saeed.palang.types;

import amu.saeed.palang.PalangRuntimeException;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by saeed on 8/20/17.
 */
public class PersistDataSet<T> extends DataSet<T> {
    private final List<FileInputStream> usedStreams = new ArrayList<>();
    private FileOutputStream fostream;
    private File backFile;
    private BufferedOutputStream bostream;
    private Output output = new Output(new ByteArrayOutputStream());
    private Kryo kryo = new Kryo();
    private Class<T> type;
    private LZ4BlockOutputStream compstream;
    private boolean isFirstRecord = true;

    public PersistDataSet() {
        try {
            backFile = File.createTempFile("palang", ".dat");
            backFile.deleteOnExit();
            fostream = new FileOutputStream(backFile);
            compstream = new LZ4BlockOutputStream(fostream);
            bostream = new BufferedOutputStream(compstream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> PersistDataSet<T> newDataSet(Iterator<T> iter) {
        Preconditions.checkNotNull(iter);
        PersistDataSet<T> ds = new PersistDataSet<>();
        while (iter.hasNext())
            ds.append(iter.next());
        ds.conclude();
        return ds;
    }

    public static <T> PersistDataSet<T> newDataSet(Iterable<T> elements) {
        return newDataSet(elements.iterator());
    }

    public static <T> PersistDataSet<T> newDataSet(Stream<T> stream) {
        return newDataSet(stream.iterator());
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
        if (isFirstRecord) {
            Preconditions.checkNotNull(t, "First record cannot be null!");
            isFirstRecord = false;
            type = (Class<T>) t.getClass();
        }

        output.clear();
        kryo.writeObjectOrNull(output, t, type);
        try {
            bostream.write(output.getBuffer(), 0, output.position());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        output.clear();
    }

    @Override
    protected void finalize() { close(); }


    @Override
    public void close() {
        try {
            for (FileInputStream stream : usedStreams)
                stream.close();
        } catch (IOException e) { throw new PalangRuntimeException(e); }
    }

    public long sizeOnDisk() { return backFile.length();}


    @NotNull
    @Override
    public Iterator<T> iterator() {
        try {
            FileInputStream ifstream = new FileInputStream(backFile);
            LZ4BlockInputStream zstream = new LZ4BlockInputStream(ifstream);
            final Input input = new Input(zstream);
            usedStreams.add(ifstream);

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
                    return kryo.readObjectOrNull(input, type);
                }

                @Override
                protected void finalize() {
                    try {
                        ifstream.close();
                    } catch (IOException e) { throw new PalangRuntimeException(e); }
                }

            };
            return iter;
        } catch (IOException e) {
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
