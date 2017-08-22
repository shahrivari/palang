package amu.saeed.palang.types;

import amu.saeed.palang.PalangRuntimeException;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Charsets;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 */
public abstract class DataSet<T> implements Iterable<T>, AutoCloseable {

    protected abstract void conclude();

    protected abstract void append(T t);

    public <R> DataSet<R> map(Function<? super T, ? extends R> fun) {
        DataSet<R> ds = newObjectFromThis();
        for (T t : this)
            ds.append(fun.apply(t));
        ds.conclude();
        return ds;
    }

    public <R> DataSet<R> flatMap(Function<? super T, ? extends Iterator<? extends R>> fun) {
        DataSet<R> ds = newObjectFromThis();
        for (T t : this) {
            Iterator<? extends R> iter = fun.apply(t);
            while (iter.hasNext())
                ds.append(iter.next());
        }
        ds.conclude();
        return ds;
    }

    public DataSet<T> filter(Predicate<? super T> predicate) {
        DataSet<T> ds = newObjectFromThis();
        for (T t : this)
            if (predicate.test(t))
                ds.append(t);
        ds.conclude();
        return ds;
    }

    public DataSet<T> clone() {
        DataSet<T> ds = newObjectFromThis();
        for (T t : this)
            ds.append(t);
        ds.conclude();
        return ds;

    }

    public long count() {
        long count = 0;
        for (T t : this)
            count++;
        return count;
    }

    public void saveAsTextFile(String path) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(path);
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream, Charsets.UTF_8);
        for (T t : this) {
            outputStreamWriter.write(t.toString());
            outputStreamWriter.write("\n");
        }
    }

    public void saveAsBinaryFile(String path) throws IOException {
        FileOutputStream ofstream = new FileOutputStream(path);
        BufferedOutputStream bostream = new BufferedOutputStream(ofstream);
        Output output = new Output(bostream);
        Kryo kryo = new Kryo();
        boolean isFirst = true;
        for (T t : this) {
            if (isFirst) {
                isFirst = false;
                kryo.writeClassAndObject(output, t);
            } else
                kryo.writeObject(output, t);

        }
        output.close();
        bostream.close();
        ofstream.close();
    }

    private DataSet newObjectFromThis() {
        try {
            return this.getClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new PalangRuntimeException(e);
        }

    }

    @Override
    public Spliterator<T> spliterator() {
        return Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED | Spliterator.IMMUTABLE);
    }

    public Stream<T> stream() { return StreamSupport.stream(spliterator(), false); }

    @Override
    public boolean equals(Object o) {
        // self check
        if (this == o)
            return true;
        // null check
        if (o == null)
            return false;
        // type check and cast
        if (!(o instanceof DataSet))
            return false;
        DataSet ds = (DataSet) o;
        // field comparison
        Iterator<T> iter1 = this.iterator();
        Iterator<Object> iter2 = ds.iterator();

        while (iter1.hasNext())
            if (!iter1.next().equals(iter2.next()))
                return false;

        return true;
    }

    @Override
    public abstract void close();
}
