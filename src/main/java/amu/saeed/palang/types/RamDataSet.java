package amu.saeed.palang.types;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 */
public class RamDataSet<T> extends DataSet<T> {
    List<T> list = new ArrayList<>();

    public static <T> RamDataSet<T> newDataSet(Iterable<T> elements) {
        Preconditions.checkNotNull(elements);
        RamDataSet<T> ds = new RamDataSet<>();
        for (T element : elements)
            ds.append(element);
        return ds;
    }

    public static <T> RamDataSet<T> newDataSet(Iterator<T> iter) {
        Preconditions.checkNotNull(iter);
        RamDataSet<T> ds = new RamDataSet<>();
        while (iter.hasNext())
            ds.append(iter.next());
        return ds;
    }


    @Override
    protected void conclude() {}

    @Override
    protected void append(T t) { list.add(t); }

    @Override
    public int size() { return list.size(); }

    @NotNull
    @Override
    public Iterator<T> iterator() {return list.iterator(); }

    @Override
    public void forEach(Consumer<? super T> consumer) { list.forEach(consumer); }

    @Override
    public Spliterator<T> spliterator() { return list.spliterator(); }
}
