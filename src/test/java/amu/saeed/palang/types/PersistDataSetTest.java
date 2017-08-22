package amu.saeed.palang.types;

import org.assertj.core.util.Lists;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;


/**
 */
public class PersistDataSetTest {
    private static final int COUNT = 100_000;

    private Iterator<Integer> integerIterator(final int count) {
        return new Iterator<Integer>() {
            int i = 0;
            Random random = new Random();

            @Override
            public boolean hasNext() {
                return i < count;
            }

            @Override
            public Integer next() {
                i++;
                return random.nextInt();
            }
        };
    }

    @Test
    public void testEquals() {
        ArrayList<Integer> list = Lists.newArrayList(integerIterator(COUNT));
        DataSet<Integer> ints = PersistDataSet.newDataSet(list);
        DataSet<Integer> ints2 = PersistDataSet.newDataSet(list);
        assertThat(ints).isEqualTo(ints2);
        assertThat(ints.stream().collect(Collectors.toList()))
                .isEqualTo(ints2.stream().collect(Collectors.toList()));
        assertThat(ints.stream().collect(Collectors.toList())).isEqualTo(list);
        assertThat(ints2.stream().collect(Collectors.toList())).isEqualTo(list);
    }


    @Test
    public void testMap() {
        ArrayList<Integer> list = Lists.newArrayList(integerIterator(COUNT));
        PersistDataSet<Integer> ints = PersistDataSet.newDataSet(list);
        assertThat(ints.map(t -> t + 1).stream().collect(Collectors.toList()))
                .isEqualTo(list.stream().map(t -> t + 1).collect(Collectors.toList()));
    }


    @Test
    public void testFilter() {
        ArrayList<Integer> list = Lists.newArrayList(integerIterator(COUNT));
        PersistDataSet<Integer> ints = PersistDataSet.newDataSet(list);
        assertThat(ints.filter(t -> t % 2 == 1).stream().collect(Collectors.toList()))
                .isEqualTo(list.stream().filter(t -> t % 2 == 1).collect(Collectors.toList()));
    }


    @Test
    public void testClone() {
        PersistDataSet<Integer> ints = PersistDataSet.newDataSet(integerIterator(COUNT));
        DataSet<Integer> ints2 = ints.clone();
        assertThat(ints).isEqualTo(ints2);
    }

}
