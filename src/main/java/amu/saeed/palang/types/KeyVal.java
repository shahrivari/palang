package amu.saeed.palang.types;

import com.google.common.base.Preconditions;

public class KeyVal<K, V> {
    private K key;
    private V value;

    private KeyVal() {}

    public KeyVal(K key, V value) {
        Preconditions.checkNotNull(key, "Key cannot be null!");
        this.key = key;
        this.value = value;
    }

    public K getKey() { return key; }

    public V getValue() { return value; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyVal<?, ?> keyVal = (KeyVal<?, ?>) o;

        if (!key.equals(keyVal.key)) return false;
        return value != null ? value.equals(keyVal.value) : keyVal.value == null;
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("{%s: %s}", String.valueOf(key), String.valueOf(value));
    }
}
