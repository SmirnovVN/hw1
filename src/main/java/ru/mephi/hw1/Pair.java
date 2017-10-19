package ru.mephi.hw1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Pair<K extends Writable,V extends Writable> implements Writable{


    private K key;

    private V value;

    Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return key + "=" + value;
    }

    @Override
    public int hashCode() {
        return key.hashCode() * 13 + (value == null ? 0 : value.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof Pair) {
            Pair pair = (Pair) o;
            return (key != null ? key.equals(pair.key) : pair.key == null)
                    && (value != null ? value.equals(pair.value) : pair.value == null);
        }
        return false;
    }

    public void write(DataOutput dataOutput) throws IOException {
        key.write(dataOutput);
        value.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        key.readFields(dataInput);
        value.readFields(dataInput);
    }
}
