package org.apache.uniffle.client.shuffle;

import com.google.common.base.Objects;

public class Record<K, V> {

  K key;
  V value;

  private Record(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public static <K, V> Record create(K key, V value) {
    return new Record<K, V>(key, value);
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  public void setValue(V value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "Record{" +
        "key=" + key +
        ", value=" + value +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Record<?, ?> record = (Record<?, ?>) o;
    return Objects.equal(key, record.key) &&
        Objects.equal(value, record.value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(key, value);
  }
}
