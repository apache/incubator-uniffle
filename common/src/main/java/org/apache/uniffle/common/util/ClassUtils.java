package org.apache.uniffle.common.util;

import java.lang.reflect.Constructor;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.uniffle.common.exception.RssException;

public class ClassUtils {

  public static <T> T instantiate(Class<T> clazz, Pair<Class<T>, Object>... typeAndVals)
      throws RssException {
    try {
      if (typeAndVals == null || typeAndVals.length == 0) {
        return clazz.newInstance();
      }
      Class<T>[] types = Stream.of(typeAndVals).map(x -> x.getLeft()).toArray(Class[]::new);
      Constructor<T> constructor = clazz.getConstructor(types);
      return constructor.newInstance(
          Stream.of(typeAndVals).map(x -> x.getRight()).toArray(Object[]::new)
      );
    } catch (Exception e) {
      throw new RssException(e);
    }
  }
}
