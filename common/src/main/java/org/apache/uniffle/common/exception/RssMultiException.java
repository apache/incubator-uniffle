/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.uniffle.common.exception;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Wraps rss multiple exceptions. Allows multiple exceptions to be thrown as a single exception. */
public class RssMultiException extends Exception {
  private List<Throwable> nested;

  /* ------------------------------------------------------------ */
  public RssMultiException() {
    super("RssMultiple exceptions");
  }

  /* ------------------------------------------------------------ */
  public void add(Throwable e) {
    if (e == null) throw new IllegalArgumentException();

    if (nested == null) {
      initCause(e);
      nested = new ArrayList<>();
    } else addSuppressed(e);

    if (e instanceof RssMultiException) {
      RssMultiException me = (RssMultiException) e;
      nested.addAll(me.nested);
    } else nested.add(e);
  }

  /* ------------------------------------------------------------ */
  public int size() {
    return (nested == null) ? 0 : nested.size();
  }

  /* ------------------------------------------------------------ */
  public List<Throwable> getThrowables() {
    if (nested == null) return Collections.emptyList();
    return nested;
  }

  /* ------------------------------------------------------------ */
  public Throwable getThrowable(int i) {
    return nested.get(i);
  }

  /* ------------------------------------------------------------ */
  /**
   * Throw a multiexception. If this multi exception is empty then no action is taken. If it
   * contains a single exception that is thrown, otherwise the this multi exception is thrown.
   *
   * @exception Exception the Error or Exception if nested is 1, or the MultiException itself if
   *     nested is more than 1.
   */
  public void ifExceptionThrow() throws Exception {
    if (nested == null) return;

    switch (nested.size()) {
      case 0:
        break;
      case 1:
        Throwable th = nested.get(0);
        if (th instanceof Error) throw (Error) th;
        if (th instanceof Exception) throw (Exception) th;
      default:
        throw this;
    }
  }

  /* ------------------------------------------------------------ */
  /**
   * Throw a Runtime exception. If this multi exception is empty then no action is taken. If it
   * contains a single error or runtime exception that is thrown, otherwise the this multi exception
   * is thrown, wrapped in a runtime exception.
   *
   * @exception Error If this exception contains exactly 1 {@link Error}
   * @exception RuntimeException If this exception contains 1 {@link Throwable} but it is not an
   *     error, or it contains more than 1 {@link Throwable} of any type.
   */
  public void ifExceptionThrowRuntime() throws Error {
    if (nested == null) return;

    switch (nested.size()) {
      case 0:
        break;
      case 1:
        Throwable th = nested.get(0);
        if (th instanceof Error) throw (Error) th;
        else if (th instanceof RuntimeException) throw (RuntimeException) th;
        else throw new RuntimeException(th);
      default:
        throw new RuntimeException(this);
    }
  }

  /* ------------------------------------------------------------ */
  /**
   * Throw a multiexception. If this multi exception is empty then no action is taken. If it
   * contains a any exceptions then this multi exception is thrown.
   *
   * @throws RssMultiException the multiexception if there are nested exception
   */
  public void ifExceptionThrowMulti() throws RssMultiException {
    if (nested == null) return;

    if (nested.size() > 0) throw this;
  }

  /* ------------------------------------------------------------ */
  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    str.append(org.eclipse.jetty.util.MultiException.class.getSimpleName());
    if ((nested == null) || (nested.size() <= 0)) {
      str.append("[]");
    } else {
      str.append(nested);
    }
    return str.toString();
  }
}
