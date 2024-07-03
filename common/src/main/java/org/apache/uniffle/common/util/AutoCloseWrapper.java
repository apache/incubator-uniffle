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

package org.apache.uniffle.common.util;

import com.google.common.annotations.VisibleForTesting;
import org.apache.uniffle.common.exception.RssException;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class AutoCloseWrapper<T extends Closeable> implements Closeable {
    private static final AtomicInteger REF_COUNTER = new AtomicInteger(0);
    private volatile T t;
    private Supplier<T> cf;

    public AutoCloseWrapper(Supplier<T> cf) {
        this.cf = cf;
    }

    public synchronized T get() {
//        if (REF_COUNTER.get() == 0) {
//            synchronized (this) {
//                if (REF_COUNTER.get() == 0) {
//                    t = cf.get();
//                    System.out.println(this + "==============get============");
//                }
//            }
//        }
//        REF_COUNTER.incrementAndGet();
        return cf.get();
    }

    @Override
    public synchronized void close() throws IOException {
//        int count = REF_COUNTER.get();
//        if (count == 0) {
//            return;
//        }
//        if (REF_COUNTER.compareAndSet(count, count - 1)) {
//            if (REF_COUNTER.get() == 0) {
//                try {
//                   // t.close();
//                    System.out.println(this + "==============close============");
//                } catch (Exception e) {
//                    throw new IOException("Failed to close the resource", e);
//                } finally {
//                    t = null;
//                }
//            }
//        }
    }

    public synchronized void forceClose() throws IOException {
        while (t != null) {
            this.close();
        }
    }

    @VisibleForTesting
    public int getRefCount() {
        return REF_COUNTER.get();
    }

    public static <T, X extends Closeable> T run(
            AutoCloseWrapper<X> autoCloseWrapper, AutoCloseCmd<T, X> cmd) {
        try (AutoCloseWrapper<X> wrapper = autoCloseWrapper) {
            return cmd.execute(wrapper.get());
        } catch (IOException e) {
            throw new RssException("Error closing client with error:", e);
        }
    }

    public interface AutoCloseCmd<T, X> {
        T execute(X x);
    }
}
