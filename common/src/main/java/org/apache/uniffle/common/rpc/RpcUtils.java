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

package org.apache.uniffle.common.rpc;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;

import org.apache.uniffle.common.metrics.RPCMetrics;

/**
 * Utilities for handling server RPC calls.
 *
 * <p>There are two types of RPC calls: 1. RPCs that only throw IOException 2. Streaming RPCs For
 * each of these, if failureOk is set, non-fatal errors will only be logged at the DEBUG level and
 * failure metrics will not be recorded.
 */
public class RpcUtils {

  /**
   * Calls the given {@link RpcCallableThrowsIOException} and handles any exceptions thrown. If the
   * RPC fails, a warning or error will be logged.
   *
   * @param logger the logger to use for this call
   * @param metrics the RPC metrics to update
   * @param callable the callable to call
   * @param methodName the name of the method, used for metrics
   * @param description the format string of the description, used for logging
   * @param responseObserver gRPC response observer
   * @param args the arguments for the description
   * @param <T> the return type of the callable
   */
  public static <T> void call(
      Logger logger,
      RPCMetrics metrics,
      RpcCallableThrowsIOException<T> callable,
      String methodName,
      String description,
      StreamObserver<T> responseObserver,
      Object... args) {
    call(logger, metrics, callable, methodName, false, description, responseObserver, args);
  }

  /**
   * Calls the given {@link RpcCallableThrowsIOException} and handles any exceptions thrown.
   *
   * <p>The failureOk parameter indicates whether or not AlluxioExceptions and IOExceptions are
   * expected results (for example it would be false for the exists() call). In this case, we do not
   * log the failure or increment failure metrics. When a RuntimeException is thrown, we always
   * treat it as a failure and log an error and increment metrics.
   *
   * @param logger the logger to use for this call
   * @param metrics the RPC metrics to update
   * @param callable the callable to call
   * @param methodName the name of the method, used for metrics
   * @param failureOk whether failures are expected (affects logging and metrics)
   * @param description the format string of the description, used for logging
   * @param responseObserver gRPC response observer
   * @param args the arguments for the description
   * @param <T> the return type of the callable
   */
  public static <T> void call(
      Logger logger,
      RPCMetrics metrics,
      RpcCallableThrowsIOException<T> callable,
      String methodName,
      boolean failureOk,
      String description,
      StreamObserver<T> responseObserver,
      Object... args) {
    T response;
    try {
      response = callAndReturn(logger, metrics, callable, methodName, failureOk, description, args);
    } catch (StatusException | StatusRuntimeException e) {
      responseObserver.onError(e);
      return;
    }
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  /**
   * Calls the given {@link RpcCallableThrowsIOException} and returns its result. Exceptions are
   * logged, accounted for in metrics and then rethrown at the end.
   *
   * <p>The failureOk parameter indicates whether or not AlluxioExceptions and IOExceptions are
   * expected results (for example it would be false for the exists() call). In this case, we do not
   * log the failure or increment failure metrics. When a RuntimeException is thrown, we always
   * treat it as a failure and log an error and increment metrics.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param methodName the name of the method, used for metrics
   * @param failureOk whether failures are expected (affects logging and metrics)
   * @param description the format string of the description, used for logging
   * @param args the arguments for the description
   * @param <T> the return type of the callable
   * @return the result. (Null if failed)
   */
  public static <T> T callAndReturn(
      Logger logger,
      RPCMetrics metrics,
      RpcCallableThrowsIOException<T> callable,
      String methodName,
      boolean failureOk,
      String description,
      Object... args)
      throws StatusException {
    String debugDesc = logger.isDebugEnabled() ? String.format(description, args) : null;
    long startTimeMs = System.nanoTime();
    try {
      logger.debug("Enter: {}: {}", methodName, debugDesc);
      T res = callable.call();
      logger.debug("Exit: {}: {}", methodName, description);
      return res;
    } catch (Exception e) {
      recordFailure(e, methodName, failureOk, description, debugDesc, logger, args);
      throw Status.INTERNAL.withDescription(e.getMessage()).asException();
    } finally {
      long durationUs = (System.nanoTime() - startTimeMs) / 1000L;
      metrics.incCounter("TotalRpcsProcessingTimeUs", durationUs);
      metrics.incCounter("TotalRpcsOps");
    }
  }

  private static void recordFailure(
      Throwable e,
      String methodName,
      boolean failureOk,
      String description,
      String debugDesc,
      Logger logger,
      Object[] args) {
    logger.debug("Exit (Error): {}: {}", methodName, debugDesc, e);
    if (!failureOk) {
      if (!logger.isDebugEnabled()) {
        logger.warn(
            "Exit (Error): {}: {}, Error={}",
            methodName,
            String.format(description, args),
            e.toString());
      }
    }
  }

  /**
   * An interface representing a callable which can only throw IO exceptions.
   *
   * @param <T> the return type of the callable
   */
  public interface RpcCallableThrowsIOException<T> {
    /**
     * The RPC implementation.
     *
     * @return the return value from the RPC
     */
    T call() throws Exception;
  }

  /**
   * An interface representing a streaming RPC callable.
   *
   * @param <T> the return type of the callable
   */
  public interface StreamingRpcCallable<T> {
    /**
     * The RPC implementation.
     *
     * @return the return value from the RPC
     */
    T call() throws Exception;

    /**
     * Handles exception.
     *
     * @param throwable the exception
     */
    void exceptionCaught(Throwable throwable);
  }
}
