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

package org.apache.uniffle.client.impl.grpc;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.retry.RetryStrategy;

/**
 * This class refers to https://github.com/grpc/grpc-java/issues/5856
 */
class RetryInterceptor implements ClientInterceptor {
  private static final Logger LOGGER = LoggerFactory.getLogger(RetryInterceptor.class);

  private int retryNumber = 0;
  private RetryStrategy retryStrategy;

  RetryInterceptor(RetryStrategy retryStrategy) {
    this.retryStrategy = retryStrategy;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      final MethodDescriptor<ReqT, RespT> method,
      final CallOptions callOptions,
      final Channel next) {

    class RetryingUnaryRequestClientCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {

      Listener listener;
      Metadata metadata;
      ReqT msg;
      int req;
      ClientCall call;

      @Override
      public void start(Listener listener, Metadata metadata) {
        this.listener = listener;
        this.metadata = metadata;
      }

      @Override
      public void sendMessage(ReqT msg) {
        assert this.msg == null;
        this.msg = msg;
      }

      @Override
      public void request(int num) {
        req += num;
        assert this.msg == null;
      }

      @Override
      public boolean isReady() {
        return false;
      }

      @Override
      public void halfClose() {
        startCall(new CheckingListener());
      }

      private void startCall(Listener listener) {
        call = next.newCall(method, callOptions);
        Metadata headers = new Metadata();
        headers.merge(metadata);
        call.start(listener, headers);
        assert this.msg != null;
        call.request(req);
        call.sendMessage(msg);
        call.halfClose();
      }

      @Override
      public void cancel(String s, Throwable t) {
        if (call != null) { // need synchronization
          call.cancel(s, t);
        }
        // technically should use CallOptions.getExecutor() if set
        listener.onClose(Status.CANCELLED.withDescription(s).withCause(t), new Metadata());
      }

      class CheckingListener extends ForwardingClientCallListener {

        Listener<RespT> delegate;

        @Override
        protected Listener delegate() {
          if (delegate == null) {
            throw new IllegalStateException();
          }
          return delegate;
        }

        @Override
        public void onHeaders(Metadata headers) {
          delegate = listener;
          super.onHeaders(headers);
        }

        @Override
        public void onClose(Status status, Metadata trailers) {
          if (delegate != null) {
            super.onClose(status, trailers);
            return;
          }
          if (!retryStrategy.needToRetry(status.getCode().name(), retryNumber++)) {
            delegate = listener;
            super.onClose(status, trailers);
            return;
          }
          startCall(new CheckingListener()); // to allow multiple retries
        }
      }
    }

    return new RetryingUnaryRequestClientCall<>();
  }
}
