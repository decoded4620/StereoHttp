package com.decoded.stereohttp;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.pool.BasicNIOConnPool;
import org.apache.http.nio.protocol.AbstractAsyncResponseConsumer;
import org.apache.http.nio.protocol.BasicAsyncRequestProducer;
import org.apache.http.nio.protocol.BasicAsyncResponseConsumer;
import org.apache.http.nio.protocol.HttpAsyncRequester;
import org.apache.http.protocol.HttpCoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;


/**
 * The internal class used by Stereo to make http requests on its non-blocking Http Client.
 */
public class StereoHttpRequest {
  private static final Logger LOG = LoggerFactory.getLogger(StereoHttpRequest.class);
  private final HttpCoreContext coreContext;
  private final HttpHost httpHost;
  private final HttpRequest httpRequest;
  private final BasicNIOConnPool pool;
  private final HttpAsyncRequester requester;

  private FutureCallback<HttpResponse> callback;

  private List<Consumer<StereoResponse>> completionMappers = new ArrayList<>();
  private List<Consumer<Exception>> errorMappers = new ArrayList<>();
  private List<Runnable> cancellationMappers = new ArrayList<>();
  private List<Runnable> afterMappers = new ArrayList<>();
  private AbstractAsyncResponseConsumer<HttpResponse> responseConsumer = new BasicAsyncResponseConsumer();

  /**
   * Construct an NIO Reuest
   *
   * @param pool        a {@link BasicNIOConnPool} for outgoing connection requests
   * @param requester   the {@link HttpAsyncRequester}
   * @param httpHost    the {@link HttpHost}
   * @param httpRequest the {@link HttpRequest}
   */
  StereoHttpRequest(BasicNIOConnPool pool,
                           HttpAsyncRequester requester,
                           HttpHost httpHost,
                           HttpRequest httpRequest
  ) {
    this.pool = pool;
    this.requester = requester;
    this.httpHost = httpHost;
    this.httpRequest = httpRequest;
    this.coreContext = HttpCoreContext.create();
    this.callback = new FutureCallback<HttpResponse>() {
      public void completed(final HttpResponse response) {
        completionMappers.forEach(consumer -> consumer.accept(new StereoResponseImpl(response)));
      }

      public void failed(final Exception ex) {
        LOG.error("StereoHttpRequest Failed: ", ex);
        errorMappers.forEach(consumer -> consumer.accept(ex));
      }

      public void cancelled() {
        LOG.error("StereoHttpRequest was cancelled");
        cancellationMappers.forEach(Runnable::run);
      }
    };
  }

  // helper for efficient debug logging
  private static void debugIf(Supplier<String> message) {
    if(LOG.isErrorEnabled()) {
      LOG.debug(message.get());
    }
  }

  /**
   * Consume the response
   * @param responseConsumer a consumer.
   * @return this {@link StereoHttpRequest}
   */
  public StereoHttpRequest setResponseConsumer(AbstractAsyncResponseConsumer<HttpResponse> responseConsumer) {
    this.responseConsumer = responseConsumer;
    return this;
  }

  /**
   * Package private, only called by the HttpClient internally.
   *
   * @return this {@link StereoHttpRequest}
   * @throws IllegalStateException if the http client is not initialized
   */
  StereoHttpRequest execute() {
    if (requester == null) {
      throw new IllegalStateException("RestRequest was null, which means the http client was not properly initialized.");
    }

    debugIf(() -> "execute http request to " + httpHost + ", " + httpRequest.toString());
    requester.execute(new BasicAsyncRequestProducer(httpHost, httpRequest), responseConsumer, pool, coreContext,
                      callback);
    return this;
  }

  /**
   * When the response is available, if the completion mapper is set
   * it will be called with the response.
   *
   * @param mapper a Consumer to accept the response.
   * @return this {@link StereoHttpRequest}
   */
  public StereoHttpRequest map(Consumer<StereoResponse> mapper) {
    if(!completionMappers.contains(mapper)) {
      Optional.ofNullable(mapper).ifPresent(completionMappers::add);
    }
    return this;
  }

  /**
   * When the response fails, if the error mapper is set
   * it will be called with the exception.
   *
   * @param exceptionMapper a Consumer to accept the error.
   * @return this {@link StereoHttpRequest}
   */
  public StereoHttpRequest exceptionally(Consumer<Exception> exceptionMapper) {
    Optional.ofNullable(exceptionMapper).ifPresent(errorMappers::add);
    return this;
  }

  /**
   * When the response is cancelled, if the cancellation mapper is set
   * it will be called.
   *
   * @param cancellationMapper a Consumer to accept the cancellation.
   * @return this {@link StereoHttpRequest}
   */
  public StereoHttpRequest cancelling(Runnable cancellationMapper) {
    Optional.ofNullable(cancellationMapper).ifPresent(cancellationMappers::add);
    return this;
  }

  /**
   * After everything (error or not) do this.
   *
   * @param afterMapper a Runnable to run after.
   * @return this {@link StereoHttpRequest}
   */
  public StereoHttpRequest andThen(Runnable afterMapper) {
    Optional.ofNullable(afterMapper).ifPresent(afterMappers::add);
    return this;
  }
}
