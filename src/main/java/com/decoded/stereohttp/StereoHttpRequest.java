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

import java.util.Optional;
import java.util.function.Consumer;


public class StereoHttpRequest {
  private static final Logger LOG = LoggerFactory.getLogger(StereoHttpRequest.class);
  private final HttpCoreContext coreContext;
  private final HttpHost httpHost;
  private final HttpRequest httpRequest;
  private final BasicNIOConnPool pool;
  private final HttpAsyncRequester requester;

  private FutureCallback<HttpResponse> callback;
  private Optional<Consumer<StereoResponse>> maybeCompletionMapper = Optional.empty();
  private Optional<Consumer<Exception>> maybeExceptionMapper = Optional.empty();
  private Optional<Runnable> maybeCancellationMapper = Optional.empty();
  private Optional<Runnable> maybeAfterMapper = Optional.empty();
  private AbstractAsyncResponseConsumer<HttpResponse> responseConsumer = new BasicAsyncResponseConsumer();

  /**
   * Construct an NIO Reuest
   *
   * @param pool        a {@link BasicNIOConnPool} for outgoing connection requests
   * @param requester   the {@link HttpAsyncRequester}
   * @param httpHost    the {@link HttpHost}
   * @param httpRequest the {@link HttpRequest}
   */
  public StereoHttpRequest(BasicNIOConnPool pool,
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
        maybeCompletionMapper.ifPresent(consumer -> consumer.accept(new StereoResponseImpl(response)));
      }

      public void failed(final Exception ex) {
        maybeExceptionMapper.ifPresent(consumer -> consumer.accept(ex));
      }

      public void cancelled() {
        maybeCancellationMapper.ifPresent(Runnable::run);
      }
    };
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
      throw new IllegalStateException("Request was null, which means the http client was not properly initialized.");
    }

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
    this.maybeCompletionMapper = Optional.of(mapper);
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
    this.maybeExceptionMapper = Optional.of(exceptionMapper);
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
    this.maybeCancellationMapper = Optional.of(cancellationMapper);
    return this;
  }

  /**
   * After everything (error or not) do this.
   *
   * @param afterMapper a Runnable to run after.
   * @return this {@link StereoHttpRequest}
   */
  public StereoHttpRequest andThen(Runnable afterMapper) {
    this.maybeAfterMapper = Optional.of(afterMapper);
    return this;
  }
}
