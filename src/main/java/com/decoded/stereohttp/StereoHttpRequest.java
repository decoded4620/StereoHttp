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
  public StereoHttpRequest(BasicNIOConnPool pool, HttpAsyncRequester requester, HttpHost httpHost, HttpRequest httpRequest) {
    this.pool = pool;
    this.requester = requester;
    this.httpHost = httpHost;
    this.httpRequest = httpRequest;
    this.coreContext = HttpCoreContext.create();
    callback = // Handle HTTP response from a callback
        new FutureCallback<HttpResponse>() {
          public void completed(final HttpResponse response) {
            LOG.info("Response from " + httpHost.toHostString() + " Stream?: " + response.getEntity().isStreaming());
            maybeCompletionMapper.ifPresent(consumer -> consumer.accept(new StereoResponseImpl(response)));
          }

          public void failed(final Exception ex) {
            LOG.error("Response from " + httpHost.toHostString() + " failure: ", ex);
            maybeExceptionMapper.ifPresent(consumer -> consumer.accept(ex));
          }

          public void cancelled() {
            LOG.error("Response from " + httpHost.toHostString() + " was cancelled");
            maybeCancellationMapper.ifPresent(Runnable::run);
          }
        };
  }

  public StereoHttpRequest setResponseConsumer(AbstractAsyncResponseConsumer<HttpResponse> responseConsumer) {
    this.responseConsumer = responseConsumer;
    return this;
  }

  public StereoHttpRequest execute() {
    requester.execute(new BasicAsyncRequestProducer(httpHost, httpRequest), responseConsumer, pool, coreContext,
                      callback);
    return this;
  }

  public StereoHttpRequest map(Consumer<StereoResponse> mapper) {
    this.maybeCompletionMapper = Optional.of(mapper);
    return this;
  }

  public StereoHttpRequest exceptionally(Consumer<Exception> exceptionMapper) {
    this.maybeExceptionMapper = Optional.of(exceptionMapper);
    return this;
  }

  public StereoHttpRequest cancelling(Runnable cancellationMapper) {
    this.maybeCancellationMapper = Optional.of(cancellationMapper);
    return this;
  }

  public StereoHttpRequest andThen(Runnable afterMapper) {
    this.maybeAfterMapper = Optional.of(afterMapper);
    return this;
  }
}
