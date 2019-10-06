package com.decoded.stereohttp;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;


/**
 * The internal class used by Stereo to make http requests on its non-blocking Http Client.
 */
public class StereoHttpRequestHandler {
  private static final Logger LOG = LoggerFactory.getLogger(StereoHttpRequestHandler.class);

  private FutureCallback<HttpResponse> callback;

  private List<Consumer<StereoResponse>> completionMappers = new ArrayList<>();
  private List<Consumer<Exception>> errorMappers = new ArrayList<>();
  private List<Runnable> cancellationMappers = new ArrayList<>();
  private List<Runnable> afterMappers = new ArrayList<>();
  private HttpRequest request;
  /**
   * Construct an NIO Request
   *
   * @param httpRequest the {@link HttpRequest}
   */
  public StereoHttpRequestHandler(HttpRequest httpRequest) {
    this.request = httpRequest;
    this.callback = new FutureCallback<HttpResponse>() {
        public void completed(final HttpResponse response) {
        debugIf(() -> "StereoHttpRequestHandler completed: " + response.getStatusLine().getStatusCode());
        completionMappers.forEach(consumer -> consumer.accept(new StereoResponseImpl(response)));
      }

      public void failed(final Exception ex) {
        LOG.info("StereoHttpRequestHandler failed, consumer will handle exception " + ex.getMessage());
        errorMappers.forEach(consumer -> consumer.accept(ex));
      }

      public void cancelled() {
        LOG.info("StereoHttpRequestHandler was cancelled: " + httpRequest.getRequestLine().toString());
        cancellationMappers.forEach(Runnable::run);
      }
    };
  }

  public HttpRequest getRequest() {
    return request;
  }

  // helper for efficient debug logging
  private static void debugIf(Supplier<String> message) {
    if(LOG.isDebugEnabled()) {
      LOG.debug(message.get());
    }
  }
  private static void infoIf(Supplier<String> message) {
    if(LOG.isInfoEnabled()) {
      LOG.info(message.get());
    }
  }


  public FutureCallback<HttpResponse> getApacheHttpCallback() {
    return callback;
  }

  /**
   * When the response is available, if the completion mapper is set
   * it will be called with the response.
   *
   * @param mapper a Consumer to accept the response.
   * @return this {@link StereoHttpRequestHandler}
   */
  public StereoHttpRequestHandler map(Consumer<StereoResponse> mapper) {
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
   * @return this {@link StereoHttpRequestHandler}
   */
  public StereoHttpRequestHandler exceptionally(Consumer<Exception> exceptionMapper) {
    Optional.ofNullable(exceptionMapper).ifPresent(errorMappers::add);
    return this;
  }

  /**
   * When the response is cancelled, if the cancellation mapper is set
   * it will be called.
   *
   * @param cancellationMapper a Consumer to accept the cancellation.
   * @return this {@link StereoHttpRequestHandler}
   */
  public StereoHttpRequestHandler cancelling(Runnable cancellationMapper) {
    Optional.ofNullable(cancellationMapper).ifPresent(cancellationMappers::add);
    return this;
  }

  /**
   * After everything (error or not) do this.
   *
   * @param afterMapper a Runnable to run after.
   * @return this {@link StereoHttpRequestHandler}
   */
  public StereoHttpRequestHandler andThen(Runnable afterMapper) {
    Optional.ofNullable(afterMapper).ifPresent(afterMappers::add);
    return this;
  }
}
