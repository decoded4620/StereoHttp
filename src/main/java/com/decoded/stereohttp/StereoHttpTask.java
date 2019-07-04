package com.decoded.stereohttp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;


/**
 * Http Task, which returns a type of type T The Stereo Task attempts to convert the raw result (which is expected to be
 * in JSON format) into the type T, along with any custom encoder or decoders required.
 *
 * <h2>Usage</h2>
 * <pre>
 *   new StereoHttpTask&lt;MyRecordType&gt;(MyRecordType.class, myStereoClient, 2000).execute(RestRequestBuilders...
 *   .build());
 * </pre>
 *
 * @param <T> the type to create using the underlying Http Response data  ({@link StereoResponse}) from a {@link
 *            StereoHttpRequest}.
 */
public class StereoHttpTask<T> {
  private static final Logger LOG = LoggerFactory.getLogger(StereoHttpTask.class);

  private static final AtomicLong MAX_LATENCY = new AtomicLong(Integer.MIN_VALUE);
  private static final AtomicLong MIN_LATENCY = new AtomicLong(Integer.MAX_VALUE);
  private static final AtomicInteger concurrentRequests = new AtomicInteger(0);
  private static final AtomicInteger MAX_CONCURRENT_REQUESTS = new AtomicInteger(0);

  private static final Consumer<Long> UPDATE_DELTA = (startVal) -> {
    long delta = System.currentTimeMillis() - startVal;
    if (delta < MIN_LATENCY.get()) {
      LOG.info("Min latency improved!! " + delta);
      MIN_LATENCY.set(delta);
    }

    if (delta > MAX_LATENCY.get()) {
      LOG.info("Max latency degradation!!: " + delta);
      MAX_LATENCY.set(delta);
    }
  };

  private final StereoHttpClient stereoHttpClient;
  private final int timeout;

  /**
   * StereoHttpTask
   *
   *
   * @param stereoHttpClient the stereo client.
   * @param timeout          the timeout.
   */
  public StereoHttpTask(StereoHttpClient stereoHttpClient, int timeout) {
    this.stereoHttpClient = stereoHttpClient;
    this.timeout = timeout;
  }

  // helper for efficient debug logging
  private static void debugIf(Supplier<String> message) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(message.get());
    }
  }

  public static long maxLatency() {
    return MAX_LATENCY.get();
  }

  public static long minLatency() {
    return MIN_LATENCY.get();
  }

  /**
   * Execute the restRequest.
   *
   * @param restRequest the restRequest
   * @param <ID_T>      the identifier type for the request. Used as a pass through here.
   *
   * @return a {@link CompletableFuture} of a {@link Feedback}&lt;T&gt;
   */
  public <ID_T> CompletableFuture<Feedback<T>> execute(Class<T> type, RestRequest<T, ID_T> restRequest) {
    return performQuery(restRequest, type, null);
  }

  public <ID_T> CompletableFuture<Feedback<List<T>>> executeBatch(TypeReference typeReference,
      RestRequest<List<T>, ID_T> restRequest
  ) {
    debugIf(() -> "executeBatch(): " + timeout + " ms");
    return performQuery(restRequest, null, typeReference);
  }

  /**
   * Process the ok type feedback
   * @param statusCode the code
   * @param feedback the feedback object
   * @param clazz the class to deserialize to
   * @param typeReference the type to deserialze to
   * @param response the response
   * @param startTime the start time of the request
   * @param <X> the type
   */
  private <X> void processOkResponseFeedback(int statusCode,
      Feedback<X> feedback,
      Class<X> clazz,
      TypeReference<X> typeReference,
      StereoResponse response,
      long startTime
  ) {
    UPDATE_DELTA.accept(startTime);
    if (response.getContent() != null) {
      ObjectMapper mapper = new ObjectMapper();
      try {
        if (clazz != null) {
          feedback.setSuccess(statusCode, mapper.readValue(response.getContent(), clazz), response.getContent());
        } else if (typeReference != null) {
          feedback.setSuccess(statusCode, mapper.readValue(response.getContent(), typeReference),
              response.getContent());
        } else {
          feedback.setError(HttpStatus.SC_BAD_REQUEST, response.getContent(),
              new StereoRequestException(HttpStatus.SC_BAD_REQUEST, "Could not detect required type"));
        }
      } catch (IOException ex) {
        feedback.setError(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getContent(), ex);
      }
    } else {
      feedback.setSuccess(statusCode, null, response.getContent());
    }
  }

  /**
   * Process the error type feedback
   * @param statusCode the status
   * @param feedback the feedback
   * @param response the response
   * @param startTime the start time of the request
   * @param <X> the error response
   */
  private <X> void processErrorResponseFeedback(int statusCode,
      Feedback<X> feedback,
      StereoResponse response,
      long startTime
  ) {
    feedback.setError(statusCode, response == null ? "" : response.getContent(),
        new StereoRequestException(statusCode, "Request Exception Occurred"));
    UPDATE_DELTA.accept(startTime);
  }

  /**
   * internal method to run the request.
   *
   * @param restRequest a {@link RestRequest}
   * @param <ID_T>      the type of identifier used to locate the deserializedContent for the query.
   *
   * @return the type specified feedback object for this task.
   */
  private <X, ID_T> CompletableFuture<Feedback<X>> performQuery(RestRequest<X, ID_T> restRequest,
      Class<X> clazz,
      TypeReference typeReference
  ) {

    CompletableFuture<Feedback<X>> future = new CompletableFuture<>();
    debugIf(() -> "Perform Stereo Query " + clazz.getSimpleName() + ", " + restRequest.getRequestUri());
    Callable<Feedback<X>> feedbackRunner = () -> {
      debugIf(() -> "Stereo Request Enqueue: [" + restRequest.getRequestUri() + "]");
      // used for effectively final scope rules

      Feedback<X> feedback = new Feedback<>();
      int concurrent = concurrentRequests.incrementAndGet();

      if (concurrent > MAX_CONCURRENT_REQUESTS.get()) {
        MAX_CONCURRENT_REQUESTS.set(concurrent);
      }

      stereoHttpClient.httpQuery(restRequest.getHost(), restRequest.getPort(), restRequest.getRequestMethod(),
          restRequest.getRequestUri(), (stereoRequest) -> {
            final long start = System.currentTimeMillis();
            debugIf(() -> "Stereo Request Start: [" + restRequest.getRequestUri() + "]");
            stereoRequest.map(response -> {

              final int statusCode = response.getRawHttpResponse().getStatusLine().getStatusCode();
              debugIf(
                  () -> "Stereo Response: [" + restRequest.getRequestUri() + " (" + statusCode + ")]: \n---\n" + response
                      .getContent() + "\n---");
              // all 2xx
              if (statusCode == HttpStatus.SC_OK || (String.valueOf(statusCode).startsWith("20"))) {
                processOkResponseFeedback(statusCode, feedback, clazz, typeReference, response, start);
              } else {
                // HUGE TODO - make sure we support different groups of status types.
                if(statusCode != HttpStatus.SC_INTERNAL_SERVER_ERROR) {
                  LOG.warn("TODO - Support: " + statusCode + " status code group");
                }

                processErrorResponseFeedback(statusCode, feedback, response, start);
              }
            }).exceptionally(ex -> {
              processErrorResponseFeedback(HttpStatus.SC_INTERNAL_SERVER_ERROR, feedback, null, start);
            }).cancelling(() -> {
              UPDATE_DELTA.accept(start);
              feedback.cancel();
              LOG.warn("Cancelled the restRequest");
            });
          });

      try {
        if (!feedback.await(timeout, TimeUnit.MILLISECONDS)) {
          LOG.error("Request Timed out!", new StereoRequestException(408, "Request Timed Out!"));
          feedback.setError(HttpStatus.SC_REQUEST_TIMEOUT, "{\"message\": \"Request timed out\"}",
              new StereoRequestException(HttpStatus.SC_REQUEST_TIMEOUT, "Request Timed out."));

        }
      } catch (InterruptedException ex) {
        feedback.setError(HttpStatus.SC_INTERNAL_SERVER_ERROR, "{\"message\": \"Request interrupted\"}",
            new StereoRequestException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Request Interrupted", ex));
      }

      concurrentRequests.decrementAndGet();
      // user should have data here, or null
      future.complete(feedback);
      return feedback;
    };
    stereoHttpClient.getExecutorService().submit(feedbackRunner);

    return future;
  }
}