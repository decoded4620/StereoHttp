package com.decoded.stereohttp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
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

  private static final BiConsumer<Long, CountDownLatch> UPDATE_DELTA = (startVal, latch) -> {
    long delta = System.currentTimeMillis() - startVal;
    if (delta < MIN_LATENCY.get()) {
      LOG.info("Min latency improved!! " + delta);
      MIN_LATENCY.set(delta);
    }

    if (delta > MAX_LATENCY.get()) {
      LOG.info("Max latency degradation!!: " + delta);
      MAX_LATENCY.set(delta);
    }

    latch.countDown();
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
      LOG.debug("{" + Thread.currentThread().getName() + "}:" + message.get());
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
    int concurrent = concurrentRequests.incrementAndGet();

    if(concurrent > MAX_CONCURRENT_REQUESTS.get()) {
      MAX_CONCURRENT_REQUESTS.set(concurrent);
      LOG.info("Concurrent requests now: " + concurrent);
    }

    debugIf(() -> "execute(): " + timeout + " ms");

    return CompletableFuture.supplyAsync(() -> performQuery(restRequest, type, null),
        stereoHttpClient.getExecutorService());
  }

  public <ID_T> CompletableFuture<Feedback<List<T>>> executeBatch(TypeReference typeReference,
      RestRequest<List<T>, ID_T> restRequest
  ) {
    debugIf(() -> "execute(): " + timeout + " ms");
    return CompletableFuture.supplyAsync(() -> performQuery(restRequest, null, typeReference),
        stereoHttpClient.getExecutorService());
  }

  /**
   * internal method to run the request.
   *
   * @param restRequest a {@link RestRequest}
   * @param <ID_T>      the type of identifier used to locate the deserializedContent for the query.
   *
   * @return the type specified feedback object for this task.
   */
  private <X, ID_T> Feedback<X> performQuery(RestRequest<X, ID_T> restRequest,
      Class<X> clazz,
      TypeReference typeReference
  ) {

    debugIf(() -> "Stereo Request: " + restRequest.getRequestUri());
    // used for effectively final scope rules

    Feedback<X> feedback = new Feedback<>();
    stereoHttpClient.httpQuery(restRequest.getHost(), restRequest.getPort(), restRequest.getRequestMethod(),
        restRequest.getRequestUri(), (stereoRequest) -> {
          final long start = System.currentTimeMillis();

          stereoRequest.map(response -> {

            final int statusCode = response.getRawHttpResponse().getStatusLine().getStatusCode();
            debugIf(() -> "Stereo Response: [" + restRequest.getRequestUri() + " (" + statusCode + ")]: \n---\n" + response.getContent() + "\n---");

            // all 2xx
            if (statusCode == HttpStatus.SC_OK || (String.valueOf(statusCode).startsWith("20"))) {
              ObjectMapper mapper = new ObjectMapper();
              try {

                X value = null;
                if (clazz != null) {
                  value = mapper.readValue(response.getContent(), clazz);
                } else if (typeReference != null) {
                  value = mapper.readValue(response.getContent(), typeReference);
                } else {
                  feedback.setError(HttpStatus.SC_BAD_REQUEST, response.getContent(),
                      new StereoRequestException(HttpStatus.SC_BAD_REQUEST, "Could not detect required type"));
                }

                Optional.ofNullable(value).ifPresent(v -> {
                  feedback.setSuccess(statusCode, v, response.getContent());

                  debugIf(() -> "Stereo read deserializedContent to type: " + v.getClass().getName());
                });

              } catch (Exception ex) {
                feedback.setError(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getContent(), ex);
              }
              UPDATE_DELTA.accept(start, feedback);
            } else {
              feedback.setError(statusCode, response.getContent(),
                  new StereoRequestException(statusCode, "Request Exception Occurred"));
              UPDATE_DELTA.accept(start, feedback);
            }
          }).exceptionally(ex -> {
            UPDATE_DELTA.accept(start, feedback);
            feedback.setError(HttpStatus.SC_INTERNAL_SERVER_ERROR, "", ex);
          }).cancelling(() -> {
            UPDATE_DELTA.accept(start, feedback);
            feedback.cancel();
            LOG.warn("Cancelled the restRequest");
          }).andThen(() -> {
            debugIf(() -> "Completion mapper");
          });
        });

    try {
      long start = System.currentTimeMillis();
      feedback.await(timeout, TimeUnit.MILLISECONDS);
      if(System.currentTimeMillis() - start > timeout) {
        LOG.error("Timed out!");
        feedback.setError(HttpStatus.SC_REQUEST_TIMEOUT, "{\"message\": \"Request timed out\"}",
            new StereoRequestException(HttpStatus.SC_REQUEST_TIMEOUT, "Request Timed out."));
      }
    } catch (InterruptedException ex) {
      feedback.setError(HttpStatus.SC_INTERNAL_SERVER_ERROR, "{\"message\": \"Request interrupted\"}",
          new StereoRequestException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Request Interrupted", ex));
    }

    concurrentRequests.decrementAndGet();
    // user should have data here, or null
    return feedback;
  }
}