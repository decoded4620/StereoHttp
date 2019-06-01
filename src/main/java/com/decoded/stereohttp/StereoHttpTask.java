package com.decoded.stereohttp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
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
  private final Class<T> tClass;
  private final StereoHttpClient stereoHttpClient;
  private final int timeout;

  /**
   * StereoHttpTask
   *
   * @param tClass           the class.
   * @param stereoHttpClient the stereo client.
   * @param timeout          the timeout.
   */
  public StereoHttpTask(Class<T> tClass, StereoHttpClient stereoHttpClient, int timeout) {
    this.tClass = tClass;
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
    debugIf(() -> "execute(): " + timeout + " ms");
    return CompletableFuture.supplyAsync(() -> performQuery(restRequest, type, null), stereoHttpClient.getExecutorService());
  }

  public <ID_T> CompletableFuture<Feedback<List<T>>> executeBatch(TypeReference typeReference, RestRequest<List<T>, ID_T> restRequest) {
    debugIf(() -> "execute(): " + timeout + " ms");
    return CompletableFuture.supplyAsync(() -> performQuery(restRequest, null, typeReference), stereoHttpClient.getExecutorService());
  }

  /**
   * internal method to run the request.
   *
   * @param restRequest a {@link RestRequest}
   * @param <ID_T>      the type of identifier used to locate the deserializedContent for the query.
   *
   * @return the type specified feedback object for this task.
   */
  private <X, ID_T> Feedback<X> performQuery(RestRequest<X, ID_T> restRequest, Class<X> clazz, TypeReference typeReference) {

    debugIf(() -> "Perform Query: " + restRequest.getRequestUri());
    // used for effectively final scope rules

    Feedback<X> feedback = new Feedback<>();
    stereoHttpClient.httpQuery(restRequest.getHost(), restRequest.getPort(), restRequest.getRequestMethod(),
                               restRequest.getRequestUri(), (stereoRequest) -> {
          final long start = System.currentTimeMillis();

          stereoRequest.map(response -> {
            debugIf(() -> "Stereo Response: " + response.getContent());

            final int statusCode = response.getRawHttpResponse().getStatusLine().getStatusCode();

            // all 2xx
            if (statusCode == HttpStatus.SC_OK || (String.valueOf(statusCode).startsWith("20"))) {
              ObjectMapper mapper = new ObjectMapper();
              try {

                X value;
                if(clazz != null) {
                  value=mapper.readValue(response.getContent(), clazz);
                } else if(typeReference != null) {
                  value = mapper.readValue(response.getContent(), typeReference);
                } else {
                  value = null;
                }
                debugIf(() -> "Stereo read deserializedContent to type: " + feedback.getDeserializedContent()
                    .getClass()
                    .getName());
                feedback.setSuccess(statusCode, value, response.getContent());
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
    debugIf(() -> "Waiting on deserializedContent to appear: " + timeout + " ms");

    try {
      feedback.await();
    } catch (InterruptedException ex) {
      feedback.setError(HttpStatus.SC_INTERNAL_SERVER_ERROR, "{}",
                        new StereoRequestException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Request Interrupted", ex));
    }
    debugIf(() -> "Stereo Latency: (min " + MIN_LATENCY.get() + ", max " + MAX_LATENCY.get() + ")");

    // user should have data here, or null
    return feedback;
  }

}