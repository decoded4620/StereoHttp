package com.decoded.stereohttp;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;


/**
 * Http Task of type T
 *
 * @param <T> the type to fetch with this task.
 */
public class StereoHttpTask<T> {
  private static final Logger LOG = LoggerFactory.getLogger(StereoHttpTask.class);

  private static AtomicLong maxLatency = new AtomicLong(Integer.MIN_VALUE);
  private static AtomicLong minLatency = new AtomicLong(Integer.MAX_VALUE);

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

  /**
   * Execute the restRequest.
   *
   * @param restRequest the restRequest
   * @param <ID_T>      the identifier type for the request. Used as a pass through here.
   *
   * @return CompletableFuture of T
   */
  public <ID_T> CompletableFuture<T> execute(RestRequest<T, ID_T> restRequest) {
    debugIf(() -> "execute(): " + timeout + " ms");
    return CompletableFuture.supplyAsync(() -> performQuery(restRequest), stereoHttpClient.getExecutorService());
  }

  private <ID_T> T performQuery(RestRequest<T, ID_T> restRequest) {

    debugIf(() -> "Perform Query: " + restRequest.getRequestUri());
    // used for effectively final scope rules

    ValueHolder<T> valueHolder = new ValueHolder<>();
    stereoHttpClient.httpQuery(restRequest.getHost(), restRequest.getPort(), restRequest.getRequestMethod(),
                               restRequest.getRequestUri(), (stereoRequest) -> {
          final long start = System.currentTimeMillis();

          Runnable updateDelta = () -> {
            long delta = System.currentTimeMillis() - start;
            if (delta < minLatency.get()) {
              LOG.warn("Min latency improved!! " + delta);
              minLatency.set(delta);
            }

            if (delta > maxLatency.get()) {
              LOG.warn("Max latency degradation!!: " + delta);
              maxLatency.set(delta);
            }
            valueHolder.countDown();
          };

          stereoRequest.map(response -> {
            debugIf(() -> "Stereo Response: " + response.getContent());
            ObjectMapper mapper = new ObjectMapper();
            try {
              valueHolder.value = mapper.readValue(response.getContent(), tClass);
              debugIf(() -> "Stereo read content to type: " + valueHolder.value.getClass().getName());
              updateDelta.run();
            } catch (Exception ex) {
              valueHolder.exception = ex;
              updateDelta.run();
            }
          }).exceptionally(ex -> {
            valueHolder.exception = ex;
            updateDelta.run();
          }).cancelling(() -> {
            valueHolder.wasCancelled = true;
            updateDelta.run();
            LOG.warn("Cancelled the restRequest");
          }).andThen(() -> {
            debugIf(() -> "Completion mapper");
          });
        });
    debugIf(() -> "Waiting on value to appear: " + timeout + " ms");

    try {
      valueHolder.await();
    } catch (InterruptedException ex) {
      valueHolder.exception = ex;
    }

    if (valueHolder.exception != null) {
      LOG.error("Failed to acquire value: ", valueHolder.exception);
      throw new RuntimeException(valueHolder.exception);
    }

    debugIf(() -> "Stereo Latency: (min " + minLatency.get() + ", max " + maxLatency.get() + ")");
    // user should have data here.
    return valueHolder.value;
  }

  /**
   * This contains a value reference that can be set in lambda scope.
   *
   * @param <T>
   */
  private static final class ValueHolder<T> extends CountDownLatch {
    T value;
    Throwable exception;
    boolean wasCancelled;

    public ValueHolder() {
      super(1);
    }

    @Override
    public void countDown() {
      debugIf(() -> "Count Down from: " + this.getCount());
      super.countDown();
    }
  }
}