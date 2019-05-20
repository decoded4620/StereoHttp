package com.decoded.stereohttp;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


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

  /**
   * Execute the restRequest.
   *
   * @param restRequest the restRequest
   * @return CompletableFuture
   */
  public CompletableFuture<T> execute(RestRequest<T> restRequest) {
    return CompletableFuture.supplyAsync(() -> {
      long start = System.currentTimeMillis();
      // used for effectively final scope rules
      ValueHolder<T> valueHolder = new ValueHolder<>();
      stereoHttpClient.httpQuery(restRequest.getHost(),
                                 restRequest.getPort(),
                                 restRequest.getRequestMethod(),
                                 restRequest.getRequestUri(), (stereoRequest) -> {
            stereoRequest.map(response -> {
              ObjectMapper mapper = new ObjectMapper();
              try {
                valueHolder.value = mapper.readValue(response.getContent(), tClass);
                restRequest.getResultConsumer().accept(valueHolder.value);
              } catch (JsonParseException ex) {
                LOG.error("Parse Failure: ", ex);
              } catch (IOException ex) {
                LOG.error("IOException", ex);
              }
              valueHolder.countDown();
            }).exceptionally(ex -> {
              LOG.error("Could not load data", ex);
              valueHolder.countDown();
            }).cancelling(() -> {
              LOG.warn("Cancelled the restRequest");
              valueHolder.countDown();
            }).andThen(() -> {

            });
          });
      try {
        valueHolder.await(timeout, TimeUnit.MILLISECONDS);
        long delta = System.currentTimeMillis() - start;
        if(delta < minLatency.get()) {
          minLatency.set(delta);
        }

        if(delta > maxLatency.get()) {
          maxLatency.set(delta);
        }
      } catch (InterruptedException ex) {
        throw new RuntimeException("Failed", ex);
      }
      // user should have data here.
      return valueHolder.value;
    });
  }

  /**
   * This contains a value reference that can be set in lambda scope.
   * @param <T>
   */
  private static final class ValueHolder<T> extends CountDownLatch {
    T value;

    public ValueHolder() {
      super(1);
    }
  }
}