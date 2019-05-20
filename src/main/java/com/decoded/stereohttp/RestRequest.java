package com.decoded.stereohttp;

import java.util.function.Consumer;


/**
 * RestRequest That can be passed to {@link StereoHttpTask} of type T
 *
 * @param <T> the model type we are requesting.
 */
public class RestRequest<T> {
  private final String host;
  private final int port;
  private final RequestMethod requestMethod;
  private final String requestUri;
  private final Consumer<T> resultConsumer;

  /**
   * Constructor
   *
   * @param builder a {@link Builder} of type T
   */
  public RestRequest(Builder<T> builder) {
    host = builder.host;
    port = builder.port;
    requestMethod = builder.requestMethod;
    resultConsumer = builder.resultConsumer;
    requestUri = builder.requestUri;
  }

  /**
   * Consume the result upon receipt.
   * @return a Consumer of T
   */
  public Consumer<T> getResultConsumer() {
    return resultConsumer;
  }

  /**
   * The request URI, including params.
   * @return a String
   */
  public String getRequestUri() {
    return requestUri;
  }

  /**
   * {@link RequestMethod} for the request.
   * @return RequestMethod
   */
  public RequestMethod getRequestMethod() {
    return requestMethod;
  }

  /**
   * The Host.
   * @return a String.
   */
  public String getHost() {
    return host;
  }

  /**
   * The port
   * @return an int.
   */
  public int getPort() {
    return port;
  }

  /**
   * The RestRequest Builder
   * @param <T> the type returned by executing the request.
   */
  public static class Builder<T> {
    private String host;
    private int port;
    private String requestUri;
    private RequestMethod requestMethod;
    private Consumer<T> resultConsumer;

    private Class<T> tClass;

    public Builder(Class<T> tClazz) {
      this.tClass = tClazz;
    }

    public Builder<T> setHost(String host) {
      this.host = host;
      return this;
    }

    public Builder<T> setRequestUri(String requestUri) {
      this.requestUri = requestUri;
      return this;
    }

    public Builder<T> setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder<T> setResultConsumer(Consumer<T> resultConsumer) {
      this.resultConsumer = resultConsumer;
      return this;
    }

    public Builder<T> setRequestMethod(RequestMethod requestMethod) {
      this.requestMethod = requestMethod;
      return this;
    }

    public RestRequest<T> build() {
      return new RestRequest<>(this);
    }
  }
}
