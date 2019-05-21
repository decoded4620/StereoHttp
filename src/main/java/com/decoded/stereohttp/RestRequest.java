package com.decoded.stereohttp;

import java.util.function.Consumer;


/**
 * RestRequest That can be passed to {@link StereoHttpTask} of type T
 *
 * @param <T> the model type we are requesting.
 */
public class RestRequest<T, ID_T> {
  private final String host;
  private final int port;
  private final RequestMethod requestMethod;
  private final String requestUri;
  private final Consumer<T> resultConsumer;
  private ID_T urn;
  /**
   * Constructor
   *
   * @param builder a {@link Builder} of type T
   */
  public RestRequest(Builder<T, ID_T> builder) {
    urn = builder.urn;
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
   * The Request Urn.
   * @return the Urn
   */
  public ID_T getUrn() {
    return urn;
  }

  /**
   * The RestRequest Builder
   * @param <T> the type returned by executing the request.
   */
  public static class Builder<T, ID_T> {
    private String host;
    private int port;
    private String requestUri;
    private RequestMethod requestMethod;
    private Consumer<T> resultConsumer;
    private ID_T urn;
    private Class<T> tClass;
    private Class<ID_T> idClass;

    public Builder(Class<T> tClazz, Class<ID_T> idClazz) {
      this.tClass = tClazz;
      this.idClass = idClazz;
    }

    public Builder<T, ID_T> setHost(String host) {
      this.host = host;
      return this;
    }

    public Builder<T, ID_T> setRequestUri(String requestUri) {
      this.requestUri = requestUri;
      return this;
    }

    public Builder<T, ID_T> setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder<T, ID_T> setResultConsumer(Consumer<T> resultConsumer) {
      this.resultConsumer = resultConsumer;
      return this;
    }

    public Builder<T, ID_T> setRequestMethod(RequestMethod requestMethod) {
      this.requestMethod = requestMethod;
      return this;
    }

    public Builder<T, ID_T> setUrn(ID_T urn) {
      this.urn = urn;
      return this;
    }
    public RestRequest<T, ID_T> build() {
      return new RestRequest<>(this);
    }
  }
}
