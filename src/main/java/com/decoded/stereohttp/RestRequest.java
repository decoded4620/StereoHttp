package com.decoded.stereohttp;

import com.google.common.collect.ImmutableSet;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * RestRequest That can be passed to {@link StereoHttpTask} of type T
 *
 * @param <T> the model type we are requesting.
 */
public class RestRequest<T, ID_T> {
  private Logger LOG = LoggerFactory.getLogger(RestRequest.class);

  private final String host;
  private final int port;
  private final RequestMethod requestMethod;
  private final String requestPath;
  private final Set<ID_T> identifiers;
  private final List<Pair<String, String>> requestParams;

  /**
   * Constructor
   *
   * @param builder a {@link Builder} of type T
   */
  public RestRequest(Builder<T, ID_T> builder) {
    identifiers = builder.identifiers;
    host = builder.host;
    port = builder.port;
    requestMethod = builder.requestMethod;
    requestPath = builder.requestPath;
    requestParams = builder.requestParams;
  }

  /**
   * Returns the request parameters.
   *
   * @return a Map of string parameter names to string parameter values.
   */
  public List<Pair<String, String>> getRequestParams() {
    return requestParams;
  }

  /**
   * The request URI, including params.
   *
   * @return a String
   */
  public String getRequestPath() {
    return requestPath;
  }

  /**
   * Get Request Uri
   * @return String
   */
  public String getRequestUri() {
    return requestPath + getRequestParameters();
  }

  private String getRequestParameters() {
    if (!requestParams.isEmpty()) {
      StringBuilder paramsBuilder = new StringBuilder("?");
      requestParams.forEach(pair -> {
        if (paramsBuilder.length() > 1) {
          paramsBuilder.append("&");
        }
        String correctedV;
        try {
           correctedV = URLEncoder.encode(pair.getValue(), Charset.defaultCharset().name());
        } catch (UnsupportedEncodingException ex) {
          LOG.warn("Key " + pair.getKey() + " could not be encoded!", ex);
          correctedV = "";
        }
        paramsBuilder.append(pair.getKey()).append("=").append(correctedV);
      });

      return paramsBuilder.toString();
    }
    return "";
  }

  /**
   * {@link RequestMethod} for the request.
   *
   * @return RequestMethod
   */
  public RequestMethod getRequestMethod() {
    return requestMethod;
  }

  /**
   * The Host.
   *
   * @return a String.
   */
  public String getHost() {
    return host;
  }

  /**
   * The port
   *
   * @return an int.
   */
  public int getPort() {
    return port;
  }

  /**
   * The Request Identifiers.
   *
   * @return a set of one or more identifiers used to identify a rest resource entity
   */
  public Set<ID_T> getIdentifiers() {
    return identifiers;
  }

  /**
   * The RestRequest Builder
   *
   * @param <T> the type returned by executing the request.
   */
  public static class Builder<T, ID_T> {
    private String host;
    private int port;
    private String requestPath;
    private List<Pair<String, String>> requestParams = Collections.emptyList();
    private RequestMethod requestMethod = RequestMethod.GET;
    private Set<ID_T> identifiers = Collections.emptySet();
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

    public Builder<T, ID_T> setRequestParams(List<Pair<String, String>> requestParams) {
      this.requestParams = requestParams;
      return this;
    }

    public Builder<T, ID_T> setRequestPath(String requestPath) {
      this.requestPath = requestPath;
      return this;
    }

    public Builder<T, ID_T> setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder<T, ID_T> setRequestMethod(RequestMethod requestMethod) {
      this.requestMethod = requestMethod;
      return this;
    }

    public Builder<T, ID_T> setIdentifier(ID_T identifier) {
      this.identifiers = Collections.singleton(identifier);
      return this;
    }

    public Builder<T, ID_T> setIdentifierBatch(Set<ID_T> identifiers) {
      this.identifiers = ImmutableSet.copyOf(identifiers);
      return this;
    }

    public RestRequest<T, ID_T> build() {
      return new RestRequest<>(this);
    }
  }
}
