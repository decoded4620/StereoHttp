package com.decoded.stereohttp;

import com.google.common.collect.ImmutableSet;
import com.decoded.javautil.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.*;


/**
 * RestRequest That can be passed to {@link StereoHttpTask} of type T
 *
 * @param <T> the model type we are requesting.
 */
public class RestRequest<T, ID_T> {
  private static final Logger LOG = LoggerFactory.getLogger(RestRequest.class);

  private final String host;
  private final int port;
  private final RequestMethod requestMethod;
  private final String body;
  private final String requestPath;
  private final Set<ID_T> identifiers;
  private final List<Pair<String, String>> requestParams;
  private final List<Pair<String, String>> formData;
  private final List<Pair<String, String>> urlEncodedFormData;
  private final List<Pair<String, String>> cookies;
  private final Map<String, List<String>> headers;
  private final boolean secure;
  /**
   * Constructor
   *
   * @param builder a {@link Builder} of type T
   */
  public RestRequest(Builder<T, ID_T> builder) {
    identifiers = builder.identifiers;
    headers = builder.headers;
    host = builder.host;
    port = builder.port;
    body = builder.body;
    requestMethod = builder.requestMethod;
    requestPath = builder.requestPath;
    requestParams = builder.requestParams;
    formData = builder.formData;
    urlEncodedFormData = builder.urlEncodedFormData;
    cookies = builder.cookies;
    secure = builder.secure;
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
   * Returns the list of cookies.
   * @return a list of name of value pair.
   */
  public List<Pair<String, String>> getCookies() {
    return cookies;
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
   * Returns <code>true</code> for secure requests.
   * @return a boolean
   */
  public boolean isSecure() {
    return secure;
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
   * Returns the body of the request.
   * @return a String.
   */
  public String getBody() {
    return body;
  }

  /**
   * Form Data
   * @return a set of form elements with name value pairs.
   */
  public List<Pair<String, String>> getFormData() {
    return formData;
  }

  /**
   * The form data if the content is url encoded form data.
   * @return a list of Pair of strings
   */
  public List<Pair<String, String>> getUrlEncodedFormData() {
    return urlEncodedFormData;
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
   * the headers
   * @return map of headers.
   */
  public Map<String, List<String>> getHeaders() {
    return headers;
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
    private String body;
    private int port;
    private String requestPath;
    private List<Pair<String, String>> requestParams = new ArrayList<>();
    private List<Pair<String, String>> formData = new ArrayList<>();
    private List<Pair<String, String>> urlEncodedFormData = new ArrayList<>();
    private Map<String, List<String>> headers = new HashMap<>();
    private List<Pair<String, String>> cookies = new ArrayList<>();
    private boolean secure;
    private RequestMethod requestMethod = RequestMethod.GET;
    private Set<ID_T> identifiers = Collections.emptySet();
    private Class<T> tClass;
    private Class<ID_T> idClass;

    public Builder(Class<T> tClazz, Class<ID_T> idClazz) {
      this.tClass = tClazz;
      this.idClass = idClazz;
    }

    /**
     * Sets the request body
     * @param body the body content
     * @return this builder
     */
    public Builder<T, ID_T> setBody(final String body) {
      this.body = body;
      return this;
    }

    /**
     * Sets the request cookies
     * @param cookie the cookie to set
     * @return this builder
     */
    public Builder<T, ID_T> setCookie(Pair<String, String> cookie) {
      this.cookies.add(cookie);
      return this;
    }

    /**
     * Sets the form data for the request for Multipart forms.
     * @param formDataItem multipart form data item
     * @return this builder
     */
    public Builder<T, ID_T> setFormDataItem(Pair<String, String> formDataItem) {
      this.formData.add(formDataItem);
      return this;
    }

    /**
     * Set the url encoded form data.
     * @param urlEncodedFormDataItem the url encoded form data item
     * @return Builder
     */
    public Builder<T, ID_T> setUrlEncodedFormDataItem(Pair<String, String> urlEncodedFormDataItem) {
      this.urlEncodedFormData.add(urlEncodedFormDataItem);
      return this;
    }

    /**
     * sets the host
     * @param host the host url
     * @return this builder
     */
    public Builder<T, ID_T> setHost(String host) {
      this.host = host;
      return this;
    }

    /**
     * Add a single param to the request params.
     * @param paramName the name of the param
     * @param paramValue the value of the param
     * @return this builder.
     */
    public Builder<T, ID_T> addRequestParam(final String paramName, final String paramValue) {
      this.requestParams.add(new Pair<>(paramName, paramValue));
      return this;
    }

    /**
     * Add or overwrite a specified header
     * @param headerName the header name
     * @param headerValue the value
     * @return this builder.
     */
    public Builder<T, ID_T> addHeader(final String headerName, final String headerValue) {
      if(this.headers.containsKey(headerName)) {
        LOG.warn("Overwrite header: " + headerName);
      }
      this.headers.computeIfAbsent(headerName, h -> new ArrayList<>()).add(headerValue);
      return this;
    }

    /**
     * Set the request path (after the uri)
     * @param requestPath the path of the request
     * @return this builder
     */
    public Builder<T, ID_T> setRequestPath(String requestPath) {
      this.requestPath = requestPath;
      return this;
    }

    /**
     * Set The port for the url
     * @param port a port
     * @return this builder
     */
    public Builder<T, ID_T> setPort(int port) {
      this.port = port;
      return this;
    }

    /**
     * The {@link RequestMethod}
     * @param requestMethod a method
     * @return this builder
     */
    public Builder<T, ID_T> setRequestMethod(RequestMethod requestMethod) {
      this.requestMethod = requestMethod;
      return this;
    }

    /**
     * Set the identifier
     * @param identifier the id
     * @return this builder
     */
    public Builder<T, ID_T> setIdentifier(ID_T identifier) {
      this.identifiers = Collections.singleton(identifier);
      return this;
    }

    /**
     * Sets a batch of identifiers
     * @param identifiers set of identfiers
     * @return this builder
     */
    public Builder<T, ID_T> setIdentifierBatch(Set<ID_T> identifiers) {
      this.identifiers = ImmutableSet.copyOf(identifiers);
      return this;
    }

    /**
     * Set the security of the request (https vs. http)
     * @param secure true if secure
     * @return this builder.
     */
    public Builder<T, ID_T> setSecure(final boolean secure) {
      this.secure = secure;
      return this;
    }

    /**
     * Builds a rest request.
     * @return a {@link RestRequest} of type T / ID_T
     */
    public RestRequest<T, ID_T> build() {
      return new RestRequest<>(this);
    }
  }
}
