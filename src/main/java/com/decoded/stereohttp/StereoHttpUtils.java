package com.decoded.stereohttp;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


public class StereoHttpUtils {
  private static final Logger LOG = LoggerFactory.getLogger(StereoHttpUtils.class);

  /**
   * Utility to extract Http response content from an apache HttpResponse
   *
   * @param httpResponse an HttpResponse.
   * @return Optional String
   */
  public static String getContent(HttpResponse httpResponse) {
    String content = "{}";
    try {
      InputStream stream = httpResponse.getEntity().getContent();
      if (stream.available() > 0) {
        InputStreamReader reader = new InputStreamReader(stream);
        int data = reader.read();

        StringBuilder builder = new StringBuilder("" + (char) data);
        while (data != -1) {
          data = reader.read();
          builder.append((char) data);
        }
        reader.close();
        content = builder.toString();
      }
    } catch (IOException ex) {
      LOG.error("Could not get content", ex);
    }

    return content;
  }

  public static void copyFormData(StereoHttpRequest<?, ?> fromStereoRequest, HttpEntityEnclosingRequest toHttpRequest) {
    // copy form data
    if (!fromStereoRequest.getFormData().isEmpty()) {
      final MultipartEntityBuilder builder = MultipartEntityBuilder.create();
      fromStereoRequest.getFormData().forEach(pair -> builder.addTextBody(pair.getKey(), pair.getValue()));
      toHttpRequest.setEntity(builder.build());
    } else if (!fromStereoRequest.getUrlEncodedFormData().isEmpty()) {
      List<BasicNameValuePair> pairs = new ArrayList<>();
      fromStereoRequest.getUrlEncodedFormData()
          .forEach(pair -> pairs.add(new BasicNameValuePair(pair.getKey(), pair.getValue())));
      try {
        UrlEncodedFormEntity entity = new UrlEncodedFormEntity(pairs);
        toHttpRequest.setEntity(entity);
      } catch (UnsupportedEncodingException ex) {
        LOG.warn("Not supported url encoded form data", ex);
      }
    } else {
      Optional.ofNullable(fromStereoRequest.getBody()).ifPresent(serializedEntity -> {
        try {
          if (StringUtils.isEmpty(serializedEntity)) {
            LoggingUtil.infoIf(LOG, () -> "No Entity Data...");
            toHttpRequest.setEntity(new StringEntity(""));
          } else {
            LoggingUtil.infoIf(LOG, () -> "Submitting Entity Data: " + serializedEntity.getBytes().length + " bytes");
            toHttpRequest.setEntity(new StringEntity(serializedEntity));
          }
        } catch (UnsupportedEncodingException ex) {
          LOG.warn("Not supported!", ex);
        }
      });
    }
  }

  /**
   * Copy headers and cookies from a {@link StereoHttpRequest} to an apache {@link HttpRequest} during request
   * creation.
   * @param fromStereoRequest the stereo request which contains headers and cookies to copy.
   * @param toHttpRequest the http request to copy the cookies and headers over to.
   */
  public static void copyHeadersAndCookies(StereoHttpRequest<?, ?> fromStereoRequest, HttpRequest toHttpRequest) {
    // copy headers
    fromStereoRequest.getHeaders()
        .forEach((headerName, headerValueList) -> headerValueList.forEach(
            headerValue -> toHttpRequest.addHeader(headerName, headerValue)));

    // copy cookies
    fromStereoRequest.getCookies()
        .forEach(cookiePair -> toHttpRequest.addHeader("Cookie", cookiePair.getKey() + "=" + cookiePair.getValue()));
  }
}
