package com.decoded.stereohttp;

import org.apache.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Optional;


class StereoHttpUtils {
  private static final Logger LOG = LoggerFactory.getLogger(StereoHttpUtils.class);

  /**
   * Utility to extract Http response content from an apache HttpResponse
   *
   * @param httpResponse an HttpResponse.
   * @return Optional String
   */
  static String getContent(HttpResponse httpResponse) {
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
}
