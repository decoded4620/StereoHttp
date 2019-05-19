package com.decoded.stereohttp;

public class Http {
  /**
   * Schemes for calling remote hosts with default ports
   */
  public enum Scheme {
    HTTP("http", 80),
    HTTPS("https", 443),
    FTP("ftp", 21),
    SFTP("sftp", 22),
    SSH("ssh", 22);

    private String value;
    private int defaultPort;

    Scheme(String value, int defaultPort) {
      this.value = value;
      this.defaultPort = defaultPort;
    }

    public int getDefaultPort() {
      return defaultPort;
    }

    public String getProtocol() {
      return value + "://";
    }

    public String getValue() {
      return value;
    }
  }
}
