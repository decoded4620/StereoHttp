# StereoHttp
Non Blocking Http Client implementation based on Apache NIO

Be sure to check out the docs
https://decoded4620.github.io/StereoHttp/docs/javadoc/


# Usage
The easest way is to create a `StereoHttpRequest` using a `StereoHttpRequest.Builder`, and then create a new `StereoHttpTask`, passing the request, and `StereoHttpClient` instance in for execution. This assumes that the url you hit returns JSON serialized data that can be Deserialized into the provided type, e.g. User in the example below.

e.g.
```java
 // timeout value
 private int timeout = 2000;
 // inject the client
 @Inject
 private StereoHttpClient httpClient;

 // Start the client (once)
 httpClient.start();

 // make requests with the same client (over and over)
 StereoHttpRequest.Builder<User> requestBuilder = new StereoHttpRequest.Builder<>(User.class)
        .setRequestMethod(RequestMethod.GET)
        .setHost("mywebsite-api.com")
        .setPort(80)
        .setRequestUri("/api/users/get?id=123");

// the task that will execute the request, and map the response.
StereoHttpTask<User> task = new StereoHttpTask<>(User.class, stereoHttpClient, timeout);
task.execute(requestBuilder.build());
```
