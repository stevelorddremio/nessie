/*
 * Copyright (C) 2020 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.projectnessie.client.auth;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.RequestFilter;

import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

public class AwsAuth implements RequestFilter {

  private final ObjectMapper objectMapper;
  private final Aws4Signer signer;
  private final AwsCredentialsProvider awsCredentialsProvider;
  private final Region region = Region.US_WEST_2;

  /**
   * Construct AWS signer filter.
   */
  public AwsAuth(ObjectMapper objectMapper) {
    this.awsCredentialsProvider = DefaultCredentialsProvider.create();
    this.signer = Aws4Signer.create();
    this.objectMapper = objectMapper;
  }

  private SdkHttpFullRequest prepareRequest(String url, HttpClient.Method method, Optional<Object> entity) {
    try {
      URI uri = new URI(url);
      SdkHttpFullRequest.Builder builder = SdkHttpFullRequest.builder()
                                                             .uri(uri)
                                                             .method(SdkHttpMethod.fromValue(method.name()));

      Arrays.stream(uri.getQuery().split("&")).map(s -> s.split("=")).forEach(s -> builder.putRawQueryParameter(s[0], s[1]));

      if (entity.isPresent()) {
        try {
          byte[] bytes = objectMapper.writeValueAsBytes(entity.get());
          builder.contentStreamProvider(() -> new ByteArrayInputStream(bytes));
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }
      return builder.build();
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  @Override
  public void filter(RequestContext context) {
    SdkHttpFullRequest modifiedRequest =
        signer.sign(prepareRequest(context.getUri(), context.getMethod(), context.getBody()),
                  Aws4SignerParams.builder()
                                  .signingName("execute-api")
                                  .awsCredentials(awsCredentialsProvider.resolveCredentials())
                                  .signingRegion(region)
                                  .build());
    for (Map.Entry<String, List<String>> entry : modifiedRequest.toBuilder().headers().entrySet()) {
      if (context.getHeaders().containsKey(entry.getKey())) {
        continue;
      }
      entry.getValue().forEach(a -> context.putHeader(entry.getKey(), a));
    }
  }
}
