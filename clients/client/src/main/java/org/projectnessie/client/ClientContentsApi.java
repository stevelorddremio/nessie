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
package org.projectnessie.client;

import javax.validation.constraints.NotNull;

import org.projectnessie.api.ContentsApi;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.MultiGetContentsRequest;
import org.projectnessie.model.MultiGetContentsResponse;

class ClientContentsApi implements ContentsApi {

  private final HttpClient client;

  public ClientContentsApi(HttpClient client) {
    this.client = client;
  }

  @Override
  public Contents getContents(@NotNull ContentsKey key, String ref) throws NessieNotFoundException {
    return client.newRequest().path("contents").path(key.toPathString())
                 .queryParam("ref", ref)
                 .get()
                 .readEntity(Contents.class);
  }

  @Override
  public MultiGetContentsResponse getMultipleContents(@NotNull String ref, @NotNull MultiGetContentsRequest request)
      throws NessieNotFoundException {
    return client.newRequest().path("contents")
                 .queryParam("ref", ref)
                 .post(request)
                 .readEntity(MultiGetContentsResponse.class);
  }


  @Override
  public void setContents(@NotNull ContentsKey key, String branch, @NotNull String hash, String message,
                          @NotNull Contents contents) throws NessieNotFoundException, NessieConflictException {
    client.newRequest().path("contents").path(key.toPathString())
          .queryParam("branch", branch)
          .queryParam("hash", hash)
          .queryParam("message", message)
          .post(contents);
  }

  @Override
  public void deleteContents(ContentsKey key, String branch, String hash, String message)
      throws NessieNotFoundException, NessieConflictException {
    client.newRequest().path("contents").path(key.toPathString())
          .queryParam("branch", branch)
          .queryParam("hash", hash)
          .queryParam("message", message)
          .delete();
  }
}
