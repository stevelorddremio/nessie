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

import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.projectnessie.api.TreeApi;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Merge;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.model.Transplant;

import com.fasterxml.jackson.core.type.TypeReference;

class ClientTreeApi implements TreeApi {

  private static final TypeReference<List<Reference>> REFERENCE_LIST = new TypeReference<List<Reference>>() {
  };

  private final HttpClient client;

  public ClientTreeApi(HttpClient client) {
    this.client = client;
  }

  @Override
  public List<Reference> getAllReferences() {
    return client.newRequest().path("trees").get().readEntity(REFERENCE_LIST);
  }

  @Override
  public void createReference(@NotNull Reference reference)
      throws NessieNotFoundException, NessieConflictException {
    client.newRequest().path("trees/tree").post(reference);
  }

  @Override
  public Reference getReferenceByName(@NotNull String refName) throws NessieNotFoundException {
    return client.newRequest().path("trees/tree/{ref}").resolveTemplate("ref", refName).get().readEntity(Reference.class);
  }

  @Override
  public void assignTag(@NotNull String tagName, @NotNull String expectedHash, @NotNull Tag tag)
      throws NessieNotFoundException, NessieConflictException {
    client.newRequest().path("trees/tag/{tagName}")
          .resolveTemplate("tagName", tagName)
          .queryParam("expectedHash", expectedHash)
          .put(tag);
  }

  @Override
  public void deleteTag(@NotNull String tagName, @NotNull String expectedHash) throws NessieConflictException, NessieNotFoundException {
    client.newRequest().path("trees/tag/{tagName}")
          .resolveTemplate("tagName", tagName)
          .queryParam("expectedHash", expectedHash)
          .delete();
  }

  @Override
  public void assignBranch(@NotNull String branchName, @NotNull String expectedHash,
                           @NotNull Branch branch) throws NessieNotFoundException, NessieConflictException {
    client.newRequest().path("trees/branch/{branchName}")
          .resolveTemplate("branchName", branchName)
          .queryParam("expectedHash", expectedHash)
          .put(branch);
  }

  @Override
  public void deleteBranch(@NotNull String branchName, @NotNull String expectedHash)
      throws NessieConflictException, NessieNotFoundException {
    client.newRequest().path("trees/branch/{branchName}")
          .resolveTemplate("branchName", branchName)
          .queryParam("expectedHash", expectedHash)
          .delete();
  }

  @Override
  public Branch getDefaultBranch() {
    return client.newRequest().path("trees/tree").get().readEntity(Branch.class);
  }

  @Override
  public LogResponse getCommitLog(@NotNull String ref) throws NessieNotFoundException {
    return client.newRequest().path("trees/tree/{ref}/log").resolveTemplate("ref", ref)
                 .get()
                 .readEntity(LogResponse.class);
  }

  @Override
  public void transplantCommitsIntoBranch(
      @NotNull String branchName,
      @NotNull String expectedHash,
      String message,
      @Valid Transplant transplant)
      throws NessieNotFoundException, NessieConflictException {
    client.newRequest().path("trees/branch/{branchName}/transplant")
          .resolveTemplate("branchName", branchName)
          .queryParam("expectedHash", expectedHash)
          .queryParam("message", message)
          .post(transplant);
  }

  @Override
  public void mergeRefIntoBranch(
      @NotNull String branchName,
      @NotNull String expectedHash,
      @NotNull @Valid Merge merge)
      throws NessieNotFoundException, NessieConflictException {
    client.newRequest().path("trees/branch/{branchName}/merge")
          .resolveTemplate("branchName", branchName)
          .queryParam("expectedHash", expectedHash)
          .post(merge);
  }

  @Override
  public EntriesResponse getEntries(@NotNull String refName) throws NessieNotFoundException {
    return client.newRequest().path("trees/tree/{ref}/entries")
                 .resolveTemplate("ref", refName)
                 .get()
                 .readEntity(EntriesResponse.class);
  }

  @Override
  public void commitMultipleOperations(
      String branch,
      @NotNull String expectedHash,
      String message,
      @NotNull Operations operations) throws NessieNotFoundException, NessieConflictException {
    client.newRequest().path("trees/branch/{branchName}/commit")
          .resolveTemplate("branchName", branch)
          .queryParam("expectedHash", expectedHash)
          .queryParam("message", message)
          .post(operations);
  }
}
