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

package org.projectnessie.server;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.projectnessie.model.Validation.HASH_MESSAGE;
import static org.projectnessie.model.Validation.REF_NAME_MESSAGE;
import static org.projectnessie.model.Validation.REF_NAME_OR_HASH_MESSAGE;
import static org.projectnessie.server.ReferenceMatchers.referenceWithNameAndType;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.TreeApi;
import org.projectnessie.client.NessieClient;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.rest.NessieBadRequestException;
import org.projectnessie.client.rest.NessieHttpResponseFilter;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableMerge;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.ImmutablePut;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.MultiGetContentsRequest;
import org.projectnessie.model.MultiGetContentsResponse.ContentsWithKey;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
class TestRest {

  public static final String COMMA_VALID_HASH_1 = ",1234567890123456789012345678901234567890123456789012345678901234";
  public static final String COMMA_VALID_HASH_2 = ",1234567890123456789012345678901234567890";
  public static final String COMMA_VALID_HASH_3 = ",1234567890123456";

  private NessieClient client;
  private TreeApi tree;
  private ContentsApi contents;
  private HttpClient httpClient;

  @BeforeEach
  void init() {
    URI uri = URI.create("http://localhost:19121/api/v1");
    client = NessieClient.builder().withUri(uri).build();
    tree = client.getTreeApi();
    contents = client.getContentsApi();

    ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    httpClient = HttpClient.builder().setBaseUri(uri).setObjectMapper(mapper).build();
    httpClient.register(new NessieHttpResponseFilter(mapper));
  }

  @AfterEach
  void closeClient() {
    client.close();
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "normal",
      "with-no_space",
      "slash/thing"
  })
  void referenceNames(String refNamePart) throws NessieNotFoundException, NessieConflictException {
    String tagName = "tag" + refNamePart;
    String branchName = "branch" + refNamePart;
    String branchName2 = "branch2" + refNamePart;

    String someHash = tree.getReferenceByName("main").getHash();

    tree.createReference(Tag.of(tagName, someHash));
    tree.createReference(Branch.of(branchName, someHash));
    tree.createReference(Branch.of(branchName2, someHash));

    Map<String, Reference> references = tree.getAllReferences().stream()
        .filter(r -> "main".equals(r.getName()) || r.getName().endsWith(refNamePart))
        .collect(Collectors.toMap(Reference::getName, Function.identity()));
    assertThat(references.values(), containsInAnyOrder(
        referenceWithNameAndType("main", Branch.class),
        referenceWithNameAndType(tagName, Tag.class),
        referenceWithNameAndType(branchName, Branch.class),
        referenceWithNameAndType(branchName2, Branch.class)));

    Reference tagRef = references.get(tagName);
    Reference branchRef = references.get(branchName);
    Reference branchRef2 = references.get(branchName2);

    String tagHash = tagRef.getHash();
    String branchHash = branchRef.getHash();
    String branchHash2 = branchRef2.getHash();

    assertThat(tree.getReferenceByName(tagName), equalTo(tagRef));
    assertThat(tree.getReferenceByName(branchName), equalTo(branchRef));

    EntriesResponse entries = tree.getEntries(tagName);
    assertThat(entries, notNullValue());
    entries = tree.getEntries(branchName);
    assertThat(entries, notNullValue());

    LogResponse log = tree.getCommitLog(tagName);
    assertThat(log, notNullValue());
    log = tree.getCommitLog(branchName);
    assertThat(log, notNullValue());

    // Need to have at least one op, otherwise all following operations (assignTag/Branch, merge, delete) will fail
    ImmutablePut op = ImmutablePut.builder().key(ContentsKey.of("some-key")).contents(IcebergTable.of("foo")).build();
    Operations ops = ImmutableOperations.builder().addOperations(op).build();
    tree.commitMultipleOperations(branchName, branchHash, "One dummy op", ops);
    log = tree.getCommitLog(branchName);
    String newHash = log.getOperations().get(0).getHash();

    tree.assignTag(tagName, tagHash, Tag.of(tagName, newHash));
    tree.assignBranch(branchName, newHash, Branch.of(branchName, newHash));

    tree.mergeRefIntoBranch(branchName2, branchHash2, ImmutableMerge.builder().fromHash(newHash).build());

    tree.deleteTag(tagName, newHash);
    tree.deleteBranch(branchName, newHash);
  }

  @Test
  void multiget() throws NessieNotFoundException, NessieConflictException {
    final String branch = "foo";
    tree.createReference(Branch.of(branch, null));
    Reference r = tree.getReferenceByName(branch);
    ContentsKey a = ContentsKey.of("a");
    ContentsKey b = ContentsKey.of("b");
    IcebergTable ta = IcebergTable.of("path1");
    IcebergTable tb = IcebergTable.of("path2");
    contents.setContents(a, branch, r.getHash(), "commit 1", ta);
    contents.setContents(b, branch, r.getHash(), "commit 2", tb);
    List<ContentsWithKey> keys =
        contents.getMultipleContents("foo", MultiGetContentsRequest.of(a, b, ContentsKey.of("noexist"))).getContents();
    List<ContentsWithKey> expected = Arrays.asList(ContentsWithKey.of(a, ta), ContentsWithKey.of(b,  tb));
    assertThat(keys, Matchers.containsInAnyOrder(expected.toArray()));
    tree.deleteBranch(branch, tree.getReferenceByName(branch).getHash());
  }

  @Test
  void checkSpecialCharacterRoundTrip() throws NessieNotFoundException, NessieConflictException {
    final String branch = "specialchar";
    tree.createReference(Branch.of(branch, null));
    Reference r = tree.getReferenceByName(branch);
    //ContentsKey k = ContentsKey.of("/%国","国.国");
    ContentsKey k = ContentsKey.of("a.b","c.d");
    IcebergTable ta = IcebergTable.of("path1");
    contents.setContents(k, branch, r.getHash(), "commit 1", ta);
    assertEquals(ContentsWithKey.of(k, ta), contents.getMultipleContents(branch, MultiGetContentsRequest.of(k)).getContents().get(0));
    assertEquals(ta, contents.getContents(k, branch));
    tree.deleteBranch(branch, tree.getReferenceByName(branch).getHash());
  }

  @Test
  void checkServerErrorPropagation() throws NessieNotFoundException, NessieConflictException {
    final String branch = "bar";
    tree.createReference(Branch.of(branch, null));
    NessieConflictException e = assertThrows(NessieConflictException.class, () -> tree.createReference(Branch.of(branch, null)));
    assertThat(e.getMessage(), Matchers.containsString("already exists"));
  }

  @ParameterizedTest
  @CsvSource({
      "x/" + COMMA_VALID_HASH_1,
      "abc'" + COMMA_VALID_HASH_1,
      ".foo" + COMMA_VALID_HASH_2,
      "abc'def'..'blah" + COMMA_VALID_HASH_2,
      "abc'de..blah" + COMMA_VALID_HASH_3,
      "abc'de@{blah" + COMMA_VALID_HASH_3
  })
  void invalidBranchNames(String invalidBranchName, String validHash) {
    Operations ops = ImmutableOperations.builder().build();
    ContentsKey key = ContentsKey.of("x");
    Contents cts = IcebergTable.of("moo");
    MultiGetContentsRequest mgReq = MultiGetContentsRequest.of(key);
    Tag tag = Tag.of("valid", validHash);
    assertAll(
        () -> assertEquals("Bad Request (HTTP/400): commitMultipleOperations.branchName: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.commitMultipleOperations(invalidBranchName, validHash, null, ops)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): deleteBranch.branchName: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.deleteBranch(invalidBranchName, validHash)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): getCommitLog.ref: " + REF_NAME_OR_HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.getCommitLog(invalidBranchName)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): getEntries.refName: " + REF_NAME_OR_HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.getEntries(invalidBranchName)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): getReferenceByName.refName: " + REF_NAME_OR_HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.getReferenceByName(invalidBranchName)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): assignTag.tagName: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.assignTag(invalidBranchName, validHash, tag)).getMessage()),
        () -> assertThat(
            assertThrows(NessieBadRequestException.class,
                () -> tree.mergeRefIntoBranch(invalidBranchName, validHash, null)).getMessage(),
            allOf(
                containsString("Bad Request (HTTP/400): "),
                containsString("mergeRefIntoBranch.branchName: " + REF_NAME_MESSAGE),
                containsString("mergeRefIntoBranch.merge: must not be null")
            )),
        () -> assertEquals("Bad Request (HTTP/400): deleteTag.tagName: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.deleteTag(invalidBranchName, validHash)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): transplantCommitsIntoBranch.branchName: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.transplantCommitsIntoBranch(invalidBranchName, validHash, null, null)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): setContents.branch: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> contents.setContents(key, invalidBranchName, validHash, null, cts)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): deleteContents.branch: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> contents.deleteContents(key, invalidBranchName, validHash, null)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): getContents.ref: " + REF_NAME_OR_HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> contents.getContents(key, invalidBranchName)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): getMultipleContents.ref: " + REF_NAME_OR_HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> contents.getMultipleContents(invalidBranchName, mgReq)).getMessage())
    );
  }

  @ParameterizedTest
  @CsvSource({
      "" + COMMA_VALID_HASH_1,
      "abc'" + COMMA_VALID_HASH_1,
      ".foo" + COMMA_VALID_HASH_2,
      "abc'def'..'blah" + COMMA_VALID_HASH_2,
      "abc'de..blah" + COMMA_VALID_HASH_3,
      "abc'de@{blah" + COMMA_VALID_HASH_3
  })
  void invalidHashes(String invalidHashIn, String validHash) {
    // CsvSource maps an empty string as null
    String invalidHash = invalidHashIn != null ? invalidHashIn : "";

    String validBranchName = "hello";
    Operations ops = ImmutableOperations.builder().build();
    ContentsKey key = ContentsKey.of("x");
    Contents cts = IcebergTable.of("moo");
    MultiGetContentsRequest mgReq = MultiGetContentsRequest.of(key);
    Tag tag = Tag.of("valid", validHash);
    assertAll(
        () -> assertEquals("Bad Request (HTTP/400): commitMultipleOperations.hash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.commitMultipleOperations(validBranchName, invalidHash, null, ops)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): deleteBranch.hash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.deleteBranch(validBranchName, invalidHash)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): assignTag.oldHash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.assignTag(validBranchName, invalidHash, tag)).getMessage()),
        () -> assertThat(
            assertThrows(NessieBadRequestException.class,
                () -> tree.mergeRefIntoBranch(validBranchName, invalidHash, null)).getMessage(),
            allOf(
                containsString("Bad Request (HTTP/400): "),
                containsString("mergeRefIntoBranch.merge: must not be null"),
                containsString("mergeRefIntoBranch.hash: " + HASH_MESSAGE)
            )),
        () -> assertEquals("Bad Request (HTTP/400): deleteTag.hash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.deleteTag(validBranchName, invalidHash)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): transplantCommitsIntoBranch.hash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.transplantCommitsIntoBranch(validBranchName, invalidHash, null, null)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): setContents.hash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> contents.setContents(key, validBranchName, invalidHash, null, cts)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): deleteContents.hash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> contents.deleteContents(key, validBranchName, invalidHash, null)).getMessage()),
        () -> assertThat(
            assertThrows(NessieBadRequestException.class,
                () -> contents.getMultipleContents(invalidHash, null)).getMessage(),
            allOf(
                containsString("Bad Request (HTTP/400): "),
                containsString("getMultipleContents.request: must not be null"),
                containsString("getMultipleContents.ref: " + REF_NAME_OR_HASH_MESSAGE)
            ))
    );
  }

  @ParameterizedTest
  @CsvSource({
      "" + COMMA_VALID_HASH_1,
      "abc'" + COMMA_VALID_HASH_1,
      ".foo" + COMMA_VALID_HASH_2,
      "abc'def'..'blah" + COMMA_VALID_HASH_2,
      "abc'de..blah" + COMMA_VALID_HASH_3,
      "abc'de@{blah" + COMMA_VALID_HASH_3
  })
  void invalidTags(String invalidTagNameIn, String validHash) {
    // CsvSource maps an empty string as null
    String invalidTagName = invalidTagNameIn != null ? invalidTagNameIn : "";

    String validBranchName = "hello";
    ContentsKey key = ContentsKey.of("x");
    MultiGetContentsRequest mgReq = MultiGetContentsRequest.of(key);
    // Need the string-ified JSON representation of `Tag` here, because `Tag` itself performs
    // validation.
    String tag = "{\"type\": \"TAG\", \"name\": \"" + invalidTagName + "\", \"hash\": \"" + validHash + "\"}";
    String branch = "{\"type\": \"BRANCH\", \"name\": \"" + invalidTagName + "\", \"hash\": \"" + validHash + "\"}";
    String different = "{\"type\": \"FOOBAR\", \"name\": \"" + invalidTagName + "\", \"hash\": \"" + validHash + "\"}";
    assertAll(
        () -> assertEquals("Bad Request (HTTP/400): assignTag.tag: must not be null",
            assertThrows(NessieBadRequestException.class,
                () -> unwrap(() ->
                    httpClient.newRequest().path("trees/tag/{tagName}")
                        .resolveTemplate("tagName", validBranchName)
                        .queryParam("expectedHash", validHash)
                        .put(null))
            ).getMessage()),
        () -> assertThat(
            assertThrows(NessieBadRequestException.class,
                () -> unwrap(() ->
                    httpClient.newRequest().path("trees/tag/{tagName}")
                        .resolveTemplate("tagName", validBranchName)
                        .queryParam("expectedHash", validHash)
                        .put(tag))
            ).getMessage(),
            startsWith("Bad Request (HTTP/400): Cannot construct instance of "
                + "`org.projectnessie.model.ImmutableTag`, problem: "
                + REF_NAME_MESSAGE + " - but was: " + invalidTagName + "\n")),
        () -> assertThat(
            assertThrows(NessieBadRequestException.class,
                () -> unwrap(() ->
                    httpClient.newRequest().path("trees/tag/{tagName}")
                        .resolveTemplate("tagName", validBranchName)
                        .queryParam("expectedHash", validHash)
                        .put(branch))
            ).getMessage(),
            startsWith("Bad Request (HTTP/400): Could not resolve type id 'BRANCH' as a subtype of "
                + "`org.projectnessie.model.Tag`: Class `org.projectnessie.model.Branch` "
                + "not subtype of `org.projectnessie.model.Tag`\n")),
        () -> assertThat(
            assertThrows(NessieBadRequestException.class,
                () -> unwrap(() ->
                    httpClient.newRequest().path("trees/tag/{tagName}")
                        .resolveTemplate("tagName", validBranchName)
                        .queryParam("expectedHash", validHash)
                        .put(different))
            ).getMessage(),
            startsWith("Bad Request (HTTP/400): Could not resolve type id 'FOOBAR' as a subtype of "
                + "`org.projectnessie.model.Tag`: known type ids = []\n"))
    );
  }

  void unwrap(Executable exec) throws Throwable {
    try {
      exec.execute();
    } catch (Throwable targetException) {
      if (targetException instanceof HttpClientException) {
        if (targetException.getCause() instanceof NessieNotFoundException
            || targetException.getCause() instanceof NessieConflictException) {
          throw targetException.getCause();
        }
      }

      throw targetException;
    }
  }
}
