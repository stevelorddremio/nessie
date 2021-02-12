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
package org.projectnessie.server.error;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.ws.rs.core.Response;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.rest.NessieBadRequestException;
import org.projectnessie.client.rest.NessieHttpResponseFilter;
import org.projectnessie.client.rest.NessieInternalServerException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.quarkus.test.junit.QuarkusTest;

/**
 * Test reported exceptions both for cases when {@code javax.validation} fails (when the Nessie infra
 * code isn't even run) and exceptions reported <em>by</em> Nessie.
 */
@QuarkusTest
class TestNessieError {

  static String baseURI = "http://localhost:19121/api/v1/nessieErrorTest";

  private static HttpClient client;

  @BeforeAll
  static void setup() {
    ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    client = HttpClient.builder()
        .setBaseUri(baseURI)
        .setObjectMapper(mapper)
        .build();
    client.register(new NessieHttpResponseFilter(mapper));
  }

  @Test
  void nullParameterQueryGet() {
    assertEquals("Bad Request (HTTP/400): nullParameterQueryGet.hash: must not be null",
                 assertThrows(NessieBadRequestException.class,
                   () ->
                       client.newRequest()
                             .path("nullParameterQueryGet")
                             .get()).getMessage());
  }

  @Test
  void nullParameterQueryPost() {
    assertEquals("Bad Request (HTTP/400): nullParameterQueryPost.hash: must not be null",
                 assertThrows(NessieBadRequestException.class,
                   () ->
                       client.newRequest()
                             .path("nullParameterQueryPost")
                             .post("")).getMessage());
  }

  @Test
  void emptyParameterQueryGet() {
    assertAll(
        () -> assertEquals("Bad Request (HTTP/400): emptyParameterQueryGet.hash: must not be empty",
                 assertThrows(NessieBadRequestException.class,
                   () ->
                       client.newRequest()
                             .path("emptyParameterQueryGet")
                             .get()).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): emptyParameterQueryGet.hash: must not be empty",
                 assertThrows(NessieBadRequestException.class,
                   () ->
                       client.newRequest()
                             .path("emptyParameterQueryGet")
                             .queryParam("hash", "")
                             .get()).getMessage())
    );
  }

  @Test
  void blankParameterQueryGet() {
    assertAll(
        () -> assertEquals("Bad Request (HTTP/400): blankParameterQueryGet.hash: must not be blank",
                 assertThrows(NessieBadRequestException.class,
                   () ->
                       client.newRequest()
                             .path("blankParameterQueryGet")
                             .get()).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): blankParameterQueryGet.hash: must not be blank",
                 assertThrows(NessieBadRequestException.class,
                   () ->
                       client.newRequest()
                             .path("blankParameterQueryGet")
                             .queryParam("hash", "")
                             .get()).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): blankParameterQueryGet.hash: must not be blank",
                 assertThrows(NessieBadRequestException.class,
                   () ->
                       client.newRequest()
                             .path("blankParameterQueryGet")
                             .queryParam("hash", "   ")
                             .get()).getMessage())
    );
  }

  @Test
  void entityValueViolation() {
    assertAll(
        () -> assertThat(assertThrows(NessieBadRequestException.class,
          () ->
              client.newRequest()
                    .path("basicEntity")
                    .put("not really valid json")
         ).getMessage(),
         startsWith("Bad Request (HTTP/400): Unrecognized token 'not': was expecting (JSON String, Number, "
                    + "Array, Object or token 'null', 'true' or 'false')\n")),
        () -> assertThat(assertThrows(NessieBadRequestException.class,
          () ->
              client.newRequest()
                    .path("basicEntity")
                    .put("{}")
         ).getMessage(),
         startsWith("Bad Request (HTTP/400): Missing required creator property 'value' (index 0)\n")),
        () -> assertThat(assertThrows(NessieBadRequestException.class,
          () ->
              client.newRequest()
                    .path("basicEntity")
                    .put("{\"value\":null}")
         ).getMessage(),
         equalTo("Bad Request (HTTP/400): basicEntity.entity.value: must not be null")),
        () -> assertThat(assertThrows(NessieBadRequestException.class,
          () ->
              client.newRequest()
                    .path("basicEntity")
                    .put("{\"value\":1.234}")
         ).getMessage(),
         equalTo("Bad Request (HTTP/400): basicEntity.entity.value: must be greater than or equal to 3"))
    );
  }

  @Test
  void brokenEntitySerialization() {
    // send something that cannot be deserialized
    assertThat(assertThrows(NessieBadRequestException.class,
        () ->
            unwrap(() -> client.newRequest()
                  .path("basicEntity")
                  .put(new OtherEntity("bar")))
       ).getMessage(),
        startsWith("Bad Request (HTTP/400): Missing required creator property 'value' (index 0)\n"));
  }

  @Test
  void nessieNotFoundException() {
    NessieNotFoundException ex = assertThrows(NessieNotFoundException.class,
        () -> unwrap(() ->
            client.newRequest()
                  .path("nessieNotFound")
                  .get()));
    assertAll(
        () -> assertEquals("not-there-message",
                           ex.getMessage()),
        () -> assertThat(ex.getServerStackTrace(),
                         startsWith("org.projectnessie.error.NessieNotFoundException: not-there-message\n")),
        () -> assertEquals(Response.Status.NOT_FOUND.getStatusCode(), ex.getStatus())
    );
  }

  @Test
  void nonConstraintValidationExceptions() {
    // Exceptions that trigger the "else-ish" part in ResteasyExceptionMapper.toResponse()
    assertAll(
        () -> assertThat(
            assertThrows(NessieInternalServerException.class,
                () -> unwrap(() ->
                    client.newRequest()
                        .path("constraintDefinitionException")
                        .get())).getMessage(),
            startsWith("Internal Server Error (HTTP/500): javax.validation.ConstraintDefinitionException: meep\n")),
        () -> assertThat(
            assertThrows(NessieInternalServerException.class,
                () -> unwrap(() ->
                    client.newRequest()
                        .path("constraintDeclarationException")
                        .get())).getMessage(),
            startsWith("Internal Server Error (HTTP/500): javax.validation.ConstraintDeclarationException: meep\n")),
        () -> assertThat(
            assertThrows(NessieInternalServerException.class,
                () -> unwrap(() ->
                    client.newRequest()
                        .path("groupDefinitionException")
                        .get())).getMessage(),
            startsWith("Internal Server Error (HTTP/500): javax.validation.GroupDefinitionException: meep\n"))
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
