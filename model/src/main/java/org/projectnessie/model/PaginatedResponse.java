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
package org.projectnessie.model;

import javax.annotation.Nullable;

import org.immutables.value.Value.Default;

public interface PaginatedResponse {

  /**
   * Whether there are more result-items than returned by this response object.
   * <p>If there are more result-items, the value returned by {@link #getToken()} can
   * be used in the next invocation to get the next "page" of results.</p>
   * @return {@code true}, if there are more result items.
   */
  @Default
  default boolean hasMore() {
    return false;
  }

  /**
   * Pass this value to the next invocation of the API function to get the next page
   * of results.
   * <p>Paging tokens are opaque and the structure may change without prior notice
   * even in patch releases.</p>
   * @return paging-token for the next invocation of an API function, if {@link #hasMore()} is {@code true}.
   *     Undefined, if {@link #hasMore()} is {@code false}.
   */
  @Nullable
  String getToken();
}
