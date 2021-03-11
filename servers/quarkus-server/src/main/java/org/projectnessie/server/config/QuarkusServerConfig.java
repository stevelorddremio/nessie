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
package org.projectnessie.server.config;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.projectnessie.services.config.ServerConfig;

import io.quarkus.arc.config.ConfigProperties;

/**
 * Nessie server config for Quarkus.
 */
@ConfigProperties(prefix = "nessie.server")
public interface QuarkusServerConfig extends ServerConfig {

  @ConfigProperty(name = "default-branch", defaultValue = "main")
  @Override
  String getDefaultBranch();

  @ConfigProperty(name = "send-stacktrace-to-client", defaultValue = "false")
  @Override
  boolean sendStacktraceToClient();
}
