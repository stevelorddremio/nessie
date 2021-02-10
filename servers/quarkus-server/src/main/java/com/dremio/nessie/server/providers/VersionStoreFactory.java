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
package com.dremio.nessie.server.providers;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.stream.Stream;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.server.config.ApplicationConfig;
import com.dremio.nessie.server.config.ApplicationConfig.VersionStoreDynamoConfig;
import com.dremio.nessie.server.config.ApplicationConfig.VersionStoreRocksConfig;
import com.dremio.nessie.server.config.converters.VersionStoreType;
import com.dremio.nessie.services.config.ServerConfig;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.NamedRef;
import com.dremio.nessie.versioned.ReferenceAlreadyExistsException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.VersionStore;
import com.dremio.nessie.versioned.WithHash;
import com.dremio.nessie.versioned.impl.JGitVersionStore;
import com.dremio.nessie.versioned.impl.TieredVersionStore;
import com.dremio.nessie.versioned.memory.InMemoryVersionStore;
import com.dremio.nessie.versioned.store.dynamo.DynamoStore;
import com.dremio.nessie.versioned.store.dynamo.DynamoStoreConfig;
import com.dremio.nessie.versioned.store.rocksdb.RocksDBStore;
import com.dremio.nessie.versioned.store.rocksdb.RocksDBStoreConfig;

import software.amazon.awssdk.regions.Region;

@Singleton
public class VersionStoreFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(VersionStoreFactory.class);

  private final ApplicationConfig config;

  @Inject
  public VersionStoreFactory(ApplicationConfig config) {
    this.config = config;
  }

  @ConfigProperty(name = "quarkus.dynamodb.aws.region")
  String region;
  @ConfigProperty(name = "quarkus.dynamodb.endpoint-override")
  Optional<String> endpoint;


  @Produces
  public StoreWorker<Contents, CommitMeta> worker() {
    return new TableCommitMetaStoreWorker();
  }

  /**
   * default config for lambda function.
   */
  @Produces
  @Singleton
  public VersionStore<Contents, CommitMeta> configuration(
      TableCommitMetaStoreWorker storeWorker, Repository repository, ServerConfig config) {
    final VersionStore<Contents, CommitMeta> store = getVersionStore(storeWorker, repository);
    try (Stream<WithHash<NamedRef>> str = store.getNamedRefs()) {
      if (!str.findFirst().isPresent()) {
        // if this is a new database, create a branch with the default branch name.
        try {
          store.create(BranchName.of(config.getDefaultBranch()), Optional.empty());
        } catch (ReferenceNotFoundException | ReferenceAlreadyExistsException e) {
          LOGGER.warn("Failed to create default branch of {}.", config.getDefaultBranch(), e);
        }
      }
    }

    return store;
  }

  private VersionStore<Contents, CommitMeta> getVersionStore(TableCommitMetaStoreWorker storeWorker, Repository repository) {
    switch (config.getVersionStoreConfig().getVersionStoreType()) {
      case DYNAMO:
        LOGGER.info("Using Dynamo Version store");
        return new TieredVersionStore<>(storeWorker, createDynamoConnection(), false);
      case JGIT:
        LOGGER.info("Using JGit Version store");
        return new JGitVersionStore<>(repository, storeWorker);
      case ROCKSDB:
        LOGGER.info("Using Rocks Version store");
        return new TieredVersionStore<>(storeWorker, createRocksConnection(), false);
      case INMEMORY:
        LOGGER.info("Using In Memory Version store");
        return InMemoryVersionStore.<Contents, CommitMeta>builder()
            .metadataSerializer(storeWorker.getMetadataSerializer())
            .valueSerializer(storeWorker.getValueWorker())
            .build();
      default:
        throw new RuntimeException(String.format("unknown JGit repo type %s", config.getVersionStoreConfig().getVersionStoreType()));
    }
  }

  /**
   * create a dynamo store based on config.
   */
  private DynamoStore createDynamoConnection() {
    if (!config.getVersionStoreConfig().getVersionStoreType().equals(VersionStoreType.DYNAMO)) {
      return null;
    }

    VersionStoreDynamoConfig in = config.getVersionStoreDynamoConfig();
    DynamoStore dynamo = new DynamoStore(
        DynamoStoreConfig.builder()
          .endpoint(endpoint.map(e -> {
            try {
              return new URI(e);
            } catch (URISyntaxException uriSyntaxException) {
              throw new RuntimeException(uriSyntaxException);
            }
          }))
          .region(Region.of(region))
          .initializeDatabase(in.isDynamoInitialize())
          .tablePrefix(in.getTablePrefix())
          .build());
    dynamo.start();
    return dynamo;
  }

  /**
   * Create a RocksDB store based on config.
   */
  private RocksDBStore createRocksConnection() {
    if (!config.getVersionStoreConfig().getVersionStoreType().equals(VersionStoreType.ROCKSDB)) {
      return null;
    }

    final VersionStoreRocksConfig in = config.getVersionStoreRocksConfig();
    final RocksDBStore rocks = new RocksDBStore(RocksDBStoreConfig.builder()
        .dbDirectory(in.getDbDirectory())
        .build());
    rocks.start();
    return rocks;
  }

  /**
   * produce a git repo based on config.
   */
  @Produces
  public Repository repository() throws IOException, GitAPIException {
    if (!config.getVersionStoreConfig().getVersionStoreType().equals(VersionStoreType.JGIT)) {
      return null;
    }
    switch (config.getVersionStoreJGitConfig().getJgitStoreType()) {
      case DISK:
        LOGGER.info("JGit Version store has been configured with the file backend");
        File jgitDir = new File(config.getVersionStoreJGitConfig().getJgitDirectory()
                                      .orElseThrow(() -> new RuntimeException("Please set nessie.version.store.jgit.directory")));
        if (!jgitDir.exists()) {
          if (!jgitDir.mkdirs()) {
            throw new RuntimeException(
              String.format("Couldn't create file at %s", config.getVersionStoreJGitConfig().getJgitDirectory().get()));
          }
        }
        LOGGER.info("File backend is at {}", jgitDir.getAbsolutePath());
        return Git.init().setDirectory(jgitDir).call().getRepository();
      case INMEMORY:
        LOGGER.info("JGit Version store has been configured with the in memory backend");
        return new InMemoryRepository.Builder().setRepositoryDescription(new DfsRepositoryDescription()).build();
      default:
        throw new RuntimeException(String.format("unknown jgit repo type %s", config.getVersionStoreJGitConfig().getJgitStoreType()));
    }
  }
}
