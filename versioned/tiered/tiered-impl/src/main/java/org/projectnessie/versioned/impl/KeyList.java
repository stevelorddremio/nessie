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
package org.projectnessie.versioned.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.immutables.value.Value.Immutable;
import org.projectnessie.versioned.impl.KeyMutation.MutationType;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.Store;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * Interface and implementations related to managing the key list within Dynamo.
 */
abstract class KeyList {

  public static final KeyList EMPTY = new CompleteList(Collections.emptyList(), ImmutableList.of());

  enum Type {
    INCREMENTAL,
    FULL
  }

  abstract KeyList plus(Id parent, List<KeyMutation> mutations);

  abstract Optional<KeyList> createCheckpointIfNeeded(InternalL1 startingPoint, Store store);

  abstract Type getType();

  static IncrementalList incremental(
      Id previousCheckpointL1,
      List<KeyMutation> mutations,
      int distanceFromCheckpointCommits) {
    return ImmutableIncrementalList.builder()
        .previousCheckpoint(previousCheckpointL1)
        .distanceFromCheckpointCommits(distanceFromCheckpointCommits)
        .mutations(mutations).build();
  }

  abstract Stream<InternalKey> getKeys(InternalL1 startingPoint, Store store);


  abstract List<KeyMutation> getMutations();

  abstract List<Id> getFragments();

  boolean isEmptyIncremental() {
    return getType() == Type.INCREMENTAL && getMutations().isEmpty();
  }

  boolean isFull() {
    return getType() == Type.FULL;
  }

  @Immutable
  abstract static class IncrementalList extends KeyList {

    public static final int MAX_DELTAS = 50;

    public abstract List<KeyMutation> getMutations();

    public abstract Id getPreviousCheckpoint();

    public abstract int getDistanceFromCheckpointCommits();

    @Override
    public KeyList plus(Id parent, List<KeyMutation> mutations) {
      return ImmutableIncrementalList.builder()
          .addAllMutations(mutations)
          .distanceFromCheckpointCommits(getDistanceFromCheckpointCommits() + 1)
          .previousCheckpoint(getPreviousCheckpoint()).build();
    }

    @Override
    public Optional<KeyList> createCheckpointIfNeeded(InternalL1 startingPoint, Store store) {
      if (getDistanceFromCheckpointCommits() < MAX_DELTAS) {
        return Optional.empty();
      }


      return Optional.of(generateNewCheckpoint(startingPoint, store));
    }


    @Override
    Stream<InternalKey> getKeys(InternalL1 startingPoint, Store store) {
      IterResult keys = getKeysIter(startingPoint, store);
      if (keys.isChanged()) {
        return keys.keyList;
      }

      return keys.list.getKeys(startingPoint, store);
    }

    private CompleteList generateNewCheckpoint(InternalL1 startingPoint, Store store) {

      IterResult result = getKeysIter(startingPoint, store);
      if (!result.isChanged()) {
        return result.list;
      }

      final KeyAccumulator accum = new KeyAccumulator(store, result.previousFragmentIds);
      result.keyList.forEach(accum::addKey);
      accum.close();

      return accum.getCompleteList(getMutations());
    }

    private IterResult getKeysIter(InternalL1 startingPoint, Store store) {
      HistoryRetriever retriever = new HistoryRetriever(store, startingPoint, getPreviousCheckpoint(), true, false, true);
      final CompleteList complete;
      // incrementals, from oldest to newest.
      final List<KeyList> incrementals;

      { // load the lists.
        ImmutableList<KeyList> keyLists = retriever.getStream()
            .map(h -> h.getL1().getKeyList())
            .filter(kl -> !kl.isEmptyIncremental())
            .collect(ImmutableList.toImmutableList());

        // the very last keylist should be a completelist, given the correct stop.
        KeyList last = keyLists.get(keyLists.size() - 1);
        Preconditions.checkArgument(last.isFull());
        complete = (CompleteList) last;
        incrementals = Lists.reverse(keyLists.subList(0, keyLists.size() - 1));
      }

      Set<InternalKey> removals = new HashSet<>();
      Set<InternalKey> adds = new HashSet<>();


      // determine the unique list of mutations. Operations that cancel each other out are ignored for checkpoint purposes.
      for (KeyList kl : incrementals) {
        Preconditions.checkArgument(kl.getType() == Type.INCREMENTAL);
        IncrementalList il = (IncrementalList) kl;
        il.getMutations().forEach(m -> {
          final InternalKey key = m.getKey();
          if (m.getType() == MutationType.ADDITION) {
            if (removals.contains(key)) {
              removals.remove(key);
            } else {
              adds.add(key);
            }
          } else if (m.getType() == MutationType.REMOVAL) {
            if (adds.contains(key)) {
              adds.remove(key);
            } else {
              removals.add(key);
            }
          } else {
            throw new IllegalStateException("Invalid mutation type: " + m.getType().name());
          }
        });
      }


      if (removals.isEmpty() && adds.isEmpty()) {
        return IterResult.unchanged(complete);
      }

      return IterResult.changed(
          complete.fragmentIds.stream().collect(ImmutableSet.toImmutableSet()),
          Stream.concat(
              complete.getKeys(startingPoint, store).filter(k -> !removals.contains(k)),
              adds.stream()));
    }

    @Override
    public Type getType() {
      return Type.INCREMENTAL;
    }

    private static class IterResult {
      private final CompleteList list;
      private final Stream<InternalKey> keyList;
      private final Set<Id> previousFragmentIds;

      private IterResult(CompleteList list, Stream<InternalKey> keyList, Set<Id> previousFragmentIds) {
        super();
        this.list = list;
        this.keyList = keyList;
        this.previousFragmentIds = previousFragmentIds;
      }

      public static IterResult unchanged(CompleteList list) {
        return new IterResult(list, null, null);
      }

      public static IterResult changed(Set<Id> previousFragmentIds, Stream<InternalKey> keys) {
        return new IterResult(null, keys, previousFragmentIds);
      }

      public boolean isChanged() {
        return keyList != null;
      }

    }

  }


  /**
   * A complete list is composed as one or more fragments. Each fragment's id is generated by the hashed value of its
   * contents.
   *
   * <p>Fragments lists are designed to minimize Dynamo record churn. Early fragment lists have the oldest entries.
   * Whenever a key is added, it is added to the last fragment (or a new fragment if the last fragment is oversized).
   * As such, over time the early fragments rarely if ever get restated.
   */
  static class CompleteList extends KeyList {
    private final List<Id> fragmentIds;
    private final List<KeyMutation> mutations;

    public CompleteList(List<Id> fragmentIds, List<KeyMutation> mutations) {
      this.fragmentIds = Preconditions.checkNotNull(fragmentIds);
      this.mutations = ImmutableList.copyOf(mutations);
    }

    @Override
    public KeyList plus(Id parent, List<KeyMutation> mutations) {
      return ImmutableIncrementalList.builder()
          .addAllMutations(mutations)
          .distanceFromCheckpointCommits(1)
          .previousCheckpoint(parent)
          .build();
    }

    @Override
    public Type getType() {
      return Type.FULL;
    }

    @Override
    public Optional<KeyList> createCheckpointIfNeeded(InternalL1 startingPoint, Store store) {
      // checkpoint not needed, already a checkpoint.
      return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      CompleteList that = (CompleteList) o;

      return Objects.equals(fragmentIds, that.fragmentIds)
          && Objects.equals(mutations, that.mutations);
    }

    @Override
    public int hashCode() {
      int result = fragmentIds != null ? fragmentIds.hashCode() : 0;
      result = 31 * result + (mutations != null ? mutations.hashCode() : 0);
      return result;
    }

    @Override
    Stream<InternalKey> getKeys(InternalL1 startingPoint, Store store) {
      return fragmentIds.stream().flatMap(f -> {
        InternalFragment fragment = EntityType.KEY_FRAGMENT.loadSingle(store, f);
        return fragment.getKeys().stream();
      });
    }

    @Override
    List<KeyMutation> getMutations() {
      return mutations;
    }

    @Override
    List<Id> getFragments() {
      return ImmutableList.copyOf(fragmentIds);
    }
  }


  /**
   * Accumulates keys until we have enough to fill the ~max DynamoDB record size.
   *
   * <p>TODO: consider moving this data to S3.
   *
   * <p>TODO: move to a prefix encoded format.
   */
  static class KeyAccumulator {
    private static final int MAX_SIZE = 400_000 - 8096;
    private final Store store;
    private final Set<Id> presaved;
    private final List<InternalKey> currentList = new ArrayList<>();
    private final List<Id> fragmentIds = new ArrayList<>();
    private int currentListSize;

    public KeyAccumulator(Store store, Set<Id> presaved) {
      super();
      this.store = store;
      this.presaved = presaved;
    }

    public void addKey(InternalKey key) {
      currentList.add(key);
      currentListSize += key.estimatedSize();

      rotate(false);
    }

    private void rotate(boolean always) {
      if (!currentList.isEmpty() && (always || aboveThreshold())) {
        InternalFragment fragment = new InternalFragment(currentList);
        currentList.clear();
        currentListSize = 0;
        if (!presaved.contains(fragment.getId())) {
          // only save if we didn't save on the last checkpoint. This could still be a dupe of an older list but since the object
          // is hashed, the value will be a simple overwrite of the same data.
          store.save(Collections.singletonList(EntityType.KEY_FRAGMENT.createSaveOpForEntity(fragment)));
          fragmentIds.add(fragment.getId());
        }
      }
    }

    private boolean aboveThreshold() {
      return currentListSize > MAX_SIZE;
    }

    public void close() {
      rotate(true);
    }

    public CompleteList getCompleteList(List<KeyMutation> mutations) {
      Preconditions.checkArgument(currentList.isEmpty());
      return new CompleteList(fragmentIds, mutations);
    }
  }


}
