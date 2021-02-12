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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.Store;

import com.google.common.collect.AbstractIterator;

/**
 * Enables retrieval of L1 history.
 */
class HistoryRetriever {

  private final boolean retrieveL1;
  private final boolean retrieveCommit;
  private final boolean includeEndEmpty;
  private final Store store;
  private final InternalL1 start;
  private final Id end;

  public HistoryRetriever(Store store, InternalL1 start, Id end, boolean retrieveL1, boolean retrieveCommit, boolean includeEndEmpty) {
    super();
    this.store = store;
    this.start = start;
    this.end = end;
    this.retrieveL1 = retrieveL1;
    this.retrieveCommit = retrieveCommit;
    this.includeEndEmpty = includeEndEmpty;
  }

  class HistoryItem {

    private Id id;
    private InternalL1 l1;
    private InternalCommitMetadata commitMetadata;

    public HistoryItem(Id id) {
      this.id = id;
    }

    public InternalL1 getL1() {
      return l1;
    }

    public Id getId() {
      return id;
    }

    public InternalCommitMetadata getMetadata() {
      return commitMetadata;
    }

    @Override
    public String toString() {
      return "HistoryItem [id=" + id + ", l1=" + l1 + ", commitMetadata=" + commitMetadata + "]";
    }

  }

  Stream<HistoryItem> getStream() {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new HistoryIterator(), 0), false);
  }

  private class HistoryIterator extends AbstractIterator<HistoryItem> {

    private Iterator<HistoryItem> currentIterator;
    private boolean isLast = false;
    private HistoryItem previous;

    public HistoryIterator() {
      this.previous = null;

      if (start.getId().equals(InternalL1.EMPTY_ID) && !includeEndEmpty) {
        this.currentIterator = Collections.emptyIterator();
        this.isLast = true;
      } else {
        HistoryItem item = new HistoryItem(start.getId());
        item.l1 = start;
        if (retrieveCommit && !start.getMetadataId().isEmpty()) {
          item.commitMetadata = EntityType.COMMIT_METADATA.loadSingle(store, start.getMetadataId());
        }
        this.currentIterator = Collections.singleton(item).iterator();
      }
    }

    @Override
    protected HistoryItem computeNext() {
      while (!currentIterator.hasNext() && !isLast && previous.getL1() != null) {
        try {
          calculateNextList(previous.getL1().getParentList());
        } catch (ReferenceNotFoundException e) {
          throw new RuntimeException(e);
        }
      }

      if (!currentIterator.hasNext()) {
        endOfData();
        return null;
      }

      previous = currentIterator.next();
      return previous;
    }

    private void calculateNextList(ParentList list) throws ReferenceNotFoundException {
      int max = list.getParents().size();
      final List<HistoryItem> items = new ArrayList<>();
      final EntityLoadOps loadOps = new EntityLoadOps();
      final EntityLoadOps secondOps = new EntityLoadOps();
      final List<Id> ids = list.getParents();
      for (int i = 0; i < max; i++) {
        final boolean lastInList = i == max - 1;
        Id parent = ids.get(i);

        if (parent.isEmpty()) {
          break;
        }

        if (!includeEndEmpty && parent.equals(InternalL1.EMPTY_ID)) {
          isLast = true;
          break;
        }

        final HistoryItem item = new HistoryItem(parent);
        items.add(item);
        if (retrieveL1 || retrieveCommit || lastInList) {
          loadOps.load(EntityType.L1, InternalL1.class, parent, l1 -> item.l1 = l1);
        }

        if (retrieveCommit && !parent.equals(InternalL1.EMPTY_ID)) {
          secondOps.loadDeferred(EntityType.COMMIT_METADATA,
              InternalCommitMetadata.class,
              () -> item.l1.getMetadataId(),
              cmd -> item.commitMetadata = cmd);
        }

        if (parent.equals(end)) {
          isLast = true;
          break;
        }
      }

      if (items.isEmpty()) {
        currentIterator = Collections.emptyIterator();
        isLast = true;
        return;
      }

      store.load(loadOps.build(secondOps::buildOptional));
      currentIterator = items.iterator();
    }

  }

  public static Id findCommonParent(Store store, InternalL1 head1, InternalL1 head2, int maxDepth) {
    Iterator<Id> r1 = new HistoryRetriever(store, head1, Id.EMPTY, false, false, true).getStream().map(HistoryItem::getId).iterator();
    Iterator<Id> r2 = new HistoryRetriever(store, head2, Id.EMPTY, false, false, true).getStream().map(HistoryItem::getId).iterator();
    Set<Id> r1Set = new LinkedHashSet<>();
    Set<Id> r2Set = new LinkedHashSet<>();
    int remainingDepth = maxDepth;
    while (remainingDepth > 0) {
      int page = Math.min(remainingDepth, ParentList.MAX_PARENT_LIST_SIZE);
      addNextPage(r1, r1Set, page);
      addNextPage(r2, r2Set, page);
      remainingDepth -= page;
      Optional<Id> id = r1Set.stream().filter(r2Set::contains).findFirst();
      if (id.isPresent()) {
        return id.get();
      }

      if (!r1.hasNext() && !r2.hasNext()) {
        break;
      }
    }

    throw new IllegalStateException(
        String.format("Unable to find common parent within specified history depth. "
            + "The maximum number of items allowed is %d.", maxDepth));
  }

  private static <T> void addNextPage(Iterator<T> input, Collection<T> consumer, int count) {
    while (input.hasNext() && count > 0) {
      consumer.add(input.next());
      count--;
    }
  }
}
