#include <assert.h>
#include <limits.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "crc.h"

#include "dict.h"

#define DICT_HT_SIZE_MINIMUM 4

#define DICT_STATS_VEC_SIZE 50

#define NOTUSED(V) ((void)V)

// Hash table size should always be a power of 2.
static uint64_t limitedHTSize(uint64_t size) {
  uint64_t i = DICT_HT_SIZE_MINIMUM;

  if (size >= LONG_MAX)
    return LONG_MAX + 1LU;
  while (1) {
    if (i >= size)
      return i;
    i *= 2;
  }

  assert(0);
}

// Expand hash table and launch incremental rehash progress.
static int expand(dict *d, uint64_t size) {
  if (dictRehashing(d) || d->ht[0].filled > size)
    return -1;

  size = limitedHTSize(size);

  if (size == d->ht[0].size)
    return -1;

  dictht ht;
  ht.size = size;
  ht.mask = size - 1;
  ht.entries = calloc(size, sizeof(dictEntry *));
  ht.filled = 0;

  if (d->ht[0].entries == NULL) {
    d->ht[0] = ht;
    return 0;
  }

  d->ht[1] = ht;
  // Start incremental rehashing.
  d->rehash = 0;
  return 0;
}

static int tryExpand(dict *d) {
  if (dictRehashing(d))
    return 0;

  if (d->ht[0].size == 0)
    return expand(d, DICT_HT_SIZE_MINIMUM);

  if (d->ht[0].filled >= d->ht[0].size) {
    return expand(d, d->ht[0].filled * 2);
  }

  return 0;
}

static int64_t getExistingOrIndex(dict *d, const void *key, uint64_t hash,
                                  dictEntry **existing) {
  uint64_t i;
  uint8_t ht;
  dictEntry *e;

  if (existing)
    *existing = NULL;

  if (tryExpand(d) == -1)
    return -1;

  for (ht = 0; ht <= 1; ht++) {
    i = hash & d->ht[ht].mask;
    e = d->ht[ht].entries[i];
    while (e) {
      if (dictKeyCmp(d, key, e->key)) {
        if (existing)
          *existing = e;
        return -1;
      }
      e = e->next;
    }
    if (!dictRehashing(d))
      break;
  }
  return i;
}

static void resetht(dictht *ht) {
  ht->entries = NULL;
  ht->size = 0;
  ht->mask = 0;
  ht->filled = 0;
}

uint64_t dictHashFnDefault(const void *key, int len) {
  crc_t crc;
  crc = crc_init();
  crc = crc_update(crc, (unsigned char *)key, len);
  crc = crc_finalize(crc);

  return crc;
}

dict *dictCreate(dictType *type) {
  if (!type->hashFn || !type->keyCmp) {
    return NULL;
  }

  dict *d = malloc(sizeof(*d));
  assert(d);

  resetht(&d->ht[0]);
  resetht(&d->ht[1]);
  d->type = *type;
  d->rehash = -1;
  d->iters = 0;

  return d;
}

static int rehash(dict *d, int n) {
  int emptyVisitCnt = n * 10; /* Max number of empty buckets to visit. */

  if (!dictRehashing(d))
    return 0;

  while (n-- && d->ht[0].filled != 0) {
    dictEntry *e, *nexte;

    assert(d->ht[0].size > (uint64_t)d->rehash);

    while (d->ht[0].entries[d->rehash] == NULL) {
      d->rehash++;
      if (--emptyVisitCnt == 0)
        return 1;
    }

    e = d->ht[0].entries[d->rehash];
    while (e) {
      uint64_t i;

      nexte = e->next;

      i = dictHashKeyGet(d, e->key) & d->ht[1].mask;
      e->next = d->ht[1].entries[i];
      d->ht[1].entries[i] = e;
      d->ht[0].filled--;
      d->ht[1].filled++;

      e = nexte;
    }

    d->ht[0].entries[d->rehash] = NULL;
    d->rehash++;
  }

  // Rehash progress complete.
  if (d->ht[0].filled == 0) {
    free(d->ht[0].entries);
    d->ht[0] = d->ht[1];
    resetht(&d->ht[1]);
    d->rehash = -1;
    return 0;
  }

  return 1;
}

static uint64_t mstime(void) {
  struct timeval tv;

  gettimeofday(&tv, NULL);
  return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

int dictRehashms(dict *d, int ms) {
  uint64_t start = mstime();
  int cnt = 0;

  while (rehash(d, 100)) {
    cnt += 100;
    if (mstime() > start + ms)
      break;
  }
  return cnt;
}

static void rehashStep(dict *d) {
  if (d->iters == 0)
    rehash(d, 1);
}

int dictAdd(dict *d, void *key, void *val) {
  dictEntry *e = dictAddKeyOrGetExistingEntry(d, key, NULL);

  if (!e)
    return -1;

  dictValSet(d, e, val);
  return 0;
}

dictEntry *dictAddKeyOrGetExistingEntry(dict *d, void *key,
                                        dictEntry **existing) {
  int64_t i;
  dictEntry *e;
  dictht *ht;

  if (dictRehashing(d))
    rehashStep(d);

  // Key already exists.
  if ((i = getExistingOrIndex(d, key, dictHashKeyGet(d, key), existing)) == -1)
    return NULL;

  ht = dictRehashing(d) ? &d->ht[1] : &d->ht[0];
  e = malloc(sizeof(*e));
  e->next = ht->entries[i];
  ht->entries[i] = e;
  ht->filled++;

  dictKeySet(d, e, key);
  return e;
}

int dictUpdateOrAdd(dict *d, void *key, void *val) {
  dictEntry *e, *existing, preve;

  // Add.
  e = dictAddKeyOrGetExistingEntry(d, key, &existing);
  if (e) {
    dictValSet(d, e, val);
    return 1;
  }

  // Update existing.
  preve = *existing;
  dictValSet(d, existing, val);
  dictValFree(d, &preve);
  return 0;
}

dictEntry *dictEntryDelete(dict *d, const void *key, int noEntryFree) {
  uint64_t h, i;
  dictEntry *e, *preve;
  int table;

  if (d->ht[0].filled == 0 && d->ht[1].filled == 0)
    return NULL;

  if (dictRehashing(d))
    rehashStep(d);

  h = dictHashKeyGet(d, key);

  for (table = 0; table <= 1; table++) {
    i = h & d->ht[table].mask;
    e = d->ht[table].entries[i];
    preve = NULL;
    while (e) {
      if (dictKeyCmp(d, key, e->key)) {
        if (preve)
          preve->next = e->next;
        else
          d->ht[table].entries[i] = e->next;
        if (!noEntryFree) {
          dictKeyFree(d, e);
          dictValFree(d, e);
          free(e);
        }
        d->ht[table].filled--;

        // Success.
        return e;
      }
      preve = e;
      e = e->next;
    }
    if (!dictRehashing(d))
      break;
  }

  // Not found.
  return NULL;
}

int dictEntryFree(dict *d, dictEntry *he) {
  if (he == NULL)
    return -1;

  dictKeyFree(d, he);
  dictValFree(d, he);
  free(he);

  return 0;
}

static int clearht(dict *d, dictht *ht) {
  uint64_t i;
  for (i = 0; i < ht->size && ht->filled > 0; i++) {
    dictEntry *e, *nexte;

    if ((e = ht->entries[i]) == NULL)
      continue;

    while (e) {
      nexte = e->next;

      dictKeyFree(d, e);
      dictValFree(d, e);
      free(e);

      ht->filled--;
      e = nexte;
    }
  }

  free(ht->entries);

  resetht(ht);

  return 0;
}

int dictDestroy(dict *d) {
  clearht(d, &d->ht[0]);
  clearht(d, &d->ht[1]);
  free(d);

  return 0;
}

dictEntry *dictFind(dict *d, const void *key) {
  dictEntry *e;
  uint64_t h, i;
  int table;

  if (dictFilled(d) == 0)
    return NULL;

  if (dictRehashing(d))
    rehashStep(d);

  h = dictHashKeyGet(d, key);
  for (table = 0; table <= 1; table++) {
    i = h & d->ht[table].mask;
    e = d->ht[table].entries[i];

    while (e) {
      if (dictKeyCmp(d, key, e->key))
        return e;

      e = e->next;
    }

    if (!dictRehashing(d))
      return NULL;
  }

  return NULL;
}

static int iteratorReset(dictIterator *iter) {
  iter->i = -1;
  iter->ht = &iter->d->ht[0];
  iter->entry = NULL;
  iter->nextEntry = NULL;

  return 0;
}

dictIterator *dictIteratorCreate(dict *d) {
  dictIterator *iter = malloc(sizeof(*iter));
  assert(iter);

  iter->d = d;

  if (tryExpand(d) == -1) {
    return NULL;
  }

  iteratorReset(iter);

  return iter;
}

int dictIteratorDestroy(dictIterator *iter) {
  if (!(iter->i == -1 && iter->ht == &iter->d->ht[0])) {
    iter->d->iters--;
  }

  free(iter);

  return 0;
}

int dictIteratorRewind(dictIterator *iter) {
  if (!(iter->i == -1 && iter->ht == &iter->d->ht[0])) {
    iter->d->iters--;
  }

  iteratorReset(iter);

  return 0;
}

// After the first dictIteratorNext() invocation, dict suspend rehash progress
// until dictIteratorRewind() or dictIteratorDestroy() was invoked.
//
// That's why dictIteratorRewind() or dictIteratorDestroy() should be invoked
// ASAP.
dictEntry *dictIteratorNext(dictIterator *iter) {
  for (;;) {
    if (!iter->entry) {
      if (iter->i == -1 && iter->ht == &iter->d->ht[0]) {
        iter->d->iters++;
      }

      if (++iter->i >= (int64_t)iter->ht->size) {
        if (dictRehashing(iter->d) && iter->ht == &iter->d->ht[0]) {
          iter->ht = &iter->d->ht[1];
          iter->i = 0;
        } else {
          break;
        }
      }

      iter->entry = iter->ht->entries[iter->i];
    } else {
      iter->entry = iter->nextEntry;
    }

    if (iter->entry) {
      // User might delete the entry returned.
      iter->nextEntry = iter->entry->next;
      return iter->entry;
    }
  }

  return NULL;
}

size_t _dictGetStatsHt(dictht *ht, char *buf, size_t len, int table) {
  uint64_t i, slots = 0, chainlen, maxchainlen = 0;
  uint64_t totchainlen = 0;
  uint64_t clvector[DICT_STATS_VEC_SIZE];
  size_t l = 0;

  if (ht->filled == 0) {
    return snprintf(buf, len, "Empty dict.\n");
  }

  for (i = 0; i < DICT_STATS_VEC_SIZE; i++)
    clvector[i] = 0;

  for (i = 0; i < ht->size; i++) {
    if (ht->entries[i] == NULL) {
      clvector[0]++;
      continue;
    }

    slots++;

    chainlen = 0;
    dictEntry *e = ht->entries[i];
    while (e) {
      chainlen++;
      e = e->next;
    }
    clvector[(chainlen < DICT_STATS_VEC_SIZE) ? chainlen
                                              : (DICT_STATS_VEC_SIZE - 1)]++;
    if (chainlen > maxchainlen)
      maxchainlen = chainlen;
    totchainlen += chainlen;
  }

  l += snprintf(buf + l, len - l,
                "Hash table %d:\n"
                " size: %ld\n"
                " filled: %ld\n"
                " different slots: %ld\n"
                " max chain length: %ld\n"
                " avg chain length (counted): %.02f\n"
                " avg chain length (computed): %.02f\n"
                " Chain length distribution:\n",
                table, ht->size, ht->filled, slots, maxchainlen,
                (float)totchainlen / slots, (float)ht->filled / slots);

  for (i = 0; i < DICT_STATS_VEC_SIZE - 1; i++) {
    if (clvector[i] == 0)
      continue;

    if (l >= len)
      break;
    l += snprintf(buf + l, len - l, "   %s%ld: %ld (%.02f%%)\n",
                  (i == DICT_STATS_VEC_SIZE - 1) ? ">= " : "", i, clvector[i],
                  ((float)clvector[i] / ht->size) * 100);
  }

  if (len)
    buf[len - 1] = '\0';

  return strlen(buf);
}

int dictStats(dict *d, char *buf, size_t len) {
  size_t l;
  char *savedBuf = buf;
  size_t savedLen = len;

  l = _dictGetStatsHt(&d->ht[0], buf, len, 0);
  buf += l;
  len -= l;

  if (dictRehashing(d) && len > 0) {
    _dictGetStatsHt(&d->ht[1], buf, len, 1);
  }

  if (savedLen)
    savedBuf[savedLen - 1] = '\0';

  return 0;
}

#ifdef DICT_BENCHMARK_MAIN

uint64_t hashCallback(const void *key) {
  return dictHashFnDefault((unsigned char *)key, strlen(key));
}

int compareCallback(const void *key1, const void *key2) {
  int i = strlen(key1);
  int j = strlen(key2);
  if (i < j)
    i = j;
  return memcmp(key1, key2, i) == 0;
}

dictType BenchmarkDictType = {hashCallback, compareCallback, NULL, NULL, NULL,
                              NULL};

#define start_benchmark() start = mstime()
#define end_benchmark(msg)                                                     \
  do {                                                                         \
    elapsed = mstime() - start;                                                \
    printf(msg ": %ld items in %ld ms\n", count, elapsed);                     \
  } while (0);

int main() {
  int64_t j;
  int64_t start, elapsed;
  dict *dict = dictCreate(&BenchmarkDictType);
  int64_t count = 1000000;
  char *key;

  start_benchmark();
  for (j = 0; j < count; j++) {
    key = malloc(8);
    snprintf(key, 8, "%lu", j);
    int n = dictAdd(dict, key, (void *)key);
    assert(n == 0);
  }
  end_benchmark("dictAdd");
  assert((int64_t)dictFilled(dict) == count);

  while (dictRehashing(dict)) {
    dictRehashms(dict, 100);
  }

  start_benchmark();
  for (j = 0; j < count; j++) {
    key = malloc(8);
    snprintf(key, 8, "%lu", j);
    dictEntry *e = dictFind(dict, key);
    assert(e != NULL);
    free(key);
  }
  end_benchmark("Linear access of existing elements");

  start_benchmark();
  for (j = 0; j < count; j++) {
    key = malloc(8);
    snprintf(key, 8, "%lu", rand() % count);
    dictEntry *de = dictFind(dict, key);
    assert(de != NULL);
    free(key);
  }
  end_benchmark("Random access of existing elements");

  start_benchmark();
  dictIterator *iter = dictIteratorCreate(dict);
  int cnt = 0;
  for (;;) {
    dictEntry *e = dictIteratorNext(iter);
    if (e == NULL)
      break;
    cnt++;
  }
  assert(cnt == count);
  end_benchmark("iterate all elements");

  start_benchmark();
  dictIteratorRewind(iter);
  cnt = 0;
  for (;;) {
    dictEntry *e = dictIteratorNext(iter);
    if (e == NULL)
      break;
    cnt++;
  }
  assert(cnt == count);
  dictIteratorDestroy(iter);
  end_benchmark("iterate all elements after rewind");

  start_benchmark();
  for (j = 0; j < count; j++) {
    key = malloc(8);
    snprintf(key, 8, "%lu", rand() % count);
    key[0] = 'M';
    dictEntry *e = dictFind(dict, key);
    assert(e == NULL);
    free(key);
  }
  end_benchmark("Accessing missing");

  start_benchmark();
  for (j = 0; j < count; j++) {
    key = malloc(8);
    snprintf(key, 8, "%lu", j);
    dictEntry *e = dictEntryDelete(dict, key, 0);
    assert(e != NULL);
    key[0] += 17; /* Change first number to letter. */
    int n = dictAdd(dict, key, (void *)j);
    assert(n == 0);
  }
  end_benchmark("Removing and adding");
}
#endif // DICT_BENCHMARK_MAIN
