#ifndef __DICT_H__
#define __DICT_H__

typedef struct dictEntry {
  void *key;
  void *val;
  struct dictEntry *next;
} dictEntry;

// The following fields must be provided: hashFn, keyCmp.
typedef struct dictType {
  uint64_t (*hashFn)(const void *key);
  int (*keyCmp)(const void *key1, const void *key2);
  void *(*keyDup)(const void *key);
  void *(*valDup)(const void *obj);
  void (*keyDestructor)(void *key);
  void (*valDestructor)(void *val);
} dictType;

typedef struct dictht {
  dictEntry **entries;
  uint64_t size;
  uint64_t mask;
  uint64_t filled;
} dictht;

typedef struct dict {
  dictType type;
  dictht ht[2];
  int64_t rehash;
  uint64_t iters;
} dict;

typedef struct dictIterator {
  dict *d;
  int64_t i;
  dictEntry *entry;
  dictEntry *nextEntry;
  dictht *ht;
} dictIterator;

#define dictValSet(d, entry, val)                                              \
  do {                                                                         \
    if ((d)->type.valDup)                                                      \
      (entry)->val = (d)->type.valDup(val);                                    \
    else                                                                       \
      (entry)->val = (val);                                                    \
  } while (0)

#define dictValGet(entry) ((entry)->val)

#define dictValFree(d, entry)                                                  \
  if ((d)->type.valDestructor)                                                 \
  (d)->type.valDestructor((entry)->val)

#define dictKeySet(d, entry, key)                                              \
  do {                                                                         \
    if ((d)->type.keyDup)                                                      \
      (entry)->key = (d)->type.keyDup(key);                                    \
    else                                                                       \
      (entry)->key = (key);                                                    \
  } while (0)

#define dictKeyGet(entry) ((entry)->key)

#define dictKeyFree(d, entry)                                                  \
  if ((d)->type.keyDestructor)                                                 \
  (d)->type.keyDestructor((entry)->key)

#define dictKeyCmp(d, key1, key2) ((d)->type.keyCmp(key1, key2))

#define dictHashKeyGet(d, key) (d)->type.hashFn(key)

#define dictSize(d) ((d)->ht[0].size + (d)->ht[1].size)
#define dictFilled(d) ((d)->ht[0].filled + (d)->ht[1].filled)

#define dictRehashing(d) ((d)->rehash != -1)

dict *dictCreate(dictType *type);
int dictDestroy(dict *d);

int dictAdd(dict *d, void *key, void *val);
dictEntry *dictAddKeyOrGetExistingEntry(dict *d, void *key,
                                        dictEntry **existing);
int dictUpdateOrAdd(dict *d, void *key, void *val);
dictEntry *dictEntryDelete(dict *d, const void *key, int noEntryFree);
int dictEntryFree(dict *d, dictEntry *he);

dictEntry *dictFind(dict *d, const void *key);

dictIterator *dictIteratorCreate(dict *d);
dictIterator *dictIteratorSuspendRehashCreate(dict *d);
int dictIteratorDestroy(dictIterator *iter);
dictEntry *dictIteratorNext(dictIterator *iter);
int dictIteratorRewind(dictIterator *iter);

int dictStats(dict *d, char *buf, size_t bufsize);
int dictRehashms(dict *d, int ms);
uint64_t dictHashFnDefault(const void *key, int len);

#endif // __DICT_H__
