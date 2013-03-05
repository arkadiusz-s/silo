#ifndef _SMALL_UNORDERED_MAP_H_
#define _SMALL_UNORDERED_MAP_H_

#include <algorithm>
#include <iterator>
#include <tr1/unordered_map>

#include "macros.h"

/**
 * For under SmallSize, uses linear probing on a fixed size array. Otherwise,
 * delegates to a regular std::unordered_map
 *
 * XXX(stephentu): Has imprecise destructor semantics. should fix this
 * XXX(stpehentu): allow custom allocator
 */
template <typename Key,
          typename T,
          size_t SmallSize = 128,
          typename Hash = std::tr1::hash<Key> >
class small_unordered_map {
public:
  typedef Key key_type;
  typedef T mapped_type;
  typedef std::pair<const key_type, mapped_type> value_type;
  typedef Hash hasher;
  typedef T & reference;
  typedef const T & const_reference;

private:
  typedef std::tr1::unordered_map<Key, T, Hash> large_table_type;
  typedef std::pair<key_type, mapped_type> bucket_value_type;

  struct bucket {
    bucket() : mapped(false) {}
    bool mapped;

    bucket_value_type *
    ptr()
    {
      return reinterpret_cast<bucket_value_type *>(&buf[0]);
    }

    const bucket_value_type *
    ptr() const
    {
      return reinterpret_cast<const bucket_value_type *>(&buf[0]);
    }

    bucket_value_type &
    ref()
    {
      return *ptr();
    }

    const bucket_value_type &
    ref() const
    {
      return *ptr();
    }

    void
    construct(const key_type &k, const mapped_type &v)
    {
      INVARIANT(!mapped);
      new (&buf[0]) bucket_value_type(k, v);
      mapped = true;
    }

    void
    destroy()
    {
      INVARIANT(mapped);
      ref().~bucket_value_type();
      mapped = false;
    }

    char buf[sizeof(value_type)];
  };

public:

  small_unordered_map()
    : n(0), large_elems(0)
  {
  }

  ~small_unordered_map()
  {
    clear();
  }

  small_unordered_map(const small_unordered_map &other)
    : n(other.n), large_elems()
  {
    if (unlikely(other.large_elems)) {
      large_elems = new large_table_type(*other.large_elems);
    } else {
      if (!n)
        return;
      for (size_t i = 0; i < SmallSize; i++) {
        bucket *const this_b = &small_elems[i];
        const bucket *const that_b = &other.small_elems[i];
        if (that_b->mapped)
          this_b->construct(that_b->ref().first,
                            that_b->ref().second);
      }
    }
  }

  small_unordered_map &
  operator=(const small_unordered_map &other)
  {
    // self assignment
    if (unlikely(this == &other))
      return *this;
    clear();
    n = other.n;
    if (unlikely(other.large_elems)) {
      large_elems = new large_table_type(*other.large_elems);
    } else {
      if (!n)
        return *this;
      for (size_t i = 0; i < SmallSize; i++) {
        bucket *const this_b = &small_elems[i];
        const bucket *const that_b = &other.small_elems[i];
        if (that_b->mapped)
          this_b->construct(that_b->ref().first,
                            that_b->ref().second);
      }
    }
    return *this;
  }

private:
  bucket *
  find_bucket(const key_type &k)
  {
    INVARIANT(!large_elems);
    size_t i = Hash()(k) % SmallSize;
    size_t n = 0;
    while (n++ < SmallSize) {
      bucket &b = small_elems[i];
      if ((b.mapped && b.ref().first == k) ||
          !b.mapped) {
        // found bucket
        return &b;
      }
      i = (i + 1) % SmallSize;
    }
    return 0;
  }

  const bucket *
  find_bucket(const key_type &k) const
  {
    return const_cast<small_unordered_map *>(this)->find_bucket(k);
  }

public:

  mapped_type &
  operator[](const key_type &k)
  {
    if (unlikely(large_elems))
      return large_elems->operator[](k);
    bucket *b = find_bucket(k);
    if (likely(b)) {
      if (!b->mapped) {
        n++;
        b->construct(k, mapped_type());
      }
      return b->ref().second;
    }
    INVARIANT(n == SmallSize);
    // small_elems is full, so spill over to large_elems
    n = 0;
    large_elems = new large_table_type;
    for (size_t idx = 0; idx < SmallSize; idx++) {
      bucket &b = small_elems[idx];
      INVARIANT(b.mapped);
      T &ref = large_elems->operator[](b.ref().first);
      std::swap(ref, b.ref().second);
      b.destroy();
    }
    return large_elems->operator[](k);
  }

  size_t
  size() const
  {
    if (unlikely(large_elems))
      return large_elems->size();
    return n;
  }

  bool
  empty() const
  {
    return size() == 0;
  }

private:
  // iterators are not stable across mutation
  template <typename SmallIterType,
            typename LargeIterType,
            typename ValueType>
  class iterator_ : public std::iterator<std::forward_iterator_tag, ValueType> {
    friend class small_unordered_map;
  public:
    iterator_() : large(false), b(0), bend(0) {}

    ValueType &
    operator*() const
    {
      if (unlikely(large))
        return *large_it;
      INVARIANT(b != bend);
      INVARIANT(b->mapped);
      return reinterpret_cast<ValueType &>(b->ref());
    }

    ValueType *
    operator->() const
    {
      if (unlikely(large))
        return &(*large_it);
      INVARIANT(b != bend);
      INVARIANT(b->mapped);
      return reinterpret_cast<ValueType *>(b->ptr());
    }

    bool
    operator==(const iterator_ &o) const
    {
      if (unlikely(large && o.large))
        return large_it == o.large_it;
      if (!large && !o.large)
        return b == o.b;
      return false;
    }

    bool
    operator!=(const iterator_ &o) const
    {
      return !operator==(o);
    }

    iterator_ &
    operator++()
    {
      if (unlikely(large)) {
        ++large_it;
        return *this;
      }
      INVARIANT(b < bend);
      do {
        b++;
      } while (b != bend && !b->mapped);
      return *this;
    }

    iterator_
    operator++(int)
    {
      iterator_ cur = *this;
      ++(*this);
      return cur;
    }

  protected:
    iterator_(SmallIterType *b, SmallIterType *bend)
      : large(false), b(b), bend(bend)
    {
      INVARIANT(b == bend || b->mapped);
    }
    iterator_(LargeIterType large_it)
      : large(true), large_it(large_it) {}

  private:
    bool large;
    SmallIterType *b;
    SmallIterType *bend;
    LargeIterType large_it;
  };

public:
  typedef
    iterator_<
      bucket,
      typename large_table_type::iterator,
      value_type>
    iterator;

  typedef
    iterator_<
      const bucket,
      typename large_table_type::const_iterator,
      const value_type>
    const_iterator;

  iterator
  begin()
  {
    if (unlikely(large_elems))
      return iterator(large_elems->begin());
    bucket *b = &small_elems[0];
    bucket *const bend = &small_elems[SmallSize];
    while (b != bend && !b->mapped)
      b++;
    return iterator(b, bend);
  }

  const_iterator
  begin() const
  {
    if (unlikely(large_elems))
      return const_iterator(large_elems->begin());
    const bucket *b = &small_elems[0];
    const bucket *const bend = &small_elems[SmallSize];
    while (b != bend && !b->mapped)
      b++;
    return const_iterator(b, bend);
  }

  iterator
  end()
  {
    if (unlikely(large_elems))
      return iterator(large_elems->end());
    return iterator(&small_elems[SmallSize], &small_elems[SmallSize]);
  }

  const_iterator
  end() const
  {
    if (unlikely(large_elems))
      return const_iterator(large_elems->end());
    return const_iterator(&small_elems[SmallSize], &small_elems[SmallSize]);
  }

  iterator
  find(const key_type &k)
  {
    if (unlikely(large_elems))
      return iterator(large_elems->find(k));
    bucket *b = find_bucket(k);
    if (likely(b) && b->mapped)
      return iterator(b, &small_elems[SmallSize]);
    return end();
  }

  const_iterator
  find(const key_type &k) const
  {
    if (unlikely(large_elems))
      return const_iterator(large_elems->find(k));
    const bucket *b = find_bucket(k);
    if (likely(b) && b->mapped)
      return const_iterator(b, &small_elems[SmallSize]);
    return end();
  }

  void
  clear()
  {
    if (unlikely(large_elems)) {
      INVARIANT(!n);
      delete large_elems;
      large_elems = NULL;
      return;
    }
    for (size_t i = 0; i < SmallSize; i++)
      if (small_elems[i].mapped)
        small_elems[i].destroy();
    n = 0;
  }

private:

  size_t n;
  bucket small_elems[SmallSize];
  large_table_type *large_elems;
};

#endif /* _SMALL_UNORDERED_MAP_H_ */