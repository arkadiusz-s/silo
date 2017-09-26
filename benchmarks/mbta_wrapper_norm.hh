#pragma once
#include <atomic>
#include <type_traits>
#include "abstract_db.h"
#include "abstract_ordered_index.h"
#include "sto/Transaction.hh"
#include "sto/MassTrans.hh"

#define OP_LOGGING 0

#include "sto/Hashtable.hh"
#include "sto/simple_str.hh"
#include "sto/StringWrapper.hh"
#include <unordered_map> 
//#include "tpcc.h"

#define STD_OP(f) \
  try { \
    f; \
  } catch (Transaction::Abort E) { \
    throw abstract_db::abstract_abort_exception(); \
  }


#if OP_LOGGING
std::atomic<long> mt_get(0);
std::atomic<long> mt_put(0);
std::atomic<long> mt_del(0);
std::atomic<long> mt_scan(0);
std::atomic<long> mt_rscan(0);
std::atomic<long> ht_get(0);
std::atomic<long> ht_put(0);
std::atomic<long> ht_insert(0);
std::atomic<long> ht_del(0);
#endif

class mbta_wrapper;

class mbta_ordered_index : public abstract_ordered_index {
public:
  mbta_ordered_index(const std::string &name, mbta_wrapper *db) : mbta(), name(name), db(db) {}

  std::string *arena(void);

    bool get(void *txn, lcdf::Str key, std::string &value, size_t max_bytes_read) {
#if OP_LOGGING
    mt_get++;
#endif
    STD_OP({
	// TODO: we'll still be faster if we just add support for max_bytes_read
        bool ret = mbta.transGet(key, value);
	// TODO: can we support this directly (max_bytes_read)? would avoid this wasted allocation
	return ret;
	  });
  }

  const char *put(void* txn,
                  lcdf::Str key,
                  const std::string &value)
  {
#if OP_LOGGING
    mt_put++;
#endif
    // TODO: there's an overload of put that takes non-const std::string and silo seems to use move for those.
    // may be worth investigating if we can use that optimization to avoid copying keys
    STD_OP({
        mbta.transPut(key, StringWrapper(value));
        return 0;
          });
  }
  
const char *insert(void *txn,
	     lcdf::Str key,
	     const std::string &value)
{
STD_OP(mbta.transInsert(key, StringWrapper(value)); return 0;)
}

void remove(void *txn, lcdf::Str key) {
#if OP_LOGGING
mt_del++;
#endif
STD_OP(mbta.transDelete(key));
}

void scan(void *txn,
    const std::string &start_key,
    const std::string *end_key,
    scan_callback &callback,
    str_arena *arena = nullptr) {
#if OP_LOGGING
mt_scan++;
#endif    
mbta_type::Str end = end_key ? mbta_type::Str(*end_key) : mbta_type::Str();
STD_OP(mbta.transQuery(start_key, end, [&] (mbta_type::Str key, std::string& value) {
  return callback.invoke(key.data(), key.length(), value);
}, arena));
}

void rscan(void *txn,
     const std::string &start_key,
     const std::string *end_key,
     scan_callback &callback,
     str_arena *arena = nullptr) {
#if 1
#if OP_LOGGING
mt_rscan++;
#endif
mbta_type::Str end = end_key ? mbta_type::Str(*end_key) : mbta_type::Str();
STD_OP(mbta.transRQuery(start_key, end, [&] (mbta_type::Str key, std::string& value) {
  return callback.invoke(key.data(), key.length(), value);
}, arena));
#endif
}

size_t size() const
{
return mbta.approx_size();
}

// TODO: unclear if we need to implement, apparently this should clear the tree and possibly return some stats
std::map<std::string, uint64_t>
clear() {
throw 2;
}

#if STO_OPACITY
typedef MassTrans<std::string, versioned_value_struct<std::string, true>, true/*opacity*/> mbta_type;
#else
typedef MassTrans<std::string, versioned_value_struct<std::string, false>, false/*opacity*/> mbta_type;
#endif
private:
friend class mbta_wrapper;
mbta_type mbta;

const std::string name;

mbta_wrapper *db;

};

#if 0
class ht_ordered_index_string : public abstract_ordered_index {
public:
ht_ordered_index_string(const std::string &name, mbta_wrapper *db) : ht(), name(name), db(db) {}

std::string *arena(void);

bool get(void *txn, lcdf::Str key, std::string &value, size_t max_bytes_read) {
#if OP_LOGGING
ht_get++;
#endif
STD_OP({
// TODO: we'll still be faster if we just add support for max_bytes_read
bool ret = ht.transGet(key, value);
// TODO: can we support this directly (max_bytes_read)? would avoid this wasted allocation
return ret;
  });
}

const char *put(
void* txn,
const lcdf::Str key,
const std::string &value)
{
#if OP_LOGGING
ht_put++;
#endif
// TODO: there's an overload of put that takes non-const std::string and silo seems to use move for those.
// may be worth investigating if we can use that optimization to avoid copying keys
STD_OP({
ht.transPut(key, StringWrapper(value));
        return 0;
          });
  }

  const char *insert(void *txn,
                     lcdf::Str key,
                     const std::string &value)
  {
#if OP_LOGGING
    ht_insert++;
#endif
    STD_OP({
	ht.transPut(key, StringWrapper(value)); return 0;
	});
  }

  void remove(void *txn, lcdf::Str key) {
#if OP_LOGGING
    ht_del++;
#endif    
    STD_OP({
	ht.transDelete(key);
    });
  }

  void scan(void *txn,
            const std::string &start_key,
            const std::string *end_key,
            scan_callback &callback,
            str_arena *arena = nullptr) {
    NDB_UNIMPLEMENTED("scan");
  }

  void rscan(void *txn,
             const std::string &start_key,
             const std::string *end_key,
             scan_callback &callback,
             str_arena *arena = nullptr) {
    NDB_UNIMPLEMENTED("rscan");
  }

  size_t size() const
  {
    return 0;
  }

  // TODO: unclear if we need to implement, apparently this should clear the tree and possibly return some stats
  std::map<std::string, uint64_t>
  clear() {
    throw 2;
  }

  typedef Hashtable<std::string, std::string, false/*opacity*/, 999983, simple_str> ht_type;
private:
  friend class mbta_wrapper;
  ht_type ht;

  const std::string name;

  mbta_wrapper *db;

};
#endif

template <class KeyType, unsigned InitSize>
class ht_ordered_index : public abstract_ordered_index {
public:
    static constexpr bool key_type_trivial = std::is_trivially_copyable<KeyType>::value;
    static constexpr bool key_type_conversion =
        !(std::is_same<KeyType, int32_t>::value || std::is_same<KeyType, std::string>::value);

#if STO_OPACITY
    typedef Hashtable<KeyType, std::string, true/*opacity*/, InitSize, simple_str> ht_type;
#else
    typedef Hashtable<KeyType, std::string, false/*opacity*/, InitSize, simple_str> ht_type;
#endif

    ht_ordered_index(const std::string &name, mbta_wrapper *db) : ht(), name(name), db(db) {}

    std::string *arena(void);

    bool get(void *txn, lcdf::Str key, std::string& value, size_t max_bytes_read) {
#if OP_LOGGING
        ht_get++;
#endif
        STD_OP({
            assert(key.length() == sizeof(KeyType));
            const KeyType& k = *reinterpret_cast<const KeyType *>(key.data());
            bool ret = ht.transGet(k, value);
            return ret;
        });
    }
    bool get(void *txn, KeyType key, std::string& value, size_t max_bytes_read = std::string::npos) {
#if OP_LOGGING
        ht_get++;
#endif
        STD_OP({
            bool ret = ht.transGet(key, value);
            return ret;
        });
    }

    const char *put(void *txn, lcdf::Str key, const std::string& value) {
#if OP_LOGGING
        ht_put++;
#endif
        STD_OP({
            assert(key.length() == sizeof(KeyType));
            const KeyType& k = *reinterpret_cast<const KeyType *>(key.data());
            ht.transPut(k, StringWrapper(value));
            return nullptr;
        });
    }
    const char *put(void *txn, KeyType key, const std::string& value) {
#if OP_LOGGING
        ht_put++;
#endif
        STD_OP({
            ht.transPut(key, StringWrapper(value));
            return nullptr;
        });
    }

    const char *insert(void *txn, lcdf::Str key, const std::string& value) {
#if OP_LOGGING
        ht_insert++;
#endif
        STD_OP({
            assert(key.length() == sizeof(KeyType));
            const KeyType& k = *reinterpret_cast<const KeyType *>(key.data());
            ht.transPut(k, StringWrapper(value));
            return nullptr;
        })
    }
    const char *insert(void *txn, KeyType key, const std::string& value) {
#if OP_LOGGING
        ht_insert++;
#endif
        STD_OP({
            ht.transPut(key, StringWrapper(value));
            return nullptr;
        });
    }

    void remove(void *txn, lcdf::Str key) {
#if OP_LOGGING
        ht_del++;
#endif
        STD_OP({
            assert(key.length() == sizeof(KeyType));
            const KeyType& k = *reinterpret_cast<const KeyType *>(key.data());
            ht.transDelete(k);
        });
    }
    void remove(void *txn, KeyType key) {
#if OP_LOGGING
        ht_del++;
#endif
        STD_OP({
            ht.transDelete(key);
        });
    }

    void scan(void *, const std::string&, const std::string *, scan_callback&, str_arena *) {
        NDB_UNIMPLEMENTED("scan");
    }
    void rscan(void *, const std::string&, const std::string *, scan_callback&, str_arena *) {
        NDB_UNIMPLEMENTED("rscan");
    }

    size_t size() const {
        return 0;
    }

    std::map<std::string, uint64_t> clear() {
        throw 2;
    }

    void print_stats() {
        std::cout << "Hashtable " << name << ": ";
        ht.print_stats();
    }

private:
    friend class mbta_wrapper;
    ht_type ht;
    const std::string name;
    mbta_wrapper *db;
};

typedef ht_ordered_index<std::string, 999983>   ht_ordered_index_string;
typedef ht_ordered_index<int32_t, 227497>       ht_ordered_index_int; // used by "item" and "warehouse" tables
typedef ht_ordered_index<customer_key, 999983>  ht_ordered_index_customer_key;
typedef ht_ordered_index<history_key, 20000003> ht_ordered_index_history_key;
typedef ht_ordered_index<oorder_key, 20000003>  ht_ordered_index_oorder_key;
typedef ht_ordered_index<stock_key, 3000017>    ht_ordered_index_stock_key;

typedef ht_ordered_index<customer_name_idx_key, 999983> ht_ordered_index_customer_ni_key;
typedef ht_ordered_index<district_key, 999983> ht_ordered_index_district_key;
typedef ht_ordered_index<new_order_key, 999983> ht_ordered_index_new_order_key;
typedef ht_ordered_index<oorder_c_id_idx_key, 999983> ht_ordered_index_oorder_cidx_key;
typedef ht_ordered_index<order_line_key, 999983> ht_ordered_index_order_line_key;

#if 0
class ht_ordered_index_int : public abstract_ordered_index {
public:
  ht_ordered_index_int(const std::string &name, mbta_wrapper *db) : ht(), name(name), db(db) {}

  std::string *arena(void);

  bool get(void *txn, lcdf::Str key, std::string &value, size_t max_bytes_read) {
    return false;
  }

  bool get(
      void *txn,
      int32_t key,
      std::string &value,
      size_t max_bytes_read = std::string::npos) {
#if OP_LOGGING
    ht_get++;
#endif
    STD_OP({
        bool ret = ht.transGet(key, value);
        return ret;
          });

  }


  const char *put(
      void* txn,
      lcdf::Str key,
      const std::string &value)
  {
    return 0;
  }

  const char *put(
      void* txn,
      int32_t key,
      const std::string &value)
  {
#if OP_LOGGING
    ht_put++;
#endif
    STD_OP({
        ht.transPut(key, StringWrapper(value));
        return 0;
          });
  }

  
  const char *insert(void *txn,
                     lcdf::Str key,
                     const std::string &value)
  {
    return 0;
  }

  const char *insert(void *txn,
                     int32_t key,
                     const std::string &value)
  {
#if OP_LOGGING
    ht_insert++;
#endif
    STD_OP({
        ht.transPut(key, StringWrapper(value)); return 0;});
  }


  void remove(void *txn, lcdf::Str key) {
      return;
  }

  void remove(void *txn, int32_t key) {
#if OP_LOGGING
    ht_del++;
#endif    
    STD_OP({
        ht.transDelete(key);});
  }     

  void scan(void *txn,
            const std::string &start_key,
            const std::string *end_key,
            scan_callback &callback,
            str_arena *arena = nullptr) {
    NDB_UNIMPLEMENTED("scan");
  }

  void rscan(void *txn,
             const std::string &start_key,
             const std::string *end_key,
             scan_callback &callback,
             str_arena *arena = nullptr) {
    NDB_UNIMPLEMENTED("rscan");
  }

  size_t size() const
  {
    return 0;
  }

  // TODO: unclear if we need to implement, apparently this should clear the tree and possibly return some stats
  std::map<std::string, uint64_t>
  clear() {
    throw 2;
  }

  void print_stats() {
    printf("Hashtable %s: ", name.data());
    ht.print_stats();
  }

  typedef Hashtable<int32_t, std::string, false/*opacity*/, 227497, simple_str> ht_type;
  //typedef std::unordered_map<K, std::string> ht_type;
private:
  friend class mbta_wrapper;
  ht_type ht;

  const std::string name;

  mbta_wrapper *db;

};

class ht_ordered_index_customer_key : public abstract_ordered_index {
public:
  ht_ordered_index_customer_key(const std::string &name, mbta_wrapper *db) : ht(), name(name), db(db) {}

  std::string *arena(void);

  bool get(void *txn, lcdf::Str key, std::string &value, size_t max_bytes_read) {
    return false;
  }

  bool get(
      void *txn,
      customer_key key,
      std::string &value,
      size_t max_bytes_read = std::string::npos) {
#if OP_LOGGING
    ht_get++;
#endif
    STD_OP({
        bool ret = ht.transGet(key, value);
        return ret;
          });

  }


  const char *put(
      void* txn,
      lcdf::Str key,
      const std::string &value)
  {
    return 0;
  }

  const char *put(
      void* txn,
      customer_key key,
      const std::string &value)
  {
#if OP_LOGGING
    ht_put++;
#endif
    STD_OP({
        ht.transPut(key, StringWrapper(value));
        return 0;
          });
  }

  
  const char *insert(void *txn,
                     lcdf::Str key,
                     const std::string &value)
  {
    return 0;
  }

  const char *insert(void *txn,
                     customer_key key,
                     const std::string &value)
  {
#if OP_LOGGING
    ht_insert++;
#endif
    STD_OP({
        ht.transPut(key, StringWrapper(value)); return 0;});
  }


  void remove(void *txn, lcdf::Str key) {
      return;
  }

  void remove(void *txn, customer_key key) {
#if OP_LOGGING
    ht_del++;
#endif    
    STD_OP({
        ht.transDelete(key);});
  }     

  void scan(void *txn,
            const std::string &start_key,
            const std::string *end_key,
            scan_callback &callback,
            str_arena *arena = nullptr) {
    NDB_UNIMPLEMENTED("scan");
  }

  void rscan(void *txn,
             const std::string &start_key,
             const std::string *end_key,
             scan_callback &callback,
             str_arena *arena = nullptr) {
    NDB_UNIMPLEMENTED("rscan");
  }

  size_t size() const
  {
    return 0;
  }

  // TODO: unclear if we need to implement, apparently this should clear the tree and possibly return some stats
  std::map<std::string, uint64_t>
  clear() {
    throw 2;
  }
  
   void print_stats() {
    printf("Hashtable %s: ", name.data());
    ht.print_stats();
  }

  typedef Hashtable<customer_key, std::string, false/*opacity*/, 999983, simple_str> ht_type;
  //typedef std::unordered_map<K, std::string> ht_type;
private:
  friend class mbta_wrapper;
  ht_type ht;

  const std::string name;

  mbta_wrapper *db;

};

class ht_ordered_index_history_key : public abstract_ordered_index {
public:
  ht_ordered_index_history_key(const std::string &name, mbta_wrapper *db) : ht(), name(name), db(db) {}

  std::string *arena(void);

  bool get(
      void *txn,
      lcdf::Str key,
      std::string &value,
      size_t max_bytes_read = std::string::npos) {
#if OP_LOGGING
    ht_get++;
#endif
    STD_OP({
        assert(key.length() == sizeof(history_key));
        const history_key& k = *(reinterpret_cast<const history_key*>(key.data())); 
        bool ret = ht.transGet(k, value);
        return ret;
          });

  }
  
  const char *put(
      void* txn,
      lcdf::Str key,
      const std::string &value)
  {
#if OP_LOGGING
    ht_put++;
#endif
    STD_OP({
        assert(key.length() == sizeof(history_key));
        const history_key& k = *(reinterpret_cast<const history_key*>(key.data()));
        ht.transPut(k, StringWrapper(value));
        return 0;
          });
  }

  const char *insert(void *txn,
                     lcdf::Str key,
                     const std::string &value)
  {
#if OP_LOGGING
    ht_insert++;
#endif
    STD_OP({
        assert(key.length() == sizeof(history_key));
        const history_key& k = *(reinterpret_cast<const history_key*>(key.data()));
        ht.transPut(k, StringWrapper(value)); return 0;});
  }

  void remove(void *txn, lcdf::Str key) {
#if OP_LOGGING
    ht_del++;
#endif    
    STD_OP({
        assert(key.length() == sizeof(history_key));
        const history_key& k = *(reinterpret_cast<const history_key*>(key.data()));
        ht.transDelete(k);});
  }     

  void scan(void *txn,
            const std::string &start_key,
            const std::string *end_key,
            scan_callback &callback,
            str_arena *arena = nullptr) {
    NDB_UNIMPLEMENTED("scan");
  }

  void rscan(void *txn,
             const std::string &start_key,
             const std::string *end_key,
             scan_callback &callback,
             str_arena *arena = nullptr) {
    NDB_UNIMPLEMENTED("rscan");
  }

  size_t size() const
  {
    return 0;
  }

  // TODO: unclear if we need to implement, apparently this should clear the tree and possibly return some stats
  std::map<std::string, uint64_t>
  clear() {
    throw 2;
  }

   void print_stats() {
    printf("Hashtable %s: ", name.data());
    ht.print_stats();
  }

  typedef Hashtable<history_key, std::string, false/*opacity*/, 20000003, simple_str> ht_type;
private:
  friend class mbta_wrapper;
  ht_type ht;

  const std::string name;

  mbta_wrapper *db;

};

class ht_ordered_index_oorder_key : public abstract_ordered_index {
public:
  ht_ordered_index_oorder_key(const std::string &name, mbta_wrapper *db) : ht(), name(name), db(db) {}

  std::string *arena(void);

  bool get(
      void *txn,
      lcdf::Str key,
      std::string &value,
      size_t max_bytes_read = std::string::npos) {
#if OP_LOGGING
    ht_get++;
#endif
    STD_OP({
        assert(key.length() == sizeof(oorder_key));
        const oorder_key& k = *(reinterpret_cast<const oorder_key*>(key.data())); 
        bool ret = ht.transGet(k, value);
        return ret;
          });

  }
  
  const char *put(
      void* txn,
      lcdf::Str key,
      const std::string &value)
  {
#if OP_LOGGING
    ht_put++;
#endif
    STD_OP({
        assert(key.length() == sizeof(oorder_key));
        const oorder_key& k = *(reinterpret_cast<const oorder_key*>(key.data()));
        ht.transPut(k, StringWrapper(value));
        return 0;
          });
  }

  const char *insert(void *txn,
                     lcdf::Str key,
                     const std::string &value)
  {
#if OP_LOGGING
    ht_insert++;
#endif
    STD_OP({
        assert(key.length() == sizeof(oorder_key));
        const oorder_key& k = *(reinterpret_cast<const oorder_key*>(key.data()));
        ht.transPut(k, StringWrapper(value)); return 0;});
  }

  void remove(void *txn, lcdf::Str key) {
#if OP_LOGGING
    ht_del++;
#endif    
    STD_OP({
        assert(key.length() == sizeof(oorder_key));
        const oorder_key& k = *(reinterpret_cast<const oorder_key*>(key.data()));
        ht.transDelete(k);});
  }     

  void scan(void *txn,
            const std::string &start_key,
            const std::string *end_key,
            scan_callback &callback,
            str_arena *arena = nullptr) {
    NDB_UNIMPLEMENTED("scan");
  }

  void rscan(void *txn,
             const std::string &start_key,
             const std::string *end_key,
             scan_callback &callback,
             str_arena *arena = nullptr) {
    NDB_UNIMPLEMENTED("rscan");
  }

  size_t size() const
  {
    return 0;
  }

  // TODO: unclear if we need to implement, apparently this should clear the tree and possibly return some stats
  std::map<std::string, uint64_t>
  clear() {
    throw 2;
  }

   void print_stats() {
    printf("Hashtable %s: ", name.data());
    ht.print_stats();
  }


  typedef Hashtable<oorder_key, std::string, false/*opacity*/, 20000003, simple_str> ht_type;
private:
  friend class mbta_wrapper;
  ht_type ht;

  const std::string name;

  mbta_wrapper *db;

};

class ht_ordered_index_stock_key : public abstract_ordered_index {
public:
  ht_ordered_index_stock_key(const std::string &name, mbta_wrapper *db) : ht(), name(name), db(db) {}

  std::string *arena(void);

  bool get(
      void *txn,
      lcdf::Str key,
      std::string &value,
      size_t max_bytes_read = std::string::npos) {
#if OP_LOGGING
    ht_get++;
#endif
    STD_OP({
        assert(key.length() == sizeof(stock_key));
        const stock_key& k = *(reinterpret_cast<const stock_key*>(key.data())); 
        bool ret = ht.transGet(k, value);
        return ret;
          });

  }
  
  const char *put(
      void* txn,
      lcdf::Str key,
      const std::string &value)
  {
#if OP_LOGGING
    ht_put++;
#endif
    STD_OP({
        assert(key.length() == sizeof(stock_key));
        const stock_key& k = *(reinterpret_cast<const stock_key*>(key.data()));
        ht.transPut(k, StringWrapper(value));
        return 0;
          });
  }

  const char *insert(void *txn,
                     lcdf::Str key,
                     const std::string &value)
  {
#if OP_LOGGING
    ht_insert++;
#endif
    STD_OP({
        assert(key.length() == sizeof(stock_key));
        const stock_key& k = *(reinterpret_cast<const stock_key*>(key.data()));
        ht.transPut(k, StringWrapper(value)); return 0;});
  }

  void remove(void *txn, lcdf::Str key) {
#if OP_LOGGING
    ht_del++;
#endif    
    STD_OP({
        assert(key.length() == sizeof(stock_key));
        const stock_key& k = *(reinterpret_cast<const stock_key*>(key.data()));
        ht.transDelete(k);});
  }     

  void scan(void *txn,
            const std::string &start_key,
            const std::string *end_key,
            scan_callback &callback,
            str_arena *arena = nullptr) {
    NDB_UNIMPLEMENTED("scan");
  }

  void rscan(void *txn,
             const std::string &start_key,
             const std::string *end_key,
             scan_callback &callback,
             str_arena *arena = nullptr) {
    NDB_UNIMPLEMENTED("rscan");
  }

  size_t size() const
  {
    return 0;
  }

  // TODO: unclear if we need to implement, apparently this should clear the tree and possibly return some stats
  std::map<std::string, uint64_t>
  clear() {
    throw 2;
  }

   void print_stats() {
    printf("Hashtable %s: ", name.data());
    ht.print_stats();
  }

  typedef Hashtable<stock_key, std::string, false/*opacity*/, 3000017, simple_str> ht_type;
private:
  friend class mbta_wrapper;
  ht_type ht;

  const std::string name;

  mbta_wrapper *db;

};
#endif

class mbta_wrapper : public abstract_db {
public:
  ssize_t txn_max_batch_size() const OVERRIDE { return 100; }
  
  void
  do_txn_epoch_sync() const
  {
    //txn_epoch_sync<Transaction>::sync();
  }

  void
  do_txn_finish() const
  {
#if PERF_LOGGING
    Transaction::print_stats();
    //    printf("v: %lu, k %lu, ref %lu, read %lu\n", version_mallocs, key_mallocs, ref_mallocs, read_mallocs);
   {
        using thd = threadinfo_t;
        thd tc = Transaction::tinfo_combined();
        printf("total_n: %llu, total_r: %llu, total_w: %llu, total_searched: %llu, total_aborts: %llu (%llu aborts at commit time), rdata_size: %llu, wdata_size: %llu\n", tc.p(txp_total_n), tc.p(txp_total_r), tc.p(txp_total_w), tc.p(txp_total_searched), tc.p(txp_total_aborts), tc.p(txp_commit_time_aborts), tc.p(txp_max_rdata_size), tc.p(txp_max_wdata_size));
    }

#endif
#if OP_LOGGING
    printf("mt_get: %ld, mt_put: %ld, mt_del: %ld, mt_scan: %ld, mt_rscan: %ld, ht_get: %ld, ht_put: %ld, ht_insert: %ld, ht_del: %ld\n", mt_get.load(), mt_put.load(), mt_del.load(), mt_scan.load(), mt_rscan.load(), ht_get.load(), ht_put.load(), ht_insert.load(), ht_del.load());
#endif 
    //txn_epoch_sync<Transaction>::finish();
  }

  void
  thread_init(bool loader)
  {
    static int tidcounter = 0;
    TThread::set_id(__sync_fetch_and_add(&tidcounter, 1));
    if (TThread::id() == 0) {
      // someone has to do this (they don't provide us with a general init callback)
      mbta_ordered_index::mbta_type::static_init();
      // need this too
      pthread_t advancer;
      pthread_create(&advancer, NULL, Transaction::epoch_advancer, NULL);
      pthread_detach(advancer);
    }
    mbta_ordered_index::mbta_type::thread_init();
  }

  void
  thread_end()
  {

  }

  size_t
  sizeof_txn_object(uint64_t txn_flags) const
  {
    return sizeof(Transaction);
  }

  static __thread str_arena *thr_arena;
  void *new_txn(
                uint64_t txn_flags,
                str_arena &arena,
                void *buf,
                TxnProfileHint hint = HINT_DEFAULT) {
    Sto::start_transaction();
    thr_arena = &arena;
    return NULL;
  }

  bool commit_txn(void *txn) {
    return Sto::try_commit();
  }

  void abort_txn(void *txn) {
    Sto::silent_abort();
  }

  abstract_ordered_index *
  open_index(const std::string &name,
             size_t value_size_hint,
             bool mostly_append = false,
             bool use_hashtable = false) {
    if (use_hashtable) {
      if (name == "customer")
        return new ht_ordered_index_customer_key(name, this);
      if (name == "customer_name_idx")
        return new ht_ordered_index_customer_ni_key(name, this);
      if (name == "district")
        return new ht_ordered_index_district_key(name, this);
      if (name == "item")
        return new ht_ordered_index_int(name, this);
      if (name == "history")
        return new ht_ordered_index_history_key(name, this);
      if (name == "new_order")
        return new ht_ordered_index_new_order_key(name, this);
      if (name == "oorder")
        return new ht_ordered_index_oorder_key(name, this);
      if (name == "oorder_c_id_idx")
        return new ht_ordered_index_oorder_cidx_key(name, this);
      if (name == "order_line")
        return new ht_ordered_index_order_line_key(name, this);
      if (name == "stock")
        return new ht_ordered_index_stock_key(name, this);
      if (name == "stock_data")
        return new ht_ordered_index_stock_key(name, this);
      if (name == "warehouse")
        return new ht_ordered_index_int(name, this);
      std::cerr << "unknown index: " << name << std::endl;
      assert(false);
      return new ht_ordered_index_int(name, this);
    }
    auto ret = new mbta_ordered_index(name, this);
    return ret;
  }

 void
 close_index(abstract_ordered_index *idx) {
   delete idx;
 }

};

__thread str_arena* mbta_wrapper::thr_arena;

std::string *mbta_ordered_index::arena() {
  return (*db->thr_arena)();
}
