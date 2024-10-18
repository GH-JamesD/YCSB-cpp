//
//  rocksdb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Modifications Copyright 2023 Chengye YU <yuchengye2013 AT outlook.com>.
//

#include "rocksdb_db.h"
#include <thread>
#include <iostream>
#include <chrono>
#include "core/core_workload.h"
#include "core/db_factory.h"
#include "utils/utils.h"

#include <rocksdb/cache.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/status.h>
#include <rocksdb/utilities/options_util.h>
#include <rocksdb/write_batch.h>

#include <boost/asio.hpp>
#include <vector>
#include "json.hpp"  // For handling JSON

using boost::asio::ip::tcp;
using json = nlohmann::json;

using json = nlohmann::json;

namespace {
  const std::string PROP_NAME = "rocksdb.dbname";
  const std::string PROP_NAME_DEFAULT = "";

  const std::string PROP_FORMAT = "rocksdb.format";
  const std::string PROP_FORMAT_DEFAULT = "single";

  const std::string PROP_MERGEUPDATE = "rocksdb.mergeupdate";
  const std::string PROP_MERGEUPDATE_DEFAULT = "false";

  const std::string PROP_DESTROY = "rocksdb.destroy";
  const std::string PROP_DESTROY_DEFAULT = "false";

  const std::string PROP_COMPRESSION = "rocksdb.compression";
  const std::string PROP_COMPRESSION_DEFAULT = "no";

  const std::string PROP_MAX_BG_JOBS = "rocksdb.max_background_jobs";
  const std::string PROP_MAX_BG_JOBS_DEFAULT = "0";

  const std::string PROP_TARGET_FILE_SIZE_BASE = "rocksdb.target_file_size_base";
  const std::string PROP_TARGET_FILE_SIZE_BASE_DEFAULT = "0";

  const std::string PROP_TARGET_FILE_SIZE_MULT = "rocksdb.target_file_size_multiplier";
  const std::string PROP_TARGET_FILE_SIZE_MULT_DEFAULT = "0";

  const std::string PROP_MAX_BYTES_FOR_LEVEL_BASE = "rocksdb.max_bytes_for_level_base";
  const std::string PROP_MAX_BYTES_FOR_LEVEL_BASE_DEFAULT = "0";

  const std::string PROP_WRITE_BUFFER_SIZE = "rocksdb.write_buffer_size";
  const std::string PROP_WRITE_BUFFER_SIZE_DEFAULT = "0";

  const std::string PROP_MAX_WRITE_BUFFER = "rocksdb.max_write_buffer_number";
  const std::string PROP_MAX_WRITE_BUFFER_DEFAULT = "0";

  const std::string PROP_COMPACTION_PRI = "rocksdb.compaction_pri";
  const std::string PROP_COMPACTION_PRI_DEFAULT = "-1";

  const std::string PROP_MAX_OPEN_FILES = "rocksdb.max_open_files";
  const std::string PROP_MAX_OPEN_FILES_DEFAULT = "-1";

  const std::string PROP_L0_COMPACTION_TRIGGER = "rocksdb.level0_file_num_compaction_trigger";
  const std::string PROP_L0_COMPACTION_TRIGGER_DEFAULT = "0";

  const std::string PROP_L0_SLOWDOWN_TRIGGER = "rocksdb.level0_slowdown_writes_trigger";
  const std::string PROP_L0_SLOWDOWN_TRIGGER_DEFAULT = "0";

  const std::string PROP_L0_STOP_TRIGGER = "rocksdb.level0_stop_writes_trigger";
  const std::string PROP_L0_STOP_TRIGGER_DEFAULT = "0";

  const std::string PROP_USE_DIRECT_WRITE = "rocksdb.use_direct_io_for_flush_compaction";
  const std::string PROP_USE_DIRECT_WRITE_DEFAULT = "false";

  const std::string PROP_USE_DIRECT_READ = "rocksdb.use_direct_reads";
  const std::string PROP_USE_DIRECT_READ_DEFAULT = "false";

  const std::string PROP_USE_MMAP_WRITE = "rocksdb.allow_mmap_writes";
  const std::string PROP_USE_MMAP_WRITE_DEFAULT = "false";

  const std::string PROP_USE_MMAP_READ = "rocksdb.allow_mmap_reads";
  const std::string PROP_USE_MMAP_READ_DEFAULT = "false";

  const std::string PROP_CACHE_SIZE = "rocksdb.cache_size";
  const std::string PROP_CACHE_SIZE_DEFAULT = "0";

  const std::string PROP_COMPRESSED_CACHE_SIZE = "rocksdb.compressed_cache_size";
  const std::string PROP_COMPRESSED_CACHE_SIZE_DEFAULT = "0";

  const std::string PROP_BLOOM_BITS = "rocksdb.bloom_bits";
  const std::string PROP_BLOOM_BITS_DEFAULT = "0";

  const std::string PROP_INCREASE_PARALLELISM = "rocksdb.increase_parallelism";
  const std::string PROP_INCREASE_PARALLELISM_DEFAULT = "false";

  const std::string PROP_OPTIMIZE_LEVELCOMP = "rocksdb.optimize_level_style_compaction";
  const std::string PROP_OPTIMIZE_LEVELCOMP_DEFAULT = "false";

  const std::string PROP_OPTIONS_FILE = "rocksdb.optionsfile";
  const std::string PROP_OPTIONS_FILE_DEFAULT = "";

  const std::string PROP_ENV_URI = "rocksdb.env_uri";
  const std::string PROP_ENV_URI_DEFAULT = "";

  const std::string PROP_FS_URI = "rocksdb.fs_uri";
  const std::string PROP_FS_URI_DEFAULT = "";

  static std::shared_ptr<rocksdb::Env> env_guard;
  static std::shared_ptr<rocksdb::Cache> block_cache;
#if ROCKSDB_MAJOR < 8
  static std::shared_ptr<rocksdb::Cache> block_cache_compressed;
#endif
} // anonymous

namespace ycsbc {


  enum class ResultStatus {
    OK,        // Operation successful
    ERROR      // Operation failed or encountered an issue
  };
  // Struct for handling responses for each requester
  struct ResponseData {
      std::string resultData;
      ResultStatus status;

      ResponseData() : status(ResultStatus::ERROR) {}
  };

std::string getCurrentThreadIdAsString() {
    std::thread::id threadId = std::this_thread::get_id();  // Get the current thread ID
    std::stringstream ss;
    ss << threadId;  // Insert thread ID into stringstream
    return ss.str();  // Convert the stringstream into a string
}

std::vector<rocksdb::ColumnFamilyHandle *> RocksdbDB::cf_handles_;
rocksdb::DB *RocksdbDB::db_ = nullptr;
int RocksdbDB::ref_cnt_ = 0;
std::mutex RocksdbDB::mu_;
std::string db_path;

void RocksdbDB::Init() {
// merge operator disabled by default due to link error

#ifdef USE_MERGEUPDATE
  class YCSBUpdateMerge : public rocksdb::AssociativeMergeOperator {
   public:
    virtual bool Merge(const rocksdb::Slice &key, const rocksdb::Slice *existing_value,
                       const rocksdb::Slice &value, std::string *new_value,
                       rocksdb::Logger *logger) const override {
      assert(existing_value);

      std::vector<Field> values;
      const char *p = existing_value->data();
      const char *lim = p + existing_value->size();
      DeserializeRow(values, p, lim);

      std::vector<Field> new_values;
      p = value.data();
      lim = p + value.size();
      DeserializeRow(new_values, p, lim);

      for (Field &new_field : new_values) {
        bool found = false;
        for (Field &field : values) {
          if (field.name == new_field.name) {
            found = true;
            field.value = new_field.value;
            break;
          }
        }
        if (!found) {
          values.push_back(new_field);
        }
      }

      SerializeRow(values, *new_value);
      return true;
    }

    virtual const char *Name() const override {
      return "YCSBUpdateMerge";
    }
  };
#endif
  const std::lock_guard<std::mutex> lock(mu_);

  const utils::Properties &props = *props_;
  const std::string format = props.GetProperty(PROP_FORMAT, PROP_FORMAT_DEFAULT);
  if (format == "single") {
    format_ = kSingleRow;
    method_read_ = &RocksdbDB::ReadSingle;
    method_scan_ = &RocksdbDB::ScanSingle;
    method_update_ = &RocksdbDB::UpdateSingle;
    method_insert_ = &RocksdbDB::InsertSingle;
    method_delete_ = &RocksdbDB::DeleteSingle;
#ifdef USE_MERGEUPDATE
    if (props.GetProperty(PROP_MERGEUPDATE, PROP_MERGEUPDATE_DEFAULT) == "true") {
      method_update_ = &RocksdbDB::MergeSingle;
    }
#endif
  } else {
    throw utils::Exception("unknown format");
  }
  fieldcount_ = std::stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,
                                            CoreWorkload::FIELD_COUNT_DEFAULT));

  ref_cnt_++;
  if (db_) {
    return;
  }

  db_path = props.GetProperty(PROP_NAME, PROP_NAME_DEFAULT);
  if (db_path == "") {
    throw utils::Exception("RocksDB db path is missing");
  }

  rocksdb::Options opt;
  opt.create_if_missing = true;
  std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;
  GetOptions(props, &opt, &cf_descs);
#ifdef USE_MERGEUPDATE
  opt.merge_operator.reset(new YCSBUpdateMerge);
#endif

  rocksdb::Status s;
  if (props.GetProperty(PROP_DESTROY, PROP_DESTROY_DEFAULT) == "true") {
    s = rocksdb::DestroyDB(db_path, opt);
    if (!s.ok()) {
      throw utils::Exception(std::string("RocksDB DestroyDB: ") + s.ToString());
    }
  }
  if (cf_descs.empty()) {
    s = rocksdb::DB::Open(opt, db_path, &db_);
  } else {
    s = rocksdb::DB::Open(opt, db_path, cf_descs, &cf_handles_, &db_);
  }
  if (!s.ok()) {
    throw utils::Exception(std::string("RocksDB Open: ") + s.ToString());
  }
  if (--ref_cnt_) {
    return;
  }
  delete db_;
}

void RocksdbDB::Cleanup() { 
  const std::lock_guard<std::mutex> lock(mu_);
  if (--ref_cnt_) {
    return;
  }
  for (size_t i = 0; i < cf_handles_.size(); i++) {
    if (cf_handles_[i] != nullptr) {
      delete cf_handles_[i];
      cf_handles_[i] = nullptr;
    }
  }
  delete db_;
}

void RocksdbDB::GetOptions(const utils::Properties &props, rocksdb::Options *opt,
                           std::vector<rocksdb::ColumnFamilyDescriptor> *cf_descs) {
  std::string env_uri = props.GetProperty(PROP_ENV_URI, PROP_ENV_URI_DEFAULT);
  std::string fs_uri = props.GetProperty(PROP_FS_URI, PROP_FS_URI_DEFAULT);
  rocksdb::Env* env =  rocksdb::Env::Default();;
  if (!env_uri.empty() || !fs_uri.empty()) {
    rocksdb::Status s = rocksdb::Env::CreateFromUri(rocksdb::ConfigOptions(),
                                                    env_uri, fs_uri, &env, &env_guard);
    if (!s.ok()) {
      throw utils::Exception(std::string("RocksDB CreateFromUri: ") + s.ToString());
    }
    opt->env = env;
  }

  const std::string options_file = props.GetProperty(PROP_OPTIONS_FILE, PROP_OPTIONS_FILE_DEFAULT);
  if (options_file != "") {
    rocksdb::ConfigOptions config_options;
    config_options.ignore_unknown_options = false;
    config_options.input_strings_escaped = true;
    config_options.env = env;
    rocksdb::Status s = rocksdb::LoadOptionsFromFile(config_options, options_file, opt, cf_descs);
    if (!s.ok()) {
      throw utils::Exception(std::string("RocksDB LoadOptionsFromFile: ") + s.ToString());
    }
  } else {
    const std::string compression_type = props.GetProperty(PROP_COMPRESSION,
                                                           PROP_COMPRESSION_DEFAULT);
    if (compression_type == "no") {
      opt->compression = rocksdb::kNoCompression;
    } else if (compression_type == "snappy") {
      opt->compression = rocksdb::kSnappyCompression;
    } else if (compression_type == "zlib") {
      opt->compression = rocksdb::kZlibCompression;
    } else if (compression_type == "bzip2") {
      opt->compression = rocksdb::kBZip2Compression;
    } else if (compression_type == "lz4") {
      opt->compression = rocksdb::kLZ4Compression;
    } else if (compression_type == "lz4hc") {
      opt->compression = rocksdb::kLZ4HCCompression;
    } else if (compression_type == "xpress") {
      opt->compression = rocksdb::kXpressCompression;
    } else if (compression_type == "zstd") {
      opt->compression = rocksdb::kZSTD;
    } else {
      throw utils::Exception("Unknown compression type");
    }

    int val = std::stoi(props.GetProperty(PROP_MAX_BG_JOBS, PROP_MAX_BG_JOBS_DEFAULT));
    if (val != 0) {
      opt->max_background_jobs = val;
    }
    val = std::stoi(props.GetProperty(PROP_TARGET_FILE_SIZE_BASE, PROP_TARGET_FILE_SIZE_BASE_DEFAULT));
    if (val != 0) {
      opt->target_file_size_base = val;
    }
    val = std::stoi(props.GetProperty(PROP_TARGET_FILE_SIZE_MULT, PROP_TARGET_FILE_SIZE_MULT_DEFAULT));
    if (val != 0) {
      opt->target_file_size_multiplier = val;
    }
    val = std::stoi(props.GetProperty(PROP_MAX_BYTES_FOR_LEVEL_BASE, PROP_MAX_BYTES_FOR_LEVEL_BASE_DEFAULT));
    if (val != 0) {
      opt->max_bytes_for_level_base = val;
    }
    val = std::stoi(props.GetProperty(PROP_WRITE_BUFFER_SIZE, PROP_WRITE_BUFFER_SIZE_DEFAULT));
    if (val != 0) {
      opt->write_buffer_size = val;
    }
    val = std::stoi(props.GetProperty(PROP_MAX_WRITE_BUFFER, PROP_MAX_WRITE_BUFFER_DEFAULT));
    if (val != 0) {
      opt->max_write_buffer_number = val;
    }
    val = std::stoi(props.GetProperty(PROP_COMPACTION_PRI, PROP_COMPACTION_PRI_DEFAULT));
    if (val != -1) {
      opt->compaction_pri = static_cast<rocksdb::CompactionPri>(val);
    }
    val = std::stoi(props.GetProperty(PROP_MAX_OPEN_FILES, PROP_MAX_OPEN_FILES_DEFAULT));
    if (val != 0) {
      opt->max_open_files = val;
    }

    val = std::stoi(props.GetProperty(PROP_L0_COMPACTION_TRIGGER, PROP_L0_COMPACTION_TRIGGER_DEFAULT));
    if (val != 0) {
      opt->level0_file_num_compaction_trigger = val;
    }
    val = std::stoi(props.GetProperty(PROP_L0_SLOWDOWN_TRIGGER, PROP_L0_SLOWDOWN_TRIGGER_DEFAULT));
    if (val != 0) {
      opt->level0_slowdown_writes_trigger = val;
    }
    val = std::stoi(props.GetProperty(PROP_L0_STOP_TRIGGER, PROP_L0_STOP_TRIGGER_DEFAULT));
    if (val != 0) {
      opt->level0_stop_writes_trigger = val;
    }

    if (props.GetProperty(PROP_USE_DIRECT_WRITE, PROP_USE_DIRECT_WRITE_DEFAULT) == "true") {
      opt->use_direct_io_for_flush_and_compaction = true;
    }
    if (props.GetProperty(PROP_USE_DIRECT_READ, PROP_USE_DIRECT_READ_DEFAULT) == "true") {
      opt->use_direct_reads = true;
    }
    if (props.GetProperty(PROP_USE_MMAP_WRITE, PROP_USE_MMAP_WRITE_DEFAULT) == "true") {
      opt->allow_mmap_writes = true;
    }
    if (props.GetProperty(PROP_USE_MMAP_READ, PROP_USE_MMAP_READ_DEFAULT) == "true") {
      opt->allow_mmap_reads = true;
    }

    rocksdb::BlockBasedTableOptions table_options;
    size_t cache_size = std::stoul(props.GetProperty(PROP_CACHE_SIZE, PROP_CACHE_SIZE_DEFAULT));
    if (cache_size > 0) {
      block_cache = rocksdb::NewLRUCache(cache_size);
      table_options.block_cache = block_cache;
    }
#if ROCKSDB_MAJOR < 8
    size_t compressed_cache_size = std::stoul(props.GetProperty(PROP_COMPRESSED_CACHE_SIZE,
                                                                PROP_COMPRESSED_CACHE_SIZE_DEFAULT));
    if (compressed_cache_size > 0) {
      block_cache_compressed = rocksdb::NewLRUCache(compressed_cache_size);
      table_options.block_cache_compressed = block_cache_compressed;
    }
#endif
    int bloom_bits = std::stoul(props.GetProperty(PROP_BLOOM_BITS, PROP_BLOOM_BITS_DEFAULT));
    if (bloom_bits > 0) {
      table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bloom_bits));
    }
    opt->table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    if (props.GetProperty(PROP_INCREASE_PARALLELISM, PROP_INCREASE_PARALLELISM_DEFAULT) == "true") {
      opt->IncreaseParallelism();
    }
    if (props.GetProperty(PROP_OPTIMIZE_LEVELCOMP, PROP_OPTIMIZE_LEVELCOMP_DEFAULT) == "true") {
      opt->OptimizeLevelStyleCompaction();
    }
  }
}

void RocksdbDB::SerializeRow(const std::vector<Field> &values, std::string &data) {
  for (const Field &field : values) {
    uint32_t len = field.name.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.name.data(), field.name.size());
    len = field.value.size();
    data.append(reinterpret_cast<char *>(&len), sizeof(uint32_t));
    data.append(field.value.data(), field.value.size());
  }
}

void RocksdbDB::DeserializeRowFilter(std::vector<Field> &values, const char *p, const char *lim,
                                     const std::vector<std::string> &fields) {
  std::vector<std::string>::const_iterator filter_iter = fields.begin();
  while (p != lim && filter_iter != fields.end()) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    if (*filter_iter == field) {
      values.push_back({field, value});
      filter_iter++;
    }
  }
  assert(values.size() == fields.size());
}

void RocksdbDB::DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                                     const std::vector<std::string> &fields) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRowFilter(values, p, lim, fields);
}

void RocksdbDB::DeserializeRow(std::vector<Field> &values, const char *p, const char *lim) {
  while (p != lim) {
    assert(p < lim);
    uint32_t len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string field(p, static_cast<const size_t>(len));
    p += len;
    len = *reinterpret_cast<const uint32_t *>(p);
    p += sizeof(uint32_t);
    std::string value(p, static_cast<const size_t>(len));
    p += len;
    values.push_back({field, value});
  }
}

void RocksdbDB::DeserializeRow(std::vector<Field> &values, const std::string &data) {
  const char *p = data.data();
  const char *lim = p + data.size();
  DeserializeRow(values, p, lim);
}

void copyResultData(ResponseData* responseData, const std::string& resultData) {
    // Copy the result data into the responseData object
    responseData->resultData = resultData;
}

ResponseData* SubmitTaskAndWaitForResponse(
    const std::string& requesterId,
    const json& task) {
    const std::string& server_ip = "127.0.0.1";
    const std::string& server_port = "12345";
    try {
        // Create I/O context and resolver
        boost::asio::io_context io_context;
        tcp::resolver resolver(io_context);
        tcp::resolver::results_type endpoints = resolver.resolve(server_ip, server_port);

        // Create and connect the socket
        tcp::socket socket(io_context);
        boost::asio::connect(socket, endpoints);

        // Convert task to a string (JSON format)
        std::string taskString = task.dump() + "\n";

        // Send the task string to the server
        boost::asio::write(socket, boost::asio::buffer(taskString));

        //std::cout << "Task sent to server: " << taskString << std::endl;

        // Buffer to store the response from the server
        boost::asio::streambuf responseBuffer;
        boost::system::error_code error;

        // Wait and read the response from the server
        boost::asio::read_until(socket, responseBuffer, "\n", error);

        /* auto now = std::chrono::high_resolution_clock::now();
        auto now_ns = std::chrono::time_point_cast<std::chrono::nanoseconds>(now);
        auto duration = now_ns.time_since_epoch();  // Duration since epoch in nanoseconds
        std::cout << "Response received at time: " << std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count() << std::endl;
        */
        if (error && error != boost::asio::error::eof) {
            throw boost::system::system_error(error);  // Handle read error
        }

        // Convert the buffer to a string (response from server)
        std::istream responseStream(&responseBuffer);
        std::string responseDataString;
        std::getline(responseStream, responseDataString);

        //std::cout << "Response received: " << responseDataString << std::endl;

        // Parse the response as JSON
        json responseJson = json::parse(responseDataString);

        // Create a ResponseData object to hold the result
        ResponseData* responseData = new ResponseData();

        // Extract result data and status from the response JSON
        std::string resultData = responseJson["resultData"];
        std::string statusString = responseJson["status"];

        // Copy the result data into the responseData object
        copyResultData(responseData, resultData);

        // Convert status string to ResultStatus enum
        if (statusString == "OK") {
            responseData->status = ResultStatus::OK;
        } else {
            responseData->status = ResultStatus::ERROR;
        }

        // Return the response object
        return responseData;

    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
        return nullptr;
    }
}




// Example function modified for proper boost::interprocess usage
DB::Status RocksdbDB::ReadSingle(const std::string &table, const std::string &key,
                                 const std::vector<std::string> *fields,
                                 std::vector<Field> &result) {
    std::string requesterId = getCurrentThreadIdAsString();

    // Prepare the task as a JSON object
    json task;
    task["requesterId"] = requesterId;
    task["operation"] = "read";
    task["key"] = key;
    task["database"] = db_path;  // Ensure db_path is initialized properly

    ResponseData* response = SubmitTaskAndWaitForResponse(requesterId, task);

    // Check result status
    if (response->status == ResultStatus::ERROR) {
        return kError;  // Handle the error case
    }

    // Deserialize the result data (no need for strlen anymore)
    std::string data = response->resultData;

    // Process result based on whether fields are provided
    if (fields != nullptr) {
        DeserializeRowFilter(result, data, *fields);  // Deserialize with field filter
    } else {
        DeserializeRow(result, data);  // Deserialize entire row
        assert(result.size() == static_cast<size_t>(fieldcount_));
    }

    return kOK;
}

DB::Status RocksdbDB::ScanSingle(const std::string &table, const std::string &key, int len,
                                 const std::vector<std::string> *fields,
                                 std::vector<std::vector<Field>> &result) {
    // Convert thread ID to string for requesterId
    std::string requesterId = getCurrentThreadIdAsString();

    // Prepare the task as a JSON object
    json task;
    task["requesterId"] = requesterId;
    task["operation"] = "scan";
    task["database"] = db_path;  // Ensure db_path is initialized properly
    task["key"] = key;
    task["len"] = len;

    // Use the generalized function to submit the task and wait for the response
    ResponseData* response = SubmitTaskAndWaitForResponse(requesterId, task);

    // Parse the response
    if (response->status == ResultStatus::OK) {
        // Use the string from response->resultData directly (no need for manual sizing)
        std::istringstream stream(response->resultData);  // String stream for parsing lines

        std::string line;
        int count = 0;
        while (std::getline(stream, line) && count < len) {
            result.push_back(std::vector<Field>());
            std::vector<Field> &values = result.back();
            if (fields != nullptr) {
                DeserializeRowFilter(values, line, *fields);
            } else {
                DeserializeRow(values, line);
                assert(values.size() == static_cast<size_t>(fieldcount_));
            }
            count++;
        }
        return kOK;
    } else {
        return kNotFound;  // Handle error or not found case
    }
}

DB::Status RocksdbDB::UpdateSingle(const std::string &table, const std::string &key,
                                   std::vector<Field> &values) {
    // Convert thread ID to string for requesterId
    std::string requesterId = getCurrentThreadIdAsString();

    // --------- Step 1: Read the existing data using thread pool ---------
    // Prepare the read task as a JSON object
    json readTask;
    readTask["requesterId"] = requesterId;
    readTask["operation"] = "read";
    readTask["database"] = db_path;  // Ensure db_path is properly initialized
    readTask["key"] = key;

    // Submit the read task and wait for the response
    ResponseData* readResponse = SubmitTaskAndWaitForResponse(requesterId, readTask);

    // Check if the read was successful and process the response
    std::string existingData;
    if (readResponse->status == ResultStatus::OK) {
        existingData = readResponse->resultData;  // Directly get the result data string
    } else if (readResponse->status == ResultStatus::ERROR) {
        return kNotFound;  // Return not found if read operation fails
    } else {
        throw utils::Exception("RocksDB Read: Error occurred during read");
    }

    // --------- Step 2: Update the data ---------
    std::vector<Field> currentValues;
    DeserializeRow(currentValues, existingData);  // Deserialize the row into fields
    assert(currentValues.size() == static_cast<size_t>(fieldcount_));  // Ensure row size matches field count

    for (Field &newField : values) {
        bool found = false;
        for (Field &curField : currentValues) {
            if (curField.name == newField.name) {
                found = true;
                curField.value = newField.value;  // Update the field's value
                break;
            }
        }
        assert(found);  // Ensure the field to update was found
    }

    // --------- Step 3: Insert the updated data using thread pool ---------
    // Serialize the updated values
    std::string updatedData;
    SerializeRow(currentValues, updatedData);  // Serialize updated row into string

    // Prepare the insert task as a JSON object
    json insertTask;
    insertTask["requesterId"] = requesterId;
    insertTask["operation"] = "insert";
    insertTask["database"] = db_path;
    insertTask["key"] = key;
    insertTask["value"] = updatedData;

    // Submit the insert task and wait for the response
    ResponseData* insertResponse = SubmitTaskAndWaitForResponse(requesterId, insertTask);

    // Check the insert response status
    if (insertResponse->status == ResultStatus::OK) {
        return kOK;  // Successfully updated and inserted
    } else {
        throw utils::Exception("RocksDB Insert: Error occurred during insert");
    }
}


DB::Status RocksdbDB::MergeSingle(const std::string &table, const std::string &key,
                                  std::vector<Field> &values) {
    // Convert thread ID to string for requesterId
    std::string requesterId = getCurrentThreadIdAsString();

    // Serialize the fields into a string format for merging
    std::string data;
    SerializeRow(values, data);  // Convert the values into a serializable string

    // Prepare the merge task as a JSON object
    json task;
    task["requesterId"] = requesterId;
    task["operation"] = "merge";
    task["database"] = db_path;  // Ensure db_path is properly initialized
    task["key"] = key;
    task["value"] = data;  // Serialized row data for merging

    // Submit the merge task and wait for the response
    ResponseData* response = SubmitTaskAndWaitForResponse(requesterId, task);

    // Check the merge response status
    if (response->status == ResultStatus::OK) {
        return kOK;  // Return success if the merge was successful
    } else {
        return kError;  // Return an error in case the merge failed
    }
}

DB::Status RocksdbDB::InsertSingle(const std::string &table, const std::string &key,
                                   std::vector<Field> &values) {
    // Convert thread ID to string for requesterId
    std::string requesterId = getCurrentThreadIdAsString();

    // Serialize the fields into a string format for insertion
    std::string data;
    SerializeRow(values, data);  // Convert the fields into a serializable string

    // Prepare the insert task as a JSON object
    json task;
    task["requesterId"] = requesterId;
    task["operation"] = "insert";
    task["database"] = db_path;  // Ensure db_path is properly initialized
    task["key"] = key;
    task["value"] = data;  // Serialized row data for insertion

    // Submit the insert task and wait for the response
    ResponseData* response = SubmitTaskAndWaitForResponse(requesterId, task);

    // Check the insert response status
    if (response->status == ResultStatus::OK) {
        return kOK;  // Return success if the insertion was successful
    } else {
        throw utils::Exception("RocksDB Insert: Error occurred during insertion");
    }
}

DB::Status RocksdbDB::DeleteSingle(const std::string &table, const std::string &key) {
    // Convert thread ID to string for requesterId
    std::string requesterId = getCurrentThreadIdAsString();

    // Prepare the delete task as a JSON object
    json task;
    task["requesterId"] = requesterId;
    task["operation"] = "delete";
    task["database"] = db_path;  // Ensure db_path is initialized
    task["key"] = key;  // Specify the key to be deleted

    // Submit the delete task and wait for the response
    ResponseData* response = SubmitTaskAndWaitForResponse(requesterId, task);

    // Check the delete response status
    if (response->status == ResultStatus::OK) {
        return kOK;  // Return success if deletion was successful
    } else {
        throw utils::Exception("RocksDB Delete: Error occurred during deletion");
    }
}

DB *NewRocksdbDB() {
  return new RocksdbDB;
}

const bool registered = DBFactory::RegisterDB("rocksdb", NewRocksdbDB);

} // ycsbc
