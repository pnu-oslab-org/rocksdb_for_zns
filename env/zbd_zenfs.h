// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)

#include <errno.h>
#include <libzbd/zbd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <bitset>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <limits>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"

#define ZONE_CUSTOM_DEBUG

// #define ZONE_HOT_COLD_SEP

#define ZONE_RESET_TRIGGER (75)  // under 25% of empty zones, RESET started
#define ZONE_GC_WATERMARK (25)   // under 25% of empty zones, GC started

#define ZONE_MAX_NOTIFY_RETRY (10)

#define ZONE_GC_THREAD_TICK (std::chrono::milliseconds(1000))
#define ZONE_GC_ENABLE (1)  // is gc enable

#if defined(ZONE_CUSTOM_DEBUG)
#pragma message("ZONE CUSTOM DEBUG mode enabled")
#else
#pragma message("ZONE CUSTOM DEBUG mode disabled")
#endif

#if ZONE_GC_ENABLE == 1
#pragma message("ZONE GC mode enabled")
#else
#pragma message("ZONE GC mode disabled")
#endif

#if defined(ZONE_HOT_COLD_SEP)
#pragma message("ZONE_HOT_COLD_SEP mode enabled")
#else
#pragma message("ZONE_HOT_COLD_SEP mode disabled")
#endif

#if defined(ZONE_MIX)
#pragma message("ZONE_MIX mode enabled")
#else
#pragma message("ZONE_MIX mode disabled")
#endif

namespace ROCKSDB_NAMESPACE {

enum class ZoneGcState { NOT_GC_TARGET, DO_RESET, NORMAL_EXIT };

enum ZoneInvalidLevel {
  ZONE_INVALID_LOW = 0,
  ZONE_INVALID_MIDDLE,
  ZONE_INVALID_HIGH,
  ZONE_INVALID_MAX,
  NR_ZONE_INVALID_LEVEL,
};

class ZonedBlockDevice;
class ZoneFile;
class ZoneExtent;

#define ZONE_EXTENT_FIND_FAIL (std::numeric_limits<uint64_t>::max())

class ZoneMapEntry {
 public:
  ZoneFile *file_;

  uint64_t extent_start_;
  uint32_t extent_length_;

  std::string filename_;
  std::atomic<bool> is_invalid_;

  Env::WriteLifeTimeHint lifetime_;

  explicit ZoneMapEntry(ZoneFile *file, ZoneExtent *extent);
};

class Zone {
  ZonedBlockDevice *zbd_;

 public:
  explicit Zone(ZonedBlockDevice *zbd, struct zbd_zone *z);

  uint64_t start_;
  uint64_t capacity_; /* remaining capacity */
  uint64_t max_capacity_;
  uint64_t wp_;
  bool open_for_write_;
  Env::WriteLifeTimeHint lifetime_;
  double total_lifetime_;
  std::atomic<long> used_capacity_;
  std::atomic<uint64_t> reset_counter_;
  std::vector<ZoneMapEntry *> file_map_;
  bool has_meta_;

  IOStatus Reset();
  IOStatus Finish();
  IOStatus Close();

  IOStatus Append(char *data, uint32_t size);
  bool IsUsed();
  bool IsFull();
  bool IsEmpty();
  uint64_t GetZoneNr();
  uint64_t GetCapacityLeft();
  double GetInvalidPercentage();
  double GetAvgZoneLevel();

  void SetZoneFile(ZoneFile *file, ZoneExtent *extent);
  void RemoveZoneFile(ZoneFile *file, ZoneExtent *extent);
  void PrintZoneFiles(FILE *fp);

  void CloseWR(); /* Done writing */
};

class VictimZoneCompare {
 public:
  bool operator()(Zone *z1, Zone *z2) {
    // true means z2 goes to front
    if (z1->GetAvgZoneLevel() == z2->GetAvgZoneLevel()) {
      return z1->GetInvalidPercentage() < z2->GetInvalidPercentage();
    }
    return z1->GetAvgZoneLevel() < z2->GetAvgZoneLevel();
  }
};

class ZonedBlockDevice {
 private:
  std::string filename_;
  uint32_t block_sz_;
  uint32_t zone_sz_;
  uint32_t nr_zones_;
  std::vector<Zone *> io_zones;
  std::recursive_mutex io_zones_mtx;
  std::vector<Zone *> meta_zones;
  int read_f_;
  int read_direct_f_;
  int write_f_;
  time_t start_time_;
  std::shared_ptr<Logger> logger_;
  uint32_t finish_threshold_ = 0;

  std::atomic<long> active_io_zones_;
  std::atomic<long> open_io_zones_;
  std::condition_variable zone_resources_;
  std::mutex zone_resources_mtx_; /* Protects active/open io zones */

  std::atomic<bool> gc_force_;
  std::atomic<bool> gc_thread_stop_;
  std::atomic<long> active_gc_;
  std::condition_variable gc_thread_cond_;
  std::condition_variable gc_force_cond_;
  std::mutex gc_force_mtx_;
  std::mutex gc_thread_mtx_;
  std::thread *gc_thread_;
  std::priority_queue<Zone *, std::vector<Zone *>, VictimZoneCompare>
      victim_queue_[NR_ZONE_INVALID_LEVEL];
  uint64_t gc_rate_limiter_;
  uint64_t reset_rate_limiter_;

  FILE *zone_log_file_;
  char *gc_buffer_;

  unsigned int max_nr_active_io_zones_;
  unsigned int max_nr_open_io_zones_;

  Zone *AllocateZoneImpl(Env::WriteLifeTimeHint lifetime, ZoneFile *file);

 public:
  explicit ZonedBlockDevice(std::string bdevname,
                            std::shared_ptr<Logger> logger);
  virtual ~ZonedBlockDevice();

  std::mutex *files_mtx_;

  IOStatus Open(bool readonly = false);

  Zone *GetIOZone(uint64_t offset);

  Zone *AllocateZone(Env::WriteLifeTimeHint lifetime, ZoneFile *file);
  Zone *AllocateZone(Env::WriteLifeTimeHint lifetime, ZoneFile *zone_file,
                     Zone *before_zone);
  Zone *AllocateMetaZone();

  uint64_t GetFreeSpace();
  std::string GetFilename();
  uint32_t GetBlockSize();
  uint32_t GetEmptyZones();

  void ResetUnusedIOZones();
  void LogZoneStats();
  void LogZoneUsage();

  int GetReadFD() { return read_f_; }
  int GetReadDirectFD() { return read_direct_f_; }
  int GetWriteFD() { return write_f_; }

  uint32_t GetZoneSize() { return zone_sz_; }
  uint32_t GetNrZones() { return nr_zones_; }
  FILE *GetZoneLogFile() { return zone_log_file_; }
  std::vector<Zone *> GetMetaZones() { return meta_zones; }

  void SetFinishTreshold(uint32_t threshold) { finish_threshold_ = threshold; }

  void NotifyIOZoneFull();
  void NotifyIOZoneClosed();
  void NotifyZoneAllocateAvail();
  void NotifyGarbageCollectionRun();

  long GetOpenIOZone() { return open_io_zones_; }
  unsigned int GetMaxNrOpenIOZone() { return max_nr_open_io_zones_; }
  long GetActiveIOZone() { return active_io_zones_; }
  unsigned int GetMaxNrActiveIOZone() { return max_nr_active_io_zones_; }

 private:
  void GarbageCollectionThread(void);
  void GarbageCollection(const bool &is_trigger,
                         const Env::WriteLifeTimeHint lifetime,
                         const bool &is_force,
                         const uint32_t &current_empty_zones);
  Slice ReadDataFromExtent(const ZoneMapEntry *item, char *scratch,
                           ZoneExtent **target_extent);
  IOStatus CopyDataToFile(const ZoneMapEntry *item, Slice &source,
                          char *scratch);
  void WaitUntilZoneOpenAvail();
  void WaitUntilZoneAllocateAvail();
  template <class Duration>
  bool GarbageCollectionSchedule(const Duration &duration);
  bool ZoneVictimEnableCheck(Zone *z, const bool &is_force);
  void ZoneSelectVictim(const int &invalid_level, const bool &is_force);
  ZoneGcState ValidDataCopy(Env::WriteLifeTimeHint lifetime, Zone *z);
  uint64_t ZoneResetAndFinish(Zone *z, bool reset_condition,
                              bool finish_condition, Zone **callback_victim);

  int AllocateEmptyZone(unsigned int best_diff, Zone *finish_victim,
                        Zone **allocated_zone, Env::WriteLifeTimeHint lifetime);
  int GetAlreadyOpenZone(Zone **allocated_zone, ZoneFile *file,
                         Env::WriteLifeTimeHint lifetime);
  std::string GetZoneFileExt(const std::string filename);
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)
