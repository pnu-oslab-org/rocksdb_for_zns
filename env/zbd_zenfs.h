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
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <limits>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"

// #define ZONE_CUSTOM_DEBUG

#define ZONE_MIX
// #define ZONE_HOT_COLD_MIX

#define ZONE_RESET_TRIGGER (75)  // Empty Zone이 30% 이하일 때

#define ZONE_FILE_MIN_MIX (1)
#define ZONE_GC_WATERMARK \
  (70)  // if you don't have 70% of empty zones, GC started
#define ZONE_GC_MAX_EXEC (5)  // execute gc count per allocate a zone
#define ZONE_GC_ENABLE (0)    // is gc enable

#if ZONE_GC_ENABLE == 1
#pragma message("ZONE GC mode enabled")
#else
#pragma message("ZONE GC mode disabled")
#endif

#if defined(ZONE_HOT_COLD_MIX)
#pragma message("ZONE_HOT_COLD_MIX mode enabled")
#else
#pragma message("ZONE_HOT_COLD_MIX mode disabled")
#endif

#if defined(ZONE_MIX)
#pragma message("ZONE_MIX mode enabled")
#else
#pragma message("ZONE_MIX mode disabled")
#endif

namespace ROCKSDB_NAMESPACE {

enum class ZoneGcState { NOT_GC_TARGET, DO_RESET, NORMAL_EXIT };

class ZonedBlockDevice;
class ZoneFile;
class ZoneExtent;

#define ZONE_EXTENT_FIND_FAIL (std::numeric_limits<uint64_t>::max())

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
  std::atomic<long> used_capacity_;
  std::unordered_map<ZoneFile *, uint64_t> file_map_;

  IOStatus Reset();
  IOStatus Finish();
  IOStatus Close();

  IOStatus Append(char *data, uint32_t size);
  bool IsUsed();
  bool IsFull();
  bool IsEmpty();
  uint64_t GetZoneNr();
  uint64_t GetCapacityLeft();

  void SetZoneFile(ZoneFile *file, uint64_t extent_start);
  uint64_t GetZoneExtent(ZoneFile *file);
  void RemoveZoneFile(ZoneFile *file);
  void PrintZoneFiles(FILE *fp);

  void CloseWR(); /* Done writing */
};

class ZonedBlockDevice {
 private:
  std::string filename_;
  uint32_t block_sz_;
  uint32_t zone_sz_;
  uint32_t nr_zones_;
  std::vector<Zone *> io_zones;
  std::mutex io_zones_mtx;
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

  FILE *zone_log_file_;

  char *gc_buffer_;

  unsigned int max_nr_active_io_zones_;
  unsigned int max_nr_open_io_zones_;

  Zone *AllocateZoneRaw(Env::WriteLifeTimeHint lifetime, ZoneFile *file,
                        bool is_gc);

 public:
  explicit ZonedBlockDevice(std::string bdevname,
                            std::shared_ptr<Logger> logger);
  virtual ~ZonedBlockDevice();

  std::mutex *files_mtx_;

  IOStatus Open(bool readonly = false);

  Zone *GetIOZone(uint64_t offset);

  Zone *AllocateZone(Env::WriteLifeTimeHint lifetime, ZoneFile *file,
                     bool is_gc);
  Zone *AllocateZone(Env::WriteLifeTimeHint lifetime, ZoneFile *zone_file,
                     Zone *before_zone, bool is_gc);
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

 private:
  void WaitUntilZoneOpenAvail();
  Zone *ZoneSelectVictim();
  ZoneGcState ZoneGc(Env::WriteLifeTimeHint lifetime, Zone *z);
  ZoneGcState ZoneResetAndFinish(Zone *z, bool reset_condition,
                                 bool finish_condition, Zone **callback_victim);
  int AllocateEmptyZone(unsigned int best_diff, Zone *finish_victim,
                        Zone **allocated_zone, Env::WriteLifeTimeHint lifetime);
  int GetAlreadyOpenZone(Zone **allocated_zone, ZoneFile *file,
                         Env::WriteLifeTimeHint lifetime);
  std::string GetZoneFileExt(const std::string filename);
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX) && defined(LIBZBD)
