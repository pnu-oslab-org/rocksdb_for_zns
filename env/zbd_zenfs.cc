// Copyright(c) Facebook, Inc.and its affiliates.All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN) && defined(LIBZBD)

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "io_zenfs.h"
#include "rocksdb/env.h"
#include "zbd_zenfs.h"

#define KB (1024)
#define MB (1024 * KB)

/* Number of reserved zones for metadata
 * Two non-offline meta zones are needed to be able
 * to roll the metadata log safely. One extra
 * is allocated to cover for one zone going offline.
 */
#define ZENFS_META_ZONES (3)
/* Minimum of number of zones that makes sense */

#define ZENFS_MIN_ZONES (32)
#define ZONE_FILE_MIN_MIX (2)

namespace ROCKSDB_NAMESPACE {

ZoneMapEntry::ZoneMapEntry(ZoneFile *file, ZoneExtent *extent) : file_(file) {
  extent_start_ = extent->start_;
  extent_length_ = extent->length_;
  lifetime_ = file->GetWriteLifeTimeHint();
  filename_ = file->GetFilename();
  is_invalid_ = false;
}

Zone::Zone(ZonedBlockDevice *zbd, struct zbd_zone *z)
    : zbd_(zbd),
      start_(zbd_zone_start(z)),
      max_capacity_(zbd_zone_capacity(z)),
      wp_(zbd_zone_wp(z)),
      open_for_write_(false),
      lifetime_(Env::WLTH_NOT_SET),
      used_capacity_(0),
      reset_counter_(0),
      has_meta_(false) {
  capacity_ = 0;
  if (!(zbd_zone_full(z) || zbd_zone_offline(z) || zbd_zone_rdonly(z)))
    capacity_ = zbd_zone_capacity(z) - (zbd_zone_wp(z) - zbd_zone_start(z));
}

bool Zone::IsUsed() { return (used_capacity_ > 0) || open_for_write_; }
uint64_t Zone::GetCapacityLeft() { return capacity_; }
double Zone::GetInvalidPercentage() {
  uint64_t write_capacity = 0;
  uint64_t invalid_capacity = 0;
  for (const auto item : file_map_) {
    write_capacity += item->extent_length_;
    if (item->is_invalid_) {
      invalid_capacity += item->extent_length_;
    }
  }
  return ((double)invalid_capacity / write_capacity) * 100.0f;
}

double Zone::GetAvgZoneLevel() {
  uint64_t total_files_level = 0;
  uint64_t number_of_files = 0;
  for (const auto item : file_map_) {
    if (!item->is_invalid_) {
      total_files_level += (uint64_t)(item->lifetime_);
      number_of_files++;
    }
  }
  return ((double)total_files_level / number_of_files);
}

bool Zone::IsFull() { return (capacity_ == 0); }
bool Zone::IsEmpty() { return (wp_ == start_); }
uint64_t Zone::GetZoneNr() { return start_ / zbd_->GetZoneSize(); }

void Zone::SetZoneFile(ZoneFile *file, ZoneExtent *extent) {
  assert(nullptr != file);

  bool is_sst = file->GetFilename().find("sst") == std::string::npos;
  bool is_log = file->GetFilename().find("log") == std::string::npos;
  if (!is_sst && !is_log) {
    has_meta_ = true;
  }
  file_map_.push_back(new ZoneMapEntry(file, extent));
}

void Zone::RemoveZoneFile(ZoneFile *file, ZoneExtent *extent) {
  for (const auto item : file_map_) {
    if (item->file_ == file && item->extent_start_ == extent->start_) {
      item->is_invalid_ = true;
      break;
    }
  }
}

void Zone::PrintZoneFiles(FILE *fp) {
  assert(nullptr != fp);

  for (const auto item : file_map_) {
    if (item->is_invalid_) {
      continue;
    }
    ZoneExtent *extent = item->file_->GetExtent(item->extent_start_);
    if (extent) {
      fprintf(fp, "(zone: %lu) %s %u\n", GetZoneNr(),
              item->file_->GetFilename().c_str(), extent->length_);
      fflush(fp);
    }
  }
  fflush(fp);
}

void Zone::CloseWR() {
  assert(open_for_write_);
  open_for_write_ = false;

  if (Close().ok()) {
    zbd_->NotifyIOZoneClosed();
  }

  if (capacity_ == 0) zbd_->NotifyIOZoneFull();
}

IOStatus Zone::Reset() {
  size_t zone_sz = zbd_->GetZoneSize();
  int fd = zbd_->GetWriteFD();
  unsigned int report = 1;
  struct zbd_zone z;
  int ret;

  assert(!IsUsed());

#ifdef ZONE_CUSTOM_DEBUG
  if (zbd_->GetZoneLogFile()) {
    fprintf(zbd_->GetZoneLogFile(), "%-10ld%-8s%-8d%-8lu%-8.2lf%-8lu\n",
            (long int)((double)clock() / CLOCKS_PER_SEC * 1000), "RESET", 0,
            GetZoneNr(), (double)(total_lifetime_ / file_map_.size()),
            file_map_.size());
    fflush(zbd_->GetZoneLogFile());
  }
#endif

  ret = zbd_reset_zones(fd, start_, zone_sz);
  if (ret) return IOStatus::IOError("Zone reset failed\n");

  ret = zbd_report_zones(fd, start_, zone_sz, ZBD_RO_ALL, &z, &report);

  if (ret || (report != 1)) return IOStatus::IOError("Zone report failed\n");

  if (zbd_zone_offline(&z))
    capacity_ = 0;
  else
    max_capacity_ = capacity_ = zbd_zone_capacity(&z);

  wp_ = start_;
  lifetime_ = Env::WLTH_NOT_SET;

  for (const auto item : file_map_) {
    free(item);
  }
  file_map_.clear();
  has_meta_ = false;

  total_lifetime_ = 0;

  reset_counter_++;

  return IOStatus::OK();
}

IOStatus Zone::Finish() {
  size_t zone_sz = zbd_->GetZoneSize();
  int fd = zbd_->GetWriteFD();
  int ret;

  assert(!open_for_write_);

#ifdef ZONE_CUSTOM_DEBUG
  if (zbd_->GetZoneLogFile()) {
    fprintf(zbd_->GetZoneLogFile(), "%-10ld%-8s%-8d%-8lu\n",
            (long int)((double)clock() / CLOCKS_PER_SEC * 1000), "FINISH", 0,
            GetZoneNr());
    fflush(zbd_->GetZoneLogFile());
  }
#endif
  ret = zbd_finish_zones(fd, start_, zone_sz);
  if (ret) return IOStatus::IOError("Zone finish failed\n");

  capacity_ = 0;
  wp_ = start_ + zone_sz;

  return IOStatus::OK();
}

IOStatus Zone::Close() {
  size_t zone_sz = zbd_->GetZoneSize();
  int fd = zbd_->GetWriteFD();
  int ret;

  assert(!open_for_write_);

  if (!(IsEmpty() || IsFull())) {
    ret = zbd_close_zones(fd, start_, zone_sz);
    if (ret) return IOStatus::IOError("Zone close failed\n");
  }

#ifdef ZONE_CUSTOM_DEBUG
  if (zbd_->GetZoneLogFile()) {
    fprintf(zbd_->GetZoneLogFile(), "%-10ld%-8s%-8d%-8lu%-8lf\n",
            (long int)((double)clock() / CLOCKS_PER_SEC * 1000), "MIX", 0,
            GetZoneNr(), GetAvgZoneLevel());
    fflush(zbd_->GetZoneLogFile());
  }
#endif

  return IOStatus::OK();
}

IOStatus Zone::Append(char *data, uint32_t size) {
  char *ptr = data;
  uint32_t left = size;
  int fd = zbd_->GetWriteFD();
  int ret;

  if (capacity_ < size) {
    return IOStatus::NoSpace("Not enough capacity for append");
  }

  assert((size % zbd_->GetBlockSize()) == 0);

  // We must add some feature to check the valid status of each Zone
  while (left) {
    ret = pwrite(fd, ptr, left, wp_);
    if (ret < 0) {
      fprintf(stderr,
              "- Zone: %lu\n\tWP: %lu + %u(%lu)\n\t# of open zones: "
              "%ld(%u)\n\t# of active zones: %ld(%u)\n",
              GetZoneNr(), wp_, left, start_ + zbd_->GetZoneSize(),
              zbd_->GetOpenIOZone(), zbd_->GetMaxNrOpenIOZone(),
              zbd_->GetActiveIOZone(), zbd_->GetMaxNrActiveIOZone());
      perror("pwrite failed");
      return IOStatus::IOError("Write failed");
    }

    ptr += ret;
    wp_ += ret;
    capacity_ -= ret;
    left -= ret;
  }

  return IOStatus::OK();
}

Zone *ZonedBlockDevice::GetIOZone(uint64_t offset) {
  for (const auto z : io_zones)
    if (z->start_ <= offset && offset < (z->start_ + zone_sz_)) return z;
  return nullptr;
}

ZonedBlockDevice::ZonedBlockDevice(std::string bdevname,
                                   std::shared_ptr<Logger> logger)
    : filename_("/dev/" + bdevname), logger_(logger) {
  Info(logger_, "New Zoned Block Device: %s", filename_.c_str());
  zone_log_file_ = nullptr;
  files_mtx_ = nullptr;
  gc_thread_ = nullptr;
  gc_thread_stop_ = false;
  gc_force_ = false;
  gc_rate_limiter_ = 0;
  reset_rate_limiter_ = 0;
};

IOStatus ZonedBlockDevice::Open(bool readonly) {
  struct zbd_zone *zone_rep;
  unsigned int reported_zones;
  size_t addr_space_sz;
  zbd_info info;
  Status s;
  uint64_t i = 0;
  uint64_t m = 0;
  int ret;
  std::stringstream sstr;

  read_f_ = zbd_open(filename_.c_str(), O_RDONLY, &info);
  if (read_f_ < 0) {
    return IOStatus::InvalidArgument("Failed to open zoned block device");
  }

  read_direct_f_ = zbd_open(filename_.c_str(), O_RDONLY, &info);
  if (read_f_ < 0) {
    return IOStatus::InvalidArgument("Failed to open zoned block device");
  }

  if (readonly) {
    write_f_ = -1;
  } else {
    write_f_ = zbd_open(filename_.c_str(), O_WRONLY | O_DIRECT, &info);
    if (write_f_ < 0) {
      return IOStatus::InvalidArgument("Failed to open zoned block device");
    }
  }

  if (info.model != ZBD_DM_HOST_MANAGED) {
    return IOStatus::NotSupported("Not a host managed block device");
  }

  if (info.nr_zones < ZENFS_MIN_ZONES) {
    return IOStatus::NotSupported(
        "To few zones on zoned block device (32 required)");
  }

  block_sz_ = info.pblock_size;
  zone_sz_ = info.zone_size;
  nr_zones_ = info.nr_zones;

#if defined(ZONE_HOT_COLD_SEP)
  sstr << "zone_e" << ZONE_RESET_TRIGGER << "_sep.log";
#else
  sstr << "zone_e" << ZONE_RESET_TRIGGER << "_no_sep.log";
#endif

#ifdef ZONE_CUSTOM_DEBUG
  zone_log_file_ = fopen(sstr.str().c_str(), "w");
  assert(NULL != zone_log_file_);

  fprintf(zone_log_file_, "%-10s%-8s%-8s%-8s%-45s%-10s%-10s%-10s\n", "TIME(ms)",
          "CMD", "ZONE(-)", "ZONE(+)", "FILE NAME", "WRITE", "FILE SIZE",
          "LEVEL");
  fflush(zone_log_file_);
#endif

  /* We need one open zone for meta data writes, the rest can be used for
   * files
   */
  if (info.max_nr_active_zones == 0)
    max_nr_active_io_zones_ = info.nr_zones;
  else
    max_nr_active_io_zones_ = info.max_nr_active_zones - 1;

  if (info.max_nr_open_zones == 0)
    max_nr_open_io_zones_ = info.nr_zones;
  else
    max_nr_open_io_zones_ = info.max_nr_open_zones - 1;

  Info(logger_, "Zone block device nr zones: %u max active: %u max open: %u \n",
       info.nr_zones, info.max_nr_active_zones, info.max_nr_open_zones);

  addr_space_sz = (uint64_t)nr_zones_ * zone_sz_;

  ret = zbd_list_zones(read_f_, 0, addr_space_sz, ZBD_RO_ALL, &zone_rep,
                       &reported_zones);

  if (ret || reported_zones != nr_zones_) {
    Error(logger_, "Failed to list zones, err: %d", ret);
    return IOStatus::IOError("Failed to list zones");
  }

  while (m < ZENFS_META_ZONES && i < reported_zones) {
    struct zbd_zone *z = &zone_rep[i++];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) == ZBD_ZONE_TYPE_SWR) {
      if (!zbd_zone_offline(z)) {
        meta_zones.push_back(new Zone(this, z));
      }
      m++;
    }
  }

  active_io_zones_ = 0;
  open_io_zones_ = 0;

  for (; i < reported_zones; i++) {
    struct zbd_zone *z = &zone_rep[i];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) == ZBD_ZONE_TYPE_SWR) {
      if (!zbd_zone_offline(z)) {
        Zone *newZone = new Zone(this, z);
        io_zones.push_back(newZone);
        if (zbd_zone_imp_open(z) || zbd_zone_exp_open(z) ||
            zbd_zone_closed(z)) {
          active_io_zones_++;
          if (zbd_zone_imp_open(z) || zbd_zone_exp_open(z)) {
            if (!readonly) {
              newZone->Close();
            }
          }
        }
      }
    }
  }

  free(zone_rep);
  start_time_ = time(NULL);

  gc_thread_ =
      new std::thread(&ZonedBlockDevice::GarbageCollectionThread, this);
  assert(nullptr != gc_thread_);

  return IOStatus::OK();
}

void ZonedBlockDevice::GarbageCollectionThread() {
  uint32_t previous_empty_zones = GetEmptyZones();
  while (GarbageCollectionSchedule(ZONE_GC_THREAD_TICK) || gc_force_) {
    uint32_t current_empty_zones = GetEmptyZones();
    bool is_trigger =
        (current_empty_zones * 100 <= GetNrZones() * ZONE_RESET_TRIGGER);
    if (gc_thread_stop_) {
      break;
    }

    is_trigger &= (current_empty_zones < previous_empty_zones);
    previous_empty_zones = current_empty_zones;
    if (is_trigger) {
      io_zones_mtx.lock();
#ifdef ZONE_CUSTOM_DEBUG
      if (gc_force_) {
        fprintf(zone_log_file_, "%-10ld%-8s%-8u%-8u\n",
                (long int)((double)clock() / CLOCKS_PER_SEC * 1000), "FORCED",
                GetNrZones(), current_empty_zones);
        fflush(zone_log_file_);
      }
#endif
      active_gc_++;
      GarbageCollection(is_trigger, Env::WLTH_NONE, gc_force_,
                        current_empty_zones);
      io_zones_mtx.unlock();
      gc_force_ = false;
      NotifyZoneAllocateAvail();
    }
  }
}

void ZonedBlockDevice::GarbageCollection(const bool &is_trigger,
                                         const Env::WriteLifeTimeHint lifetime,
                                         const bool &is_force,
                                         const uint32_t &current_empty_zones) {
  uint64_t processed_reset = 0;
  uint32_t empty_zones_for_gc = 0;
  Zone *finish_victim = nullptr;

  if (files_mtx_ == nullptr) {  // This for zenfs
    return;
  }

#ifdef ZONE_CUSTOM_DEBUG
  fprintf(zone_log_file_, "%-10ld%-8s%-8u%-8u\n",
          (long int)((double)clock() / CLOCKS_PER_SEC * 1000), "REMAIN",
          GetNrZones(), current_empty_zones);
  fflush(zone_log_file_);

  if (is_trigger) {
    fprintf(zone_log_file_, "%-10ld%-8s\n",
            (long int)((double)clock() / CLOCKS_PER_SEC * 1000), "TRIGGER");
    fflush(zone_log_file_);
  }
#endif

  double empty_ratio = (double)current_empty_zones / GetNrZones() * 100.0f;

  if (empty_ratio > 50.0f) {
    reset_rate_limiter_ = GetNrZones() / 100;
  } else if (empty_ratio > 40.0f) {
    reset_rate_limiter_ = GetNrZones() / 75;
  } else if (empty_ratio > 30.0f) {
    reset_rate_limiter_ = GetNrZones() / 50;
  } else if (empty_ratio > 20.0f) {
    reset_rate_limiter_ = GetNrZones() / 25;
  } else {
    reset_rate_limiter_ = GetNrZones();
  }

  for (const auto z : io_zones) {
    if (processed_reset > reset_rate_limiter_) {
      break;
    }
    bool reset_cond = (!z->IsUsed() && is_trigger);
    bool finish_cond =
        (z->capacity_ < (z->max_capacity_ * finish_threshold_ / 100));

    processed_reset +=
        ZoneResetAndFinish(z, reset_cond, finish_cond, &finish_victim);
  }

  empty_zones_for_gc = GetEmptyZones();

  const bool is_gc =
      ZONE_GC_ENABLE && (files_mtx_ != nullptr) &&
      (empty_zones_for_gc * 100 <= GetNrZones() * ZONE_GC_WATERMARK);

  if (!is_gc) {
    return;
  }

  empty_ratio = (double)empty_zones_for_gc / GetNrZones() * 100.0f;

  if (!files_mtx_->try_lock()) {
    return;
  }
  int invalid_level = ZONE_INVALID_MAX;
  while (invalid_level >= ZONE_INVALID_LOW) {
    if (victim_queue_[invalid_level].empty()) {
      ZoneSelectVictim(invalid_level, is_force);
    }
    while (!victim_queue_[invalid_level].empty()) {
      Zone *victim = victim_queue_[invalid_level].top();
      assert(victim != nullptr);
      victim_queue_[invalid_level].pop();
      if (!ZoneVictimEnableCheck(victim, is_force)) {
        continue;
      }
#ifdef ZONE_CUSTOM_DEBUG
      if (zone_log_file_) {
        fprintf(zone_log_file_, "%-10ld%-8s%-8lu%-8.2lf%-8.2lf%-8u%-8.2lf\n",
                (long int)((double)clock() / CLOCKS_PER_SEC * 1000), "GC_S",
                victim->GetZoneNr(), victim->GetAvgZoneLevel(),
                victim->GetInvalidPercentage(), invalid_level,
                (GetNrZones() * (double)(invalid_level + 1) / 20));
        fflush(zone_log_file_);
      }
#endif
      (void)ValidDataCopy(lifetime, victim);
#ifdef ZONE_CUSTOM_DEBUG
      if (zone_log_file_) {
        fprintf(zone_log_file_, "%-10ld%-8s%-8lu\n",
                (long int)((double)clock() / CLOCKS_PER_SEC * 1000), "GC_E",
                victim->GetZoneNr());
        fflush(zone_log_file_);
      }
#endif
    }  // victim zone processing loop
    empty_zones_for_gc = GetEmptyZones();
    empty_ratio = (double)empty_zones_for_gc / GetNrZones() * 100.0f;
    if (empty_ratio > 5.0f) {
      break;
    }
    invalid_level--;
  }  // invalid level loop
  files_mtx_->unlock();
}

void ZonedBlockDevice::NotifyIOZoneFull() {
  const std::lock_guard<std::mutex> lock(zone_resources_mtx_);
  active_io_zones_--;
  zone_resources_.notify_one();
}

void ZonedBlockDevice::NotifyIOZoneClosed() {
  const std::lock_guard<std::mutex> lock(zone_resources_mtx_);
  open_io_zones_--;
  zone_resources_.notify_one();
}

void ZonedBlockDevice::NotifyZoneAllocateAvail() {
  const std::lock_guard<std::mutex> lock(gc_thread_mtx_);
  active_gc_--;
  gc_thread_cond_.notify_all();
}

void ZonedBlockDevice::NotifyGarbageCollectionRun() {
  const std::lock_guard<std::mutex> lock(gc_force_mtx_);
  gc_force_ = true;
#ifdef ZONE_CUSTOM_DEBUG
  if (gc_force_) {
    fprintf(zone_log_file_, "%-10ld%-8s%-8u%-8u\n",
            (long int)((double)clock() / CLOCKS_PER_SEC * 1000), "NOTIFY",
            GetNrZones(), GetEmptyZones());
    fflush(zone_log_file_);
  }
#endif
  gc_force_cond_.notify_all();
}

uint64_t ZonedBlockDevice::GetFreeSpace() {
  uint64_t free = 0;
  for (const auto z : io_zones) {
    free += z->capacity_;
  }

  return free;
}

uint32_t ZonedBlockDevice::GetEmptyZones() {
  uint32_t free = 0;
  for (const auto z : io_zones) {
    free += z->IsEmpty() == true ? 1 : 0;
  }
  return free;
}

void ZonedBlockDevice::LogZoneStats() {
  uint64_t used_capacity = 0;
  uint64_t reclaimable_capacity = 0;
  uint64_t reclaimables_max_capacity = 0;
  uint64_t active = 0;
  io_zones_mtx.lock();

  for (const auto z : io_zones) {
    used_capacity += z->used_capacity_;

    if (z->used_capacity_) {
      reclaimable_capacity += z->max_capacity_ - z->used_capacity_;
      reclaimables_max_capacity += z->max_capacity_;
    }

    if (!(z->IsFull() || z->IsEmpty())) active++;
  }

  if (reclaimables_max_capacity == 0) reclaimables_max_capacity = 1;

  Info(logger_,
       "[Zonestats:time(s),used_cap(MB),reclaimable_cap(MB), "
       "avg_reclaimable(%%), active(#), active_zones(#), open_zones(#)] %ld "
       "%lu %lu %lu %lu %ld %ld\n",
       time(NULL) - start_time_, used_capacity / MB, reclaimable_capacity / MB,
       100 * reclaimable_capacity / reclaimables_max_capacity, active,
       active_io_zones_.load(), open_io_zones_.load());

  io_zones_mtx.unlock();
}

void ZonedBlockDevice::LogZoneUsage() {
  for (const auto z : io_zones) {
    int64_t used = z->used_capacity_;

    if (used > 0) {
      Debug(logger_, "Zone 0x%lX used capacity: %ld bytes (%ld MB)\n",
            z->start_, used, used / MB);
    }
  }
}

ZonedBlockDevice::~ZonedBlockDevice() {
  gc_thread_stop_ = true;
  if (gc_thread_) {
    gc_thread_->join();
    free(gc_thread_);
  }
  for (const auto z : meta_zones) {
    delete z;
  }

  for (const auto z : io_zones) {
    delete z;
  }

  zbd_close(read_f_);
  zbd_close(read_direct_f_);
  zbd_close(write_f_);

  if (zone_log_file_ != nullptr) {
    fclose(zone_log_file_);
  }
}

#define LIFETIME_DIFF_NOT_GOOD (100)

unsigned int GetLifeTimeDiff(Env::WriteLifeTimeHint zone_lifetime,
                             Env::WriteLifeTimeHint lifetime) {
  assert(lifetime <= Env::WLTH_EXTREME);

  if ((lifetime == Env::WLTH_NOT_SET) || (lifetime == Env::WLTH_NONE)) {
    if (lifetime == zone_lifetime) {
      return 0;
    } else {
      return LIFETIME_DIFF_NOT_GOOD;
    }
  }

  if (zone_lifetime > lifetime) return zone_lifetime - lifetime;

  return LIFETIME_DIFF_NOT_GOOD;
}

Zone *ZonedBlockDevice::AllocateMetaZone() {
  for (const auto z : meta_zones) {
    /* If the zone is not used, reset and use it */
    if (!z->IsUsed()) {
      if (!z->IsEmpty()) {
        if (!z->Reset().ok()) {
          Warn(logger_, "Failed resetting zone!");
          continue;
        }
      }
      return z;
    }
  }
  return nullptr;
}

void ZonedBlockDevice::ResetUnusedIOZones() {
  const std::lock_guard<std::mutex> lock(zone_resources_mtx_);
  /* Reset any unused zones */
  for (const auto z : io_zones) {
    if (!z->IsUsed() && !z->IsEmpty()) {
      if (!z->IsFull()) active_io_zones_--;
      if (!z->Reset().ok()) Warn(logger_, "Failed reseting zone");
    }
  }
}

void ZonedBlockDevice::WaitUntilZoneOpenAvail() {
  std::unique_lock<std::mutex> lk(zone_resources_mtx_);
  zone_resources_.wait(lk, [this] {
    if (open_io_zones_.load() < max_nr_open_io_zones_) return true;
    return false;
  });
}

void ZonedBlockDevice::WaitUntilZoneAllocateAvail() {
  std::unique_lock<std::mutex> lk(gc_thread_mtx_);
  gc_thread_cond_.wait(lk, [this] {
    std::thread::id thread_id = std::this_thread::get_id();
    if (active_gc_ == 0 || (thread_id == gc_thread_->get_id())) {
      return true;
    }
    return false;
  });
}

template <class Duration>
bool ZonedBlockDevice::GarbageCollectionSchedule(const Duration &duration) {
  std::unique_lock<std::mutex> lk(gc_force_mtx_);
  return !gc_force_cond_.wait_for(lk, duration, [this]() {
    bool gc_force = gc_force_;
    return gc_force;
  });
}

bool ZonedBlockDevice::ZoneVictimEnableCheck(Zone *z,
                                             const bool & /*is_force */) {
  if (z->open_for_write_ || !z->IsFull() || z->used_capacity_ == 0) {
    return false;
  }

  if (z->has_meta_) {
    return false;
  }

  if (z->file_map_.size() < ZONE_FILE_MIN_MIX) {
    return false;
  }

  for (const auto item : z->file_map_) {
    if (item->file_->GetActiveZone() != nullptr) {
      return false;
    }
  }

  return true;
}

void ZonedBlockDevice::ZoneSelectVictim(const int &invalid_level,
                                        const bool &is_force) {
  for (const auto z : io_zones) {
    if (!ZoneVictimEnableCheck(z, is_force)) {
      continue;
    }
    if (z->GetInvalidPercentage() <
        ((double)invalid_level / NR_ZONE_INVALID_LEVEL) * 100.0f) {
      continue;
    }
    victim_queue_[invalid_level].push(z);
  }
}

Slice ZonedBlockDevice::ReadDataFromExtent(const ZoneMapEntry *item,
                                           char *scratch,
                                           ZoneExtent **target_extent) {
  Slice result;
  uint64_t offset, expected_offset;

  ZoneFile *file = nullptr;
  ZoneExtent *extent = nullptr;

  assert(scratch != NULL);
  memset(scratch, 0, zone_sz_);
  assert(!item->is_invalid_);

  file = item->file_;
  extent = file->GetExtent(item->extent_start_);

  assert(file != NULL && extent != NULL);

  offset = file->GetOffset(extent);
  assert(std::numeric_limits<uint64_t>::max() != offset);

  file->GetExtent(offset, &expected_offset);
  assert(extent->start_ == expected_offset);

  memset(scratch, 0, zone_sz_);

  file->PositionedRead(offset, extent->length_, &result, scratch, false);
  assert(result.size() == extent->length_);

  *target_extent = extent;

  return result;
}

IOStatus ZonedBlockDevice::CopyDataToFile(const ZoneMapEntry *item,
                                          Slice &source, char *scratch) {
  assert(!item->is_invalid_);
  ZoneFile *file = item->file_;
  uint64_t align = 0, write_size = 0, padding_size = 0;
  IOStatus s;

  assert(file != nullptr);

  /* Write data to the file */
  align = source.size() % block_sz_;
  if (align) {
    padding_size = block_sz_ - align;
  }

  memset((char *)scratch + source.size(), 0, padding_size);
  write_size = source.size() + padding_size;
  s = file->Append((char *)scratch, write_size, source.size());
  file->PushExtent();
  if (file->GetActiveZone() && file->GetActiveZone()->open_for_write_) {
    file->CloseWR();
  } else {
    file->SetActiveZone(nullptr);
  }
  return s;
}

ZoneGcState ZonedBlockDevice::ValidDataCopy(Env::WriteLifeTimeHint /*lifetime*/,
                                            Zone *z) {
  int ret = 0;
  std::vector<std::pair<ZoneFile *, uint64_t>> gc_list;
  assert(0 != z->file_map_.size());
  assert(!z->open_for_write_);
  // assert(IOStatus::OK() == z->Finish());
  // active_io_zones_--;

  /* generate some buffer for GC */
  if (!gc_buffer_) {
    assert(zone_sz_ % block_sz_ == 0);
    ret = posix_memalign((void **)&gc_buffer_, block_sz_, zone_sz_);
    assert(0 == ret);
  }

  for (const auto item : z->file_map_) {
    if (item->is_invalid_) {
      continue;
    }
    ZoneExtent *original_vector_back = nullptr;
    ZoneExtent *target_extent = nullptr;

    ZoneFile *file = item->file_;
    uint64_t file_size = 0;
    Slice result;

    assert(nullptr != file);

    file->PushExtent();
    if (file->GetActiveZone() && file->GetActiveZone()->open_for_write_) {
      file->CloseWR();
    } else {
      file->SetActiveZone(nullptr);
    }

#ifdef ZONE_CUSTOM_DEBUG
    fprintf(zone_log_file_, "%s(before)\n\t", file->GetFilename().c_str());
    for (auto extent : file->GetExtents()) {
      fprintf(zone_log_file_, "%lu(%u) ", extent->zone_->GetZoneNr(),
              extent->length_);
    }
    fprintf(zone_log_file_, "\n");
    fflush(zone_log_file_);
#endif

    file_size = file->GetFileSize();

    original_vector_back = file->GetExtents().back();

    result = ReadDataFromExtent(item, gc_buffer_, &target_extent);

    IOStatus s = CopyDataToFile(item, result, gc_buffer_);
    assert(s == IOStatus::OK());

#ifdef ZONE_CUSTOM_DEBUG
    fprintf(zone_log_file_, "%s(ongoing)\n\t", file->GetFilename().c_str());
    for (auto extent : file->GetExtents()) {
      fprintf(zone_log_file_, "%lu(%u) ", extent->zone_->GetZoneNr(),
              extent->length_);
    }
    fprintf(zone_log_file_, "\n");
    fflush(zone_log_file_);
#endif

    file->ReplaceExtent(target_extent, original_vector_back);
#ifdef ZONE_CUSTOM_DEBUG
    fprintf(zone_log_file_, "%s(after)\n\t", file->GetFilename().c_str());
    for (auto extent : file->GetExtents()) {
      fprintf(zone_log_file_, "%lu(%u) ", extent->zone_->GetZoneNr(),
              extent->length_);
    }
    fprintf(zone_log_file_, "\n");
    fflush(zone_log_file_);
#endif

#ifdef ZONE_CUSTOM_DEBUG
    if (file->GetFileSize() != file_size) {
      fprintf(zone_log_file_, "!!! %lu <==> %lu !!!", file->GetFileSize(),
              file_size);
      fflush(zone_log_file_);
    }
#endif
    assert(file->GetFileSize() == file_size);

    delete target_extent;
  }
  z->used_capacity_ = 0;
  assert(IOStatus::OK() == z->Reset());

  return ZoneGcState::NORMAL_EXIT;
}

/* Except for NORMAL_EXIT, all states must arise "continue" operation after
 * this method */
uint64_t ZonedBlockDevice::ZoneResetAndFinish(Zone *z, bool reset_condition,
                                              bool finish_condition,
                                              Zone **callback_victim) {
  Status s;
  Zone *finish_victim = nullptr;
  if (z->open_for_write_ || z->IsEmpty() || (z->IsFull() && z->IsUsed()))
    return 0;

  if (reset_condition) {
    if (!z->IsFull()) active_io_zones_--;
    s = z->Reset();
    assert(s.ok());
    if (!s.ok()) {
      Debug(logger_, "Failed resetting zone !");
    }
    return 1;
  }

  if (finish_condition) {
    /* If there is less than finish_threshold_% remaining capacity in a
     * non-open-zone, finish the zone */
    s = z->Finish();
    if (!s.ok()) {
      Debug(logger_, "Failed finishing zone");
    }
    active_io_zones_--;
  }

  if (!z->IsFull()) {
    if (finish_victim == nullptr) {
      finish_victim = z;
    } else if (finish_victim->capacity_ > z->capacity_) {
      finish_victim = z;
    }
  }

  *callback_victim = finish_victim;
  return 0;
}

int ZonedBlockDevice::AllocateEmptyZone(unsigned int best_diff,
                                        Zone *finish_victim,
                                        Zone **allocated_zone,
                                        Env::WriteLifeTimeHint lifetime) {
  Status s;
  int new_zone = 0;
  Zone *selected_zone = nullptr;

  /* If we did not find a good match, allocate an empty one */
  if (best_diff < LIFETIME_DIFF_NOT_GOOD) {
    return new_zone;
  }
  /* If we at the active io zone limit, finish an open zone(if available) with
   * least capacity left */
  if (active_io_zones_.load() == max_nr_active_io_zones_ &&
      finish_victim != nullptr) {
    s = finish_victim->Finish();
    if (!s.ok()) {
      Debug(logger_, "Failed finishing zone");
    }
    active_io_zones_--;
  }

  if (active_io_zones_.load() < max_nr_active_io_zones_) {
    for (const auto z : io_zones) {
      if ((!z->open_for_write_) && z->IsEmpty()) {
        selected_zone = z;
        break;
      }
    }  // end of for

    if (selected_zone) {
      selected_zone->lifetime_ = lifetime;
      *allocated_zone = selected_zone;
      active_io_zones_++;
      new_zone = 1;
    }
  }

  return new_zone;
}  // namespace ROCKSDB_NAMESPACE

std::string ZonedBlockDevice::GetZoneFileExt(const std::string filename) {
  std::size_t pos = filename.find(".");
  if (pos == std::string::npos) {
    return "";
  } else {
    return filename.substr(pos);
  }
}

int ZonedBlockDevice::GetAlreadyOpenZone(Zone **allocated_zone, ZoneFile *file,
                                         Env::WriteLifeTimeHint lifetime) {
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  /* Try to fill an already open zone(with the best life time diff) */

  for (const auto z : io_zones) {
    if ((!z->open_for_write_) && (z->used_capacity_ > 0) && !z->IsFull()) {
#if defined(ZONE_HOT_COLD_SEP)
      (void)best_diff;
      (void)lifetime;

      bool is_same_level = true;

      if (z->total_lifetime_ !=
          file->GetWriteLifeTimeHint() * z->file_map_.size()) {
        is_same_level = false;
      }

      if (is_same_level) {
        best_diff = 0;
        *allocated_zone = z;
      }
#else
      unsigned int diff = GetLifeTimeDiff(z->lifetime_, lifetime);
      (void)file;
      if (diff <= best_diff) {
        *allocated_zone = z;
        best_diff = diff;
      }
#endif
    }
  }
  return best_diff;
}

Zone *ZonedBlockDevice::AllocateZoneImpl(Env::WriteLifeTimeHint lifetime,
                                         ZoneFile *file) {
  Zone *allocated_zone = nullptr;
  Zone *finish_victim = nullptr;

  unsigned int best_diff;
  int new_zone;
  /* Make sure we are below the zone open limit */
  WaitUntilZoneOpenAvail();

  best_diff = GetAlreadyOpenZone(&allocated_zone, file, lifetime);
  new_zone =
      AllocateEmptyZone(best_diff, finish_victim, &allocated_zone, lifetime);

  if (allocated_zone) {
    assert(!allocated_zone->open_for_write_);
    allocated_zone->open_for_write_ = true;
    open_io_zones_++;
    Debug(logger_,
          "Allocating zone(new=%d) start: 0x%lx wp: 0x%lx lt: %d file lt: %d\n",
          new_zone, allocated_zone->start_, allocated_zone->wp_,
          allocated_zone->lifetime_, lifetime);
  }
  return allocated_zone;
}  // namespace ROCKSDB_NAMESPACE

Zone *ZonedBlockDevice::AllocateZone(Env::WriteLifeTimeHint lifetime,
                                     ZoneFile *file) {
  Zone *allocated_zone = nullptr;
  int notify_retry_counter = ZONE_MAX_NOTIFY_RETRY;
#ifdef ZONE_CUSTOM_DEBUG
  assert(nullptr != zone_log_file_);
#endif

  do {
    WaitUntilZoneAllocateAvail();
    io_zones_mtx.lock();
    allocated_zone = AllocateZoneImpl(lifetime, file);
    io_zones_mtx.unlock();
    if (!allocated_zone) {
      NotifyGarbageCollectionRun();
      std::this_thread::yield();
      notify_retry_counter--;
      if (notify_retry_counter <= 0) {
        fprintf(stderr, "Notify retry failed detected.(%s)\n",
                file->GetFilename().c_str());
        assert(notify_retry_counter > 0);
      }
    }
  } while (!allocated_zone);
  LogZoneStats();

  allocated_zone->total_lifetime_ =
      (allocated_zone->total_lifetime_ + file->GetWriteLifeTimeHint());

  return allocated_zone;
}

Zone *ZonedBlockDevice::AllocateZone(Env::WriteLifeTimeHint lifetime,
                                     ZoneFile *zone_file, Zone *before_zone) {
  Zone *zone = nullptr;

  assert(nullptr != zone_file);
#ifdef ZONE_CUSTOM_DEBUG
  assert(nullptr != zone_log_file_);
#endif

  zone = AllocateZone(lifetime, zone_file);
#ifdef ZONE_CUSTOM_DEBUG
  if (!before_zone) {
    fprintf(zone_log_file_, "%-10ld%-8s%-8d%-8lu%-45s%-10u%-10lu%-10u\n",
            (long int)((double)clock() / CLOCKS_PER_SEC * 1000), "NEW", 0,
            zone->GetZoneNr(), zone_file->GetFilename().c_str(), 0,
            zone_file->GetFileSize(),
            (unsigned int)zone_file->GetWriteLifeTimeHint());
    fflush(zone_log_file_);
  } else {
    fprintf(zone_log_file_, "%-10ld%-8s%-8lu%-8lu%-45s%-10u%-10lu%-10u\n",
            (long int)((double)clock() / CLOCKS_PER_SEC * 1000), "EXHAUST",
            before_zone->GetZoneNr(), zone->GetZoneNr(),
            zone_file->GetFilename().c_str(), 0, zone_file->GetFileSize(),
            (unsigned int)zone_file->GetWriteLifeTimeHint());
    fflush(zone_log_file_);
  }
#else
  (void)before_zone;
#endif

  return zone;
}

std::string ZonedBlockDevice::GetFilename() { return filename_; }
uint32_t ZonedBlockDevice::GetBlockSize() { return block_sz_; }

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN) && defined(LIBZBDo#endif
        // // !defined(ROCKSDB_LITE) && !defined(OS_WIN) && defined(LIBZBD)
