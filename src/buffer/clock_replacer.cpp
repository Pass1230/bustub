//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  this->capacity = num_pages;
  this->clock_hand = 0;
  this->buffer_pool.resize(num_pages, -1);
  this->ref_bits.resize(num_pages, 0);
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  int sweep_num = 0;
  int size = replacer.size();
  if (size == 0) { return false; }

  while (sweep_num < size) {
    clock_hand %= capacity;

    // If the slot is empty or pinned (the page is not in the replacer)
    if (buffer_pool[clock_hand] == -1 || !replacer.count(buffer_pool[clock_hand])) {
      clock_hand++;
      continue;
    }

    // If the reference bit is 1, set 0
    if (ref_bits[clock_hand] == 1) {
      sweep_num++;
      ref_bits[clock_hand] = 0;
      clock_hand++;
    } else {  // If the reference bit is 0, evict the page (the victim)
      int victim_id = buffer_pool[clock_hand];
      *frame_id = victim_id;
      replacer.erase(victim_id);
      buffer_pool[clock_hand] = -1;
      page_table.erase(victim_id);
      clock_hand++;
      return true;
    }
  }

  // If no victim after sweeping, find the smallest frame_id as victim
  int smallest_id = INT_MAX;
  for (int i = 0; i < capacity; i++) {
    if (buffer_pool[i] == -1 || !replacer.count(buffer_pool[i])) {
      continue;
    }

    if (buffer_pool[i] < smallest_id) {
      smallest_id = buffer_pool[i];
    }

    *frame_id = smallest_id;
    replacer.erase(smallest_id);
    buffer_pool[clock_hand] = -1;
    page_table.erase(smallest_id);
  }
  return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  if (frame_id > capacity || !replacer.count(frame_id)) { return; }
  replacer.erase(frame_id);
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  if (frame_id > capacity) {return;}

  // If not in the buffer pool, add it to the buffer pool and the replacer
  if (!page_table.count(frame_id)) {
    for (int i = 0; i < capacity; i++) {
      // Find the empty slot clockwise from the initial clock hand position
      int slot = (i + clock_hand) % capacity;
      if (buffer_pool[slot] == -1) {
        buffer_pool[slot] = frame_id;
        page_table[frame_id] = slot;
        replacer.insert(frame_id);
        ref_bits[slot] = 1;
        break;
      }
    }
  } else {  // If in the buffer pool, add it to the replacer
    replacer.insert(frame_id);
    ref_bits[page_table[frame_id]] = 1;
  }
}

size_t ClockReplacer::Size() {
  int size = replacer.size();
  return size;
}

}  // namespace bustub