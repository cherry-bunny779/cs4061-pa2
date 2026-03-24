#ifndef MGIT_H
#define MGIT_H

#include <arpa/inet.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define MAGIC_NUMBER 0x4D474954
#define ZSTD_COMPRESSION_LEVEL 3
#define MAX_SNAPSHOT_HISTORY 5

// Represents a chunk in data.bin
typedef struct BlockTable {
    uint64_t physical_offset; // Location in data.bin
    uint32_t compressed_size; // Size of the chunk
} BlockTable;

// Represents a file or directory
typedef struct FileEntry {
    char path[4096];
    off_t size;
    uint8_t checksum[32]; // SHA-256
    int is_directory;
    int num_blocks; // Typically 1
    time_t mtime; // For Quick Check
    ino_t inode; // For Hard Link Deduplication

    BlockTable* chunks; // Array of blocks
    struct FileEntry* next; // BFS flattened list pointer
} FileEntry;

// Represents a full snapshot state
typedef struct Snapshot {
    uint32_t snapshot_id;
    uint32_t file_count;
    char message[256];
    FileEntry* files; // Head of the linked list
} Snapshot;

// --- Function Prototypes ---

void mgit_init();
void mgit_snapshot(const char* msg);
void mgit_show(const char* id_str);
void mgit_send(const char* id_str);
void mgit_receive(const char* dest_path);
void mgit_restore(const char* id_str); // NEW: Restore Command

// Crawler
FileEntry* build_file_list_bfs(const char* root, FileEntry* prev_snap_files);
void free_file_list(FileEntry* head);
void compute_hash(const char* path, uint8_t* output); // Exposed for Integrity Check

// Storage
void store_snapshot_to_disk(Snapshot* snap);
Snapshot* load_snapshot_from_disk(uint32_t id);
void write_blob_to_vault(const char* filepath, BlockTable* block);
void read_blob_from_vault(uint64_t offset, uint32_t size, int out_fd);

// Stream
ssize_t read_all(int fd, void* buf, size_t count);
ssize_t write_all(int fd, const void* buf, size_t count);

void update_head(uint32_t new_id);
uint32_t get_current_head();

#endif
