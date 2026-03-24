#include "mgit.h"
#include <arpa/inet.h>
#include <errno.h>

/* --- Safe I/O Helpers --- */
ssize_t read_all(int fd, void* buf, size_t count)
{
    size_t total = 0;
    while (total < count) {
        ssize_t ret = read(fd, (char*)buf + total, count - total);
        if (ret == 0)
            break; /* EOF */
        if (ret < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        total += ret;
    }
    return (ssize_t)total;
}

ssize_t write_all(int fd, const void* buf, size_t count)
{
    size_t total = 0;
    while (total < count) {
        ssize_t ret = write(fd, (const char*)buf + total, count - total);
        if (ret < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        total += ret;
    }
    return (ssize_t)total;
}

/* --- Serialization Helper --- */
void* serialize_snapshot(Snapshot* snap, size_t* out_len)
{
    size_t total_size = sizeof(uint32_t) * 2 + 256;
    FileEntry* curr = snap->files;
    while (curr) {
        total_size += (sizeof(FileEntry) - sizeof(void*) * 2);
        if (curr->num_blocks > 0)
            total_size += (sizeof(BlockTable) * curr->num_blocks);
        curr = curr->next;
    }

    void* buf = malloc(total_size);
    if (!buf)
        return NULL;
    char* ptr = (char*)buf;

    memcpy(ptr, &snap->snapshot_id, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    memcpy(ptr, &snap->file_count, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    memcpy(ptr, snap->message, 256);
    ptr += 256;

    curr = snap->files;
    while (curr) {
        size_t fixed_size = sizeof(FileEntry) - sizeof(void*) * 2;
        memcpy(ptr, curr, fixed_size);
        ptr += fixed_size;
        if (curr->num_blocks > 0 && curr->chunks) {
            size_t blocks_size = sizeof(BlockTable) * curr->num_blocks;
            memcpy(ptr, curr->chunks, blocks_size);
            ptr += blocks_size;
        }
        curr = curr->next;
    }

    *out_len = total_size;
    return buf;
}

/* Deserialize buffer into Snapshot */
static Snapshot* deserialize_snapshot(void* buf, size_t len)
{
    (void)len;
    Snapshot* snap = calloc(1, sizeof(Snapshot));
    if (!snap)
        return NULL;

    char* ptr = (char*)buf;
    memcpy(&snap->snapshot_id, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    memcpy(&snap->file_count, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    memcpy(snap->message, ptr, 256);
    ptr += 256;

    size_t fixed_size = sizeof(FileEntry) - sizeof(void*) * 2;
    FileEntry *head = NULL, *tail = NULL;

    for (uint32_t i = 0; i < snap->file_count; i++) {
        FileEntry* fe = calloc(1, sizeof(FileEntry));
        if (!fe)
            break;

        memcpy(fe, ptr, fixed_size);
        ptr += fixed_size;
        fe->chunks = NULL;
        fe->next = NULL;

        if (fe->num_blocks > 0) {
            fe->chunks = malloc(sizeof(BlockTable) * fe->num_blocks);
            memcpy(fe->chunks, ptr, sizeof(BlockTable) * fe->num_blocks);
            ptr += sizeof(BlockTable) * fe->num_blocks;
        }

        if (!head) {
            head = fe;
            tail = fe;
        } else {
            tail->next = fe;
            tail = fe;
        }
    }

    snap->files = head;
    return snap;
}

void mgit_send(const char* id_str)
{
    Snapshot* snap = NULL;
    int live_mode = 0;

    if (id_str) {
        uint32_t id = atoi(id_str);
        snap = load_snapshot_from_disk(id);
        if (!snap) {
            fprintf(stderr, "Error: Snapshot not found.\n");
            exit(1);
        }
    } else {
        /* Live mode: build from current directory state */
        uint32_t head_id = get_current_head();
        snap = calloc(1, sizeof(Snapshot));
        snap->snapshot_id = head_id > 0 ? head_id : 1;
        snap->files = build_file_list_bfs(".", NULL);
        live_mode = 1;

        /* Count files */
        uint32_t count = 0;
        FileEntry* curr = snap->files;
        while (curr) {
            count++;
            curr = curr->next;
        }
        snap->file_count = count;
    }

    /* 1. Handshake Phase: Send MAGIC_NUMBER */
    uint32_t magic = htonl(MAGIC_NUMBER);
    write_all(STDOUT_FILENO, &magic, 4);

    /* 2. Manifest Phase: Serialize and send */
    size_t manifest_len;
    void* manifest_buf = serialize_snapshot(snap, &manifest_len);

    uint32_t net_len = htonl((uint32_t)manifest_len);
    write_all(STDOUT_FILENO, &net_len, 4);
    write_all(STDOUT_FILENO, manifest_buf, manifest_len);
    free(manifest_buf);

    /* 3. Payload Phase: Send file data chunks */
    FileEntry* curr = snap->files;
    while (curr) {
        if (!curr->is_directory && curr->num_blocks > 0 && curr->chunks) {
            for (int b = 0; b < curr->num_blocks; b++) {
                uint32_t chunk_size = curr->chunks[b].compressed_size;
                if (chunk_size > 0) {
                    if (!live_mode) {
                        /* DISK MODE: Read from data.bin */
                        FILE* vault = fopen(".mgit/data.bin", "rb");
                        if (vault) {
                            fseek(vault, (long)curr->chunks[b].physical_offset, SEEK_SET);
                            uint8_t buf[8192];
                            uint32_t remaining = chunk_size;
                            while (remaining > 0) {
                                size_t to_read = remaining < sizeof(buf) ? remaining : sizeof(buf);
                                size_t n = fread(buf, 1, to_read, vault);
                                if (n == 0)
                                    break;
                                write_all(STDOUT_FILENO, buf, n);
                                remaining -= (uint32_t)n;
                            }
                            fclose(vault);
                        }
                    } else {
                        /* LIVE MODE: Read directly from the file */
                        FILE* f = fopen(curr->path, "rb");
                        if (f) {
                            uint8_t buf[8192];
                            size_t n;
                            while ((n = fread(buf, 1, sizeof(buf), f)) > 0) {
                                write_all(STDOUT_FILENO, buf, n);
                            }
                            fclose(f);
                        }
                    }
                }
            }
        }
        curr = curr->next;
    }

    free_file_list(snap->files);
    free(snap);
}

void mgit_receive(const char* dest_path)
{
    /* 1. Setup: Create destination and init */
    struct stat st;
    if (stat(dest_path, &st) != 0) {
        mkdir(dest_path, 0755);
    }

    /* Save current directory and chdir to dest */
    char orig_dir[4096];
    if (!getcwd(orig_dir, sizeof(orig_dir))) {
        fprintf(stderr, "Error: getcwd failed\n");
        exit(1);
    }
    if (chdir(dest_path) != 0) {
        fprintf(stderr, "Error: Cannot chdir to %s\n", dest_path);
        exit(1);
    }

    /* Init if needed */
    mgit_init();

    /* 2. Handshake Phase: Read and verify MAGIC_NUMBER */
    uint32_t magic;
    if (read_all(STDIN_FILENO, &magic, 4) != 4) {
        fprintf(stderr, "Error: Failed to read handshake\n");
        exit(1);
    }
    if (ntohl(magic) != MAGIC_NUMBER) {
        fprintf(stderr, "Error: Invalid protocol\n");
        exit(1);
    }

    /* 3. Manifest Reconstruction */
    uint32_t net_len;
    if (read_all(STDIN_FILENO, &net_len, 4) != 4) {
        fprintf(stderr, "Error: Failed to read manifest size\n");
        exit(1);
    }
    size_t manifest_len = ntohl(net_len);

    void* buf = malloc(manifest_len);
    if (!buf) {
        fprintf(stderr, "Error: malloc failed\n");
        exit(1);
    }
    if (read_all(STDIN_FILENO, buf, manifest_len) != (ssize_t)manifest_len) {
        fprintf(stderr, "Error: Failed to read manifest\n");
        free(buf);
        exit(1);
    }

    Snapshot* snap = deserialize_snapshot(buf, manifest_len);
    free(buf);
    if (!snap) {
        fprintf(stderr, "Error: Failed to deserialize manifest\n");
        exit(1);
    }

    /* 4. Processing Chunks: Read data from stdin and append to local data.bin */
    FILE* vault = fopen(".mgit/data.bin", "ab");
    if (!vault) {
        fprintf(stderr, "Error: Cannot open data.bin for appending\n");
        exit(1);
    }

    FileEntry* curr = snap->files;
    while (curr) {
        if (!curr->is_directory && curr->num_blocks > 0 && curr->chunks) {
            for (int b = 0; b < curr->num_blocks; b++) {
                uint32_t chunk_size = curr->chunks[b].compressed_size;

                /* Record where we're appending in the local vault */
                fseek(vault, 0, SEEK_END);
                uint64_t local_offset = (uint64_t)ftell(vault);

                if (chunk_size > 0) {
                    /* Read chunk data from stdin and write to local vault */
                    uint8_t iobuf[8192];
                    uint32_t remaining = chunk_size;
                    while (remaining > 0) {
                        size_t to_read = remaining < sizeof(iobuf) ? remaining : sizeof(iobuf);
                        ssize_t n = read_all(STDIN_FILENO, iobuf, to_read);
                        if (n <= 0)
                            break;
                        fwrite(iobuf, 1, (size_t)n, vault);
                        remaining -= (uint32_t)n;
                    }
                }

                /* Update physical_offset to local vault location */
                curr->chunks[b].physical_offset = local_offset;
            }
        }
        curr = curr->next;
    }

    fclose(vault);

    /* 5. Save the new snapshot to disk and update HEAD */
    uint32_t local_head = get_current_head();
    uint32_t new_id = local_head + 1;
    snap->snapshot_id = new_id;

    store_snapshot_to_disk(snap);
    update_head(new_id);

    free_file_list(snap->files);
    free(snap);

    /* Return to original directory */
    if (chdir(orig_dir) != 0) {
        fprintf(stderr, "Error: Cannot chdir back\n");
    }
}

void mgit_show(const char* id_str)
{
    Snapshot* snap = NULL;

    if (id_str) {
        /* Load a specific snapshot */
        uint32_t id = atoi(id_str);
        snap = load_snapshot_from_disk(id);

        if (!snap) {
            fprintf(stderr, "Error: Snapshot not found.\n");
            return;
        }
        printf("SNAPSHOT %d: %s\n", snap->snapshot_id, snap->message);
    } else {
        /* Live view: build from current directory */
        snap = calloc(1, sizeof(Snapshot));
        snap->files = build_file_list_bfs(".", NULL);
        printf("LIVE VIEW\n");
    }

    /* Traverse and print the file list */
    FileEntry* c = snap->files;
    while (c) {
        printf("%s %s\n", c->is_directory ? "DIR " : "FILE", c->path);
        c = c->next;
    }

    /* Clean up */
    free_file_list(snap->files);
    free(snap);
}
