#include "mgit.h"
#include <errno.h>

/* --- Helper Functions --- */
uint32_t get_current_head()
{
    FILE* fp = fopen(".mgit/HEAD", "r");
    if (!fp)
        return 0;
    uint32_t id = 0;
    if (fscanf(fp, "%u", &id) != 1)
        id = 0;
    fclose(fp);
    return id;
}

void update_head(uint32_t new_id)
{
    FILE* fp = fopen(".mgit/HEAD", "w");
    if (!fp) {
        fprintf(stderr, "Error: Cannot update HEAD\n");
        exit(1);
    }
    fprintf(fp, "%u", new_id);
    fclose(fp);
}

/* --- Blob Storage (Raw) --- */
void write_blob_to_vault(const char* filepath, BlockTable* block)
{
    FILE* src = fopen(filepath, "rb");
    if (!src) {
        fprintf(stderr, "Error: Cannot open %s for reading\n", filepath);
        exit(1);
    }

    FILE* vault = fopen(".mgit/data.bin", "ab");
    if (!vault) {
        fclose(src);
        fprintf(stderr, "Error: Cannot open data.bin for appending\n");
        exit(1);
    }

    /* Record current end of vault as the physical offset */
    fseek(vault, 0, SEEK_END);
    block->physical_offset = (uint64_t)ftell(vault);

    /* Read file and write to vault */
    uint8_t buf[8192];
    size_t total_written = 0;
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), src)) > 0) {
        fwrite(buf, 1, n, vault);
        total_written += n;
    }
    block->compressed_size = (uint32_t)total_written;

    fclose(src);
    fclose(vault);
}

void read_blob_from_vault(uint64_t offset, uint32_t size, int out_fd)
{
    FILE* vault = fopen(".mgit/data.bin", "rb");
    if (!vault) {
        fprintf(stderr, "Error: Cannot open data.bin for reading\n");
        exit(1);
    }

    fseek(vault, (long)offset, SEEK_SET);

    uint8_t buf[8192];
    uint32_t remaining = size;
    while (remaining > 0) {
        size_t to_read = remaining < sizeof(buf) ? remaining : sizeof(buf);
        size_t n = fread(buf, 1, to_read, vault);
        if (n == 0)
            break;
        write_all(out_fd, buf, n);
        remaining -= (uint32_t)n;
    }

    fclose(vault);
}

/* --- Snapshot Management --- */
void store_snapshot_to_disk(Snapshot* snap)
{
    char path[256];
    snprintf(path, sizeof(path), ".mgit/snapshots/snap_%03u.bin", snap->snapshot_id);

    FILE* fp = fopen(path, "wb");
    if (!fp) {
        fprintf(stderr, "Error: Cannot create snapshot file %s\n", path);
        exit(1);
    }

    /* Write header: snapshot_id, file_count, message */
    fwrite(&snap->snapshot_id, sizeof(uint32_t), 1, fp);
    fwrite(&snap->file_count, sizeof(uint32_t), 1, fp);
    fwrite(snap->message, 256, 1, fp);

    /* Write each FileEntry (fixed part) followed by its BlockTable */
    size_t fixed_size = sizeof(FileEntry) - sizeof(void*) * 2;
    FileEntry* curr = snap->files;
    while (curr) {
        fwrite(curr, fixed_size, 1, fp);
        if (curr->num_blocks > 0 && curr->chunks) {
            fwrite(curr->chunks, sizeof(BlockTable), curr->num_blocks, fp);
        }
        curr = curr->next;
    }

    fclose(fp);
}

Snapshot* load_snapshot_from_disk(uint32_t id)
{
    char path[256];
    snprintf(path, sizeof(path), ".mgit/snapshots/snap_%03u.bin", id);

    FILE* fp = fopen(path, "rb");
    if (!fp)
        return NULL;

    Snapshot* snap = calloc(1, sizeof(Snapshot));
    if (!snap) {
        fclose(fp);
        return NULL;
    }

    /* Read header */
    if (fread(&snap->snapshot_id, sizeof(uint32_t), 1, fp) != 1 ||
        fread(&snap->file_count, sizeof(uint32_t), 1, fp) != 1 ||
        fread(snap->message, 256, 1, fp) != 1) {
        free(snap);
        fclose(fp);
        return NULL;
    }

    /* Read FileEntries */
    size_t fixed_size = sizeof(FileEntry) - sizeof(void*) * 2;
    FileEntry *head = NULL, *tail = NULL;

    for (uint32_t i = 0; i < snap->file_count; i++) {
        FileEntry* fe = calloc(1, sizeof(FileEntry));
        if (!fe)
            break;

        if (fread(fe, fixed_size, 1, fp) != 1) {
            free(fe);
            break;
        }
        fe->chunks = NULL;
        fe->next = NULL;

        if (fe->num_blocks > 0) {
            fe->chunks = malloc(sizeof(BlockTable) * fe->num_blocks);
            if (fread(fe->chunks, sizeof(BlockTable), fe->num_blocks, fp) != (size_t)fe->num_blocks) {
                free(fe->chunks);
                free(fe);
                break;
            }
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
    fclose(fp);
    return snap;
}

void chunks_recycle(uint32_t target_id)
{
    /* Load the oldest snapshot (target_id) and the newest snapshot (HEAD) */
    uint32_t head_id = get_current_head();
    Snapshot* oldest = load_snapshot_from_disk(target_id);
    Snapshot* newest = load_snapshot_from_disk(head_id);

    if (!oldest || !newest) {
        if (oldest) {
            free_file_list(oldest->files);
            free(oldest);
        }
        if (newest) {
            free_file_list(newest->files);
            free(newest);
        }
        return;
    }

    /* Open data.bin in rb+ mode for zeroing stalled chunks */
    FILE* vault = fopen(".mgit/data.bin", "rb+");
    if (!vault) {
        free_file_list(oldest->files);
        free(oldest);
        free_file_list(newest->files);
        free(newest);
        return;
    }

    /* For each file in the oldest snapshot, check if its chunks are still referenced by HEAD */
    FileEntry* old_file = oldest->files;
    while (old_file) {
        if (!old_file->is_directory && old_file->num_blocks > 0 && old_file->chunks) {
            for (int b = 0; b < old_file->num_blocks; b++) {
                uint64_t old_offset = old_file->chunks[b].physical_offset;
                uint32_t old_size = old_file->chunks[b].compressed_size;
                int still_referenced = 0;

                /* Check if any file in the newest snapshot references this offset */
                FileEntry* new_file = newest->files;
                while (new_file) {
                    if (!new_file->is_directory && new_file->num_blocks > 0 && new_file->chunks) {
                        for (int nb = 0; nb < new_file->num_blocks; nb++) {
                            if (new_file->chunks[nb].physical_offset == old_offset) {
                                still_referenced = 1;
                                break;
                            }
                        }
                    }
                    if (still_referenced)
                        break;
                    new_file = new_file->next;
                }

                if (!still_referenced) {
                    /* Zero out the stalled chunk */
                    fseek(vault, (long)old_offset, SEEK_SET);
                    uint8_t zero_buf[4096];
                    memset(zero_buf, 0, sizeof(zero_buf));
                    uint32_t remaining = old_size;
                    while (remaining > 0) {
                        size_t to_write = remaining < sizeof(zero_buf) ? remaining : sizeof(zero_buf);
                        fwrite(zero_buf, 1, to_write, vault);
                        remaining -= (uint32_t)to_write;
                    }
                }
            }
        }
        old_file = old_file->next;
    }

    fclose(vault);
    free_file_list(oldest->files);
    free(oldest);
    free_file_list(newest->files);
    free(newest);
}

void mgit_snapshot(const char* msg)
{
    /* 1. Get current HEAD and calculate next_id */
    uint32_t head_id = get_current_head();
    uint32_t next_id = head_id + 1;

    /* Load previous snapshot files for crawling (Quick Check) */
    FileEntry* prev_files = NULL;
    if (head_id > 0) {
        Snapshot* prev_snap = load_snapshot_from_disk(head_id);
        if (prev_snap) {
            prev_files = prev_snap->files;
            free(prev_snap); /* Free the snapshot struct but keep the file list */
        }
    }

    /* 2. Build the new directory state via BFS */
    FileEntry* new_files = build_file_list_bfs(".", prev_files);

    /* 3. Count files and write new blobs to vault */
    uint32_t file_count = 0;
    FileEntry* curr = new_files;
    while (curr) {
        file_count++;

        if (!curr->is_directory && curr->num_blocks > 0 && curr->chunks) {
            if (curr->chunks[0].compressed_size == 0) {
                /* Check for hard links: if another file with the same inode was already written */
                FileEntry* dup = NULL;
                FileEntry* scan = new_files;
                while (scan != curr) {
                    if (!scan->is_directory && scan->inode == curr->inode &&
                        scan->num_blocks > 0 && scan->chunks &&
                        scan->chunks[0].compressed_size > 0) {
                        dup = scan;
                        break;
                    }
                    scan = scan->next;
                }

                if (dup) {
                    /* Hard link: copy offset and size from the already-written entry */
                    curr->chunks[0].physical_offset = dup->chunks[0].physical_offset;
                    curr->chunks[0].compressed_size = dup->chunks[0].compressed_size;
                } else {
                    /* Write new blob to vault */
                    write_blob_to_vault(curr->path, &curr->chunks[0]);
                }
            }
        }
        curr = curr->next;
    }

    /* 4. Create and store the snapshot */
    Snapshot snap;
    snap.snapshot_id = next_id;
    snap.file_count = file_count;
    memset(snap.message, 0, 256);
    strncpy(snap.message, msg, 255);
    snap.files = new_files;

    store_snapshot_to_disk(&snap);
    update_head(next_id);

    /* 5. Free memory */
    free_file_list(prev_files);
    free_file_list(new_files);

    /* 6. Enforce MAX_SNAPSHOT_HISTORY */
    if (next_id > MAX_SNAPSHOT_HISTORY) {
        uint32_t oldest_id = next_id - MAX_SNAPSHOT_HISTORY;
        chunks_recycle(oldest_id);

        char oldest_path[256];
        snprintf(oldest_path, sizeof(oldest_path), ".mgit/snapshots/snap_%03u.bin", oldest_id);
        remove(oldest_path);
    }
}
