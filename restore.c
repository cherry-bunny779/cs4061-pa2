#include "mgit.h"

/* Helper: Check if a path exists in the target snapshot */
int path_in_snapshot(Snapshot* snap, const char* path)
{
    FileEntry* curr = snap->files;
    while (curr) {
        if (strcmp(curr->path, path) == 0)
            return 1;
        curr = curr->next;
    }
    return 0;
}

/* Helper: Reverse the linked list */
FileEntry* reverse_list(FileEntry* head)
{
    FileEntry* prev = NULL;
    FileEntry* current = head;
    while (current) {
        FileEntry* next = current->next;
        current->next = prev;
        prev = current;
        current = next;
    }
    return prev;
}

void mgit_restore(const char* id_str)
{
    if (!id_str)
        return;
    uint32_t id = atoi(id_str);

    /* 1. Load Target Snapshot */
    Snapshot* target_snap = load_snapshot_from_disk(id);
    if (!target_snap) {
        fprintf(stderr, "Error: Snapshot %d not found.\n", id);
        exit(1);
    }

    /* --- PHASE 1: SANITIZATION (The Purge) --- */
    /* Remove files that exist currently but NOT in the target snapshot. */
    FileEntry* current_files = build_file_list_bfs(".", NULL);
    FileEntry* reversed = reverse_list(current_files);

    /* Iterate through reversed list. If a path is NOT in the target snapshot, remove it. */
    FileEntry* curr = reversed;
    while (curr) {
        if (strcmp(curr->path, ".") != 0 && !path_in_snapshot(target_snap, curr->path)) {
            struct stat st;
            if (lstat(curr->path, &st) == 0) {
                if (S_ISDIR(st.st_mode)) {
                    rmdir(curr->path);
                } else {
                    unlink(curr->path);
                }
            }
        }
        curr = curr->next;
    }

    free_file_list(reversed);

    /* --- PHASE 2: RECONSTRUCTION & INTEGRITY --- */
    curr = target_snap->files;
    while (curr) {
        if (strcmp(curr->path, ".") == 0) {
            curr = curr->next;
            continue;
        }

        if (curr->is_directory) {
            /* Recreate directory if it doesn't exist */
            struct stat st;
            if (stat(curr->path, &st) != 0) {
                mkdir(curr->path, 0755);
            }
        } else {
            /* Regular file: write data from vault */
            int fd = open(curr->path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (fd < 0) {
                fprintf(stderr, "Error: Cannot create file %s\n", curr->path);
                exit(1);
            }

            /* Write each block from the vault */
            for (int b = 0; b < curr->num_blocks; b++) {
                read_blob_from_vault(curr->chunks[b].physical_offset,
                                     curr->chunks[b].compressed_size, fd);
            }
            close(fd);

            /* --- INTEGRITY CHECK (Corruption Detection) --- */
            uint8_t computed_hash[32];
            compute_hash(curr->path, computed_hash);

            if (memcmp(computed_hash, curr->checksum, 32) != 0) {
                fprintf(stderr, "Error: Data corruption detected for %s\n", curr->path);
                unlink(curr->path);
                free_file_list(target_snap->files);
                free(target_snap);
                exit(1);
            }
        }
        curr = curr->next;
    }

    /* Cleanup */
    free_file_list(target_snap->files);
    free(target_snap);
}
