#include "mgit.h"

// Helper: Check if a path exists in the target snapshot
int path_in_snapshot(Snapshot* snap, const char* path)
{
    // TODO: Iterate over snap->files and return 1 if the path matches, 0 otherwise.
    return 0;
}

// Helper: Reverse the linked list
FileEntry* reverse_list(FileEntry* head)
{
    // TODO: Standard linked list reversal.
    // Why do we need this? Because BFS gives us Root -> Children.
    // To safely delete directories, we need to process Children -> Root.
    return NULL;
}

void mgit_restore(const char* id_str)
{
    if (!id_str)
        return;
    uint32_t id = atoi(id_str);

    // 1. Load Target Snapshot
    Snapshot* target_snap = load_snapshot_from_disk(id);
    if (!target_snap) {
        fprintf(stderr, "Error: Snapshot %d not found.\n", id);
        exit(1);
    }

    // --- PHASE 1: SANITIZATION (The Purge) ---
    // Remove files that exist currently but NOT in the target snapshot.
    FileEntry* current_files = build_file_list_bfs(".", NULL);
    FileEntry* reversed = reverse_list(current_files);

    // TODO: Iterate through 'reversed'.
    // If a file/dir exists on disk (but is not ".") AND is not in target_snap:
    //   - Use rmdir() if it's a directory.
    //   - Use unlink() if it's a file.

    free_file_list(reversed);

    // --- PHASE 2: RECONSTRUCTION & INTEGRITY ---
    // TODO: Iterate through target_snap->files.

    // HINT:
    // 1. If it's a directory (and not "."), recreate it using mkdir() with 0755.
    // 2. If it's a file, open it for writing ("wb").
    // 3. For each block in curr->chunks, call read_blob_from_vault() to write the data back to disk.

    // --- INTEGRITY CHECK (Corruption Detection) ---
    // TODO: After writing a file, compute its hash using your compute_hash() function.
    // Compare the newly computed hash with the curr->checksum stored in the snapshot.
    // If they do not match (memcmp), print a corruption error, unlink() the bad file,
    // and exit(1) to abort the restore.

    // Cleanup
    free_file_list(target_snap->files);
    free(target_snap);
}
