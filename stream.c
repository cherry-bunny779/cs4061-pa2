#include "mgit.h"
#include <arpa/inet.h> // For htonl/ntohl

// --- Safe I/O Helpers ---
// These are essential for handling partial reads/writes in Pipes/Sockets.
ssize_t read_all(int fd, void* buf, size_t count)
{
    size_t total = 0;
    while (total < count) {
        ssize_t ret = read(fd, (char*)buf + total, count - total);
        if (ret == 0)
            break; // EOF
        if (ret < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        total += ret;
    }
    return total;
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
    return total;
}

// --- Serialization Helper ---
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
    void* ptr = buf;

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
        if (curr->num_blocks > 0) {
            size_t blocks_size = sizeof(BlockTable) * curr->num_blocks;
            memcpy(ptr, curr->chunks, blocks_size);
            ptr += blocks_size;
        }
        curr = curr->next;
    }

    *out_len = total_size;
    return buf;
}

void mgit_send(const char* id_str)
{
    // 1. Handshake Phase
    // TODO: Send the MAGIC_NUMBER (0x4D474954) to STDOUT.
    uint32_t magic = htonl(MAGIC_NUMBER);
    write_all(STDOUT_FILENO, &magic, 4);
    // 2. Manifest Phase
    // TODO: Serialize the snapshot metadata and send its size followed by the buffer.
    size_t manifest_len;
    void* manifest_buf = serialize_snapshot(snap, &manifest_len);
    // 3. Payload Phase
    // TODO: Iterate through the files in the snapshot.
    // - If DISK MODE: Read the compressed chunks from ".mgit/data.bin" and write_all to STDOUT.
    // - If LIVE MODE: The chunks are already compressed in memory; send them directly.
    uint32_t net_len = htonl(manifest_len);
    write_all(STDOUT_FILENO, &net_len, 4);
    write_all(STDOUT_FILENO, manifest_buf, manifest_len);
    free();
}

void mgit_receive(const char* dest_path)
{
    // 1. Setup
    // TODO: mkdir(dest_path) and mgit_init() inside it.

    uint32_t magic;
    if (read_all(STDIN_FILENO, &magic, 4) != 4)
        exit(1);
    if (ntohl(magic) != MAGIC_NUMBER) {
        fprintf(stderr, "Error: Invalid protocol\n");
        exit(1);
    }

    // 2. Handshake Phase
    // TODO: read_all from STDIN and verify the MAGIC_NUMBER.

    uint32_t net_len;
    if (read_all(STDIN_FILENO, &net_len, 4) != 4)
        exit(1);
    size_t manifest_len = ntohl(net_len);

    // 3. Manifest Reconstruction
    // TODO: Read the manifest size, allocate memory, and read the serialized data.
    // Reconstruct the linked list of FileEntries.

    // 4. Processing Chunks (The Streaming OS Challenge)
    // TODO: Open ".mgit/data.bin" for appending.
    // For each file in the manifest:
    //   1. Open the file in the workspace for writing ("wb").
    //   2. While reading the compressed chunk from STDIN:
    //      - Write the raw compressed bytes into the local ".mgit/data.bin".
    //      - HINT: Don't forget to update physical_offset for the local vault!

    // 5. Cleanup
    // TODO: Save the new snapshot to disk and update HEAD.
}

void mgit_show(const char* id_str)
{
    Snapshot* snap = NULL;

    // Logic for loading a specific snapshot vs. a live view
    if (id_str) {
        /* 1. Convert id_str to an integer and load the snapshot */
        snap = ____________________________________________;

        if (!snap) {
            fprintf(stderr, "Error: Snapshot not found.\n");
            return;
        }
        printf("SNAPSHOT %d: %s\n", snap->snapshot_id, snap->message);
    } else {
        /* 2. Initialize a new snapshot structure for the live view */
        snap = ____________________________________________;

        /* 3. Populate the file list using a BFS traversal starting at "." */
        snap->files = _____________________________________;

        printf("LIVE VIEW\n");
    }

    /* 4. Traverse and print the file list */
    FileEntry* c = ____________;
    while (____) {
        printf("%s %s\n", c->is_directory ? "DIR " : "FILE", c->path);

        /* 5. Move to the next entry */
        c = ____________;
    }

    /* 6. Clean up resources to prevent memory leaks */
    ____________________________________;
    ____________________________________;
}
