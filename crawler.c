#include "mgit.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

// Helper to calculate SHA256 using system utility
void compute_hash(const char* path, uint8_t* output)
{
    // TODO: Set up a pipe to capture the output of the sha256sum command.
    // HINT: Use pipe().

    // TODO: Fork a child process.
    // HINT: In the child process:
    //       1. Close the read end of the pipe.
    //       2. Use dup2() to redirect STDOUT to the write end of the pipe.
    //       3. Silence STDERR by opening "/dev/null" and redirecting it with dup2().
    //       4. Use execlp() to run "sha256sum".

    // HINT: In the parent process:
    //       1. Close the write end of the pipe.
    //       2. Read exactly 64 characters (the hex string) from the read end.
    //       3. Convert the hex string into 32 bytes and store it in 'output'.
    //       4. Remember to wait() for the child to finish!
}

// Check if file matches previous snapshot (Quick Check)
FileEntry* find_in_prev(FileEntry* prev, const char* path)
{
    // TODO: Iterate through the 'prev' linked list.
    // Return the FileEntry if its path matches the requested path, otherwise return NULL.
    return NULL;
}

// HELPER: Check if an inode already exists in the current snapshot's list
FileEntry* find_in_current_by_inode(FileEntry* head, ino_t inode)
{
    while (head) {
        if (!head->is_directory && head->inode == inode)
            return head;
        head = head->next;
    }
    return NULL;
}

FileEntry* build_file_list_bfs(const char* root, FileEntry* prev_snap_files)
{
    FileEntry *head = NULL, *tail = NULL;

    // TODO: 1. Initialize the Root directory "." and add it to your BFS queue/list.

    // TODO: 2. Implement Level-Order Traversal (BFS)
    // - Open directories using opendir() and readdir().
    // - Ignore "." and ".." and the ".mgit" folder.
    // - Construct the full file path safely to avoid buffer overflows.
    // - Use stat() to gather size, mtime, inode, and directory status.

    // TODO: 3. Deduplication (Quick Check)
    // - First, check if the inode was already seen in the CURRENT snapshot (Hard Link).
    // - Next, check if the file matches the PREVIOUS snapshot (mtime & size match).
    // - If it matches, copy the checksum and block metadata. DO NOT re-hash.

    // TODO: 4. Deep Check
    // - If the file is modified or new, use compute_hash() to generate the SHA-256.
    // - Allocate the BlockTable (chunks). Note: physical_offset is set later in storage.c.

    // TODO: 5. Append new FileEntry to your linked list.

    return head;
}

void free_file_list(FileEntry* head)
{
    // TODO: Iterate through the linked list and free() each node,
    // including the dynamically allocated 'chunks' array within each node.
}
