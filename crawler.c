#include "mgit.h"
#include <dirent.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

/* Helper to calculate SHA256 using system utility */
void compute_hash(const char* path, uint8_t* output)
{
    int pipefd[2];
    if (pipe(pipefd) < 0) {
        fprintf(stderr, "Error: pipe() failed\n");
        exit(1);
    }

    pid_t pid = fork();
    if (pid < 0) {
        fprintf(stderr, "Error: fork() failed\n");
        exit(1);
    }

    if (pid == 0) {
        /* Child process */
        close(pipefd[0]); /* Close read end */
        dup2(pipefd[1], STDOUT_FILENO); /* Redirect stdout to pipe */
        close(pipefd[1]);

        /* Silence stderr */
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) {
            dup2(devnull, STDERR_FILENO);
            close(devnull);
        }

        execlp("sha256sum", "sha256sum", path, NULL);
        _exit(1); /* If exec fails */
    }

    /* Parent process */
    close(pipefd[1]); /* Close write end */

    char hexbuf[65];
    memset(hexbuf, 0, sizeof(hexbuf));
    ssize_t total = 0;
    while (total < 64) {
        ssize_t r = read(pipefd[0], hexbuf + total, 64 - total);
        if (r <= 0)
            break;
        total += r;
    }
    close(pipefd[0]);

    int status;
    waitpid(pid, &status, 0);

    /* Convert 64 hex chars to 32 bytes */
    for (int i = 0; i < 32; i++) {
        unsigned int byte;
        char tmp[3] = { hexbuf[i * 2], hexbuf[i * 2 + 1], '\0' };
        sscanf(tmp, "%02x", &byte);
        output[i] = (uint8_t)byte;
    }
}

/* Check if file matches previous snapshot (Quick Check) */
FileEntry* find_in_prev(FileEntry* prev, const char* path)
{
    while (prev) {
        if (strcmp(prev->path, path) == 0)
            return prev;
        prev = prev->next;
    }
    return NULL;
}

/* HELPER: Check if an inode already exists in the current snapshot's list */
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

    /* 1. Initialize the root directory "." */
    FileEntry* root_entry = calloc(1, sizeof(FileEntry));
    if (!root_entry)
        return NULL;
    strcpy(root_entry->path, ".");
    root_entry->is_directory = 1;
    root_entry->num_blocks = 0;
    root_entry->chunks = NULL;
    root_entry->next = NULL;

    struct stat st;
    if (stat(root, &st) == 0) {
        root_entry->size = st.st_size;
        root_entry->mtime = st.st_mtime;
        root_entry->inode = st.st_ino;
    }

    head = root_entry;
    tail = root_entry;

    /* 2. BFS traversal using the linked list as the queue */
    FileEntry* current = head;
    while (current) {
        if (current->is_directory) {
            DIR* dir = opendir(current->path);
            if (dir) {
                struct dirent* entry;
                while ((entry = readdir(dir)) != NULL) {
                    /* Skip . and .. and .mgit */
                    if (strcmp(entry->d_name, ".") == 0 ||
                        strcmp(entry->d_name, "..") == 0 ||
                        strcmp(entry->d_name, ".mgit") == 0)
                        continue;

                    /* Construct full path safely */
                    char fullpath[4096];
                    int written;
                    if (strcmp(current->path, ".") == 0)
                        written = snprintf(fullpath, sizeof(fullpath), "./%s", entry->d_name);
                    else
                        written = snprintf(fullpath, sizeof(fullpath), "%s/%s", current->path, entry->d_name);
                    if (written < 0 || (size_t)written >= sizeof(fullpath))
                        continue; /* Path too long, skip */

                    struct stat fst;
                    if (lstat(fullpath, &fst) != 0)
                        continue;

                    FileEntry* fe = calloc(1, sizeof(FileEntry));
                    if (!fe)
                        continue;

                    strncpy(fe->path, fullpath, sizeof(fe->path) - 1);
                    fe->size = fst.st_size;
                    fe->mtime = fst.st_mtime;
                    fe->inode = fst.st_ino;
                    fe->is_directory = S_ISDIR(fst.st_mode) ? 1 : 0;
                    fe->num_blocks = 0;
                    fe->chunks = NULL;
                    fe->next = NULL;

                    if (fe->is_directory) {
                        /* Directories have no blocks */
                    } else {
                        /* 3. Deduplication checks */

                        /* Check if inode already seen in current list (Hard Link) */
                        FileEntry* dup = find_in_current_by_inode(head, fe->inode);
                        if (dup) {
                            /* Hard link: copy metadata from the duplicate */
                            memcpy(fe->checksum, dup->checksum, 32);
                            fe->num_blocks = dup->num_blocks;
                            if (dup->num_blocks > 0 && dup->chunks) {
                                fe->chunks = malloc(sizeof(BlockTable) * dup->num_blocks);
                                memcpy(fe->chunks, dup->chunks, sizeof(BlockTable) * dup->num_blocks);
                            }
                        } else {
                            /* Quick Check: compare mtime & size with previous snapshot */
                            FileEntry* prev = find_in_prev(prev_snap_files, fe->path);
                            if (prev && prev->mtime == fe->mtime && prev->size == fe->size) {
                                /* Unchanged: copy checksum and block metadata */
                                memcpy(fe->checksum, prev->checksum, 32);
                                fe->num_blocks = prev->num_blocks;
                                if (prev->num_blocks > 0 && prev->chunks) {
                                    fe->chunks = malloc(sizeof(BlockTable) * prev->num_blocks);
                                    memcpy(fe->chunks, prev->chunks, sizeof(BlockTable) * prev->num_blocks);
                                }
                            } else {
                                /* 4. Deep Check: compute hash */
                                compute_hash(fullpath, fe->checksum);
                                /* Allocate BlockTable; physical_offset set later in storage.c */
                                fe->num_blocks = 1;
                                fe->chunks = calloc(1, sizeof(BlockTable));
                                /* size 0 signals that it needs to be written to vault */
                                fe->chunks[0].compressed_size = 0;
                                fe->chunks[0].physical_offset = 0;
                            }
                        }
                    }

                    /* 5. Append to linked list */
                    tail->next = fe;
                    tail = fe;
                }
                closedir(dir);
            }
        }
        current = current->next;
    }

    return head;
}

void free_file_list(FileEntry* head)
{
    while (head) {
        FileEntry* next = head->next;
        if (head->chunks)
            free(head->chunks);
        free(head);
        head = next;
    }
}
