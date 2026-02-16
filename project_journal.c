
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <stdbool.h>


#define FS_MAGIC           0x56534653U
#define BLOCK_SIZE         4096U
#define INODE_SIZE         128U
#define SUPERBLOCK_IDX     0U
#define JOURNAL_BLOCK_IDX  1U
#define JOURNAL_BLOCKS     16U
#define INODE_BMAP_IDX     17U
#define DATA_BMAP_IDX      18U
#define INODE_START_IDX    19U
#define DATA_START_IDX     21U
#define TOTAL_BLOCKS       85U
#define ROOT_INODE_NUM     0U
#define DIRECT_POINTERS    8U
#define NAME_LEN           28U


#define JOURNAL_MAGIC      0x4A524E4C
#define REC_DATA           1
#define REC_COMMIT         2
#define DEFAULT_IMAGE      "vsfs.img"



struct superblock {
    uint32_t magic;
    uint32_t block_size;
    uint32_t total_blocks;
    uint32_t inode_count;
    uint32_t journal_block;
    uint32_t inode_bitmap;
    uint32_t data_bitmap;
    uint32_t inode_start;
    uint32_t data_start;
    uint8_t  _pad[128 - 9 * 4];
};

struct inode {
    uint16_t type;     
    uint16_t links;
    uint32_t size;
    uint32_t direct[DIRECT_POINTERS];
    uint32_t ctime;
    uint32_t mtime;
    uint8_t _pad[128 - (2 + 2 + 4 + DIRECT_POINTERS * 4 + 4 + 4)];
};

struct dirent {
    uint32_t inode;
    char name[NAME_LEN];
};

struct journal_header {
    uint32_t magic;       
    uint32_t nbytes_used; 
};

struct rec_header {
    uint16_t type; 
    uint16_t size; 
};

struct data_record {
    struct rec_header hdr;
    uint32_t block_no;    
    uint8_t data[BLOCK_SIZE];
} __attribute__((packed));

struct commit_record {
    struct rec_header hdr;
} __attribute__((packed));




void die(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

void read_block(int fd, uint32_t block_idx, void *buffer) {
    if (pread(fd, buffer, BLOCK_SIZE, block_idx * BLOCK_SIZE) != BLOCK_SIZE) {
        die("pread block");
    }
}

int is_bit_set(uint8_t *bitmap, int index) {
    return (bitmap[index / 8] >> (index % 8)) & 1;
}

void set_bit(uint8_t *bitmap, int index) {
    bitmap[index / 8] |= (1 << (index % 8));
}



void cmd_create(int fd, const char *filename) {
    if (strlen(filename) >= NAME_LEN) {
        fprintf(stderr, "Error: Filename too long.\n");
        exit(EXIT_FAILURE);
    }

    struct superblock sb;
    read_block(fd, SUPERBLOCK_IDX, &sb);
    if (sb.magic != FS_MAGIC) die("Invalid VSFS magic");

    struct journal_header jh;
    if (pread(fd, &jh, sizeof(jh), JOURNAL_BLOCK_IDX * BLOCK_SIZE) != sizeof(jh)) die("read journal header");
    
    if (jh.magic != JOURNAL_MAGIC) {
        jh.magic = JOURNAL_MAGIC;
        jh.nbytes_used = sizeof(struct journal_header);
    }

    size_t data_rec_size = sizeof(struct rec_header) + sizeof(uint32_t) + BLOCK_SIZE;
    size_t commit_rec_size = sizeof(struct rec_header);
    size_t txn_size = (3 * data_rec_size) + commit_rec_size;

    size_t journal_capacity = JOURNAL_BLOCKS * BLOCK_SIZE;
    if (jh.nbytes_used + txn_size > journal_capacity) {
        fprintf(stderr, "Error: Journal full. Please run './journal install' first.\n");
        exit(EXIT_FAILURE);
    }

    uint8_t inode_bitmap[BLOCK_SIZE];
    read_block(fd, INODE_BMAP_IDX, inode_bitmap);

    int free_inode_idx = -1;
    for (int i = 1; i < sb.inode_count; i++) {
        if (!is_bit_set(inode_bitmap, i)) {
            free_inode_idx = i;
            break;
        }
    }
    if (free_inode_idx == -1) {
        fprintf(stderr, "Error: No free inodes.\n");
        exit(EXIT_FAILURE);
    }

    uint8_t inode_table_block_raw[BLOCK_SIZE];
    read_block(fd, INODE_START_IDX, inode_table_block_raw);
    struct inode *inodes = (struct inode *)inode_table_block_raw;
    struct inode *root_inode = &inodes[ROOT_INODE_NUM];

    if (root_inode->direct[0] == 0) die("Root inode has no data block");
    
    uint32_t root_data_blk_idx = root_inode->direct[0];
    uint8_t root_data_block[BLOCK_SIZE];
    read_block(fd, root_data_blk_idx, root_data_block);
    
    struct dirent *entries = (struct dirent *)root_data_block;
    int max_entries = BLOCK_SIZE / sizeof(struct dirent);
    int free_dirent_idx = -1;

    for (int i = 0; i < max_entries; i++) {
  
        if (entries[i].name[0] == '\0') { 
            free_dirent_idx = i;
            break;
        }
    }

    if (free_dirent_idx == -1) {
        fprintf(stderr, "Error: Root directory full.\n");
        exit(EXIT_FAILURE);
    }

    set_bit(inode_bitmap, free_inode_idx);

    uint32_t inodes_per_block = BLOCK_SIZE / INODE_SIZE;
    uint32_t target_inode_blk_offset = free_inode_idx / inodes_per_block;
    uint32_t target_inode_blk_idx = INODE_START_IDX + target_inode_blk_offset;
    
    uint8_t target_inode_block[BLOCK_SIZE];
    read_block(fd, target_inode_blk_idx, target_inode_block); 
    
    struct inode *target_block_inodes = (struct inode *)target_inode_block;
    struct inode *new_file_inode = &target_block_inodes[free_inode_idx % inodes_per_block];
    
    memset(new_file_inode, 0, sizeof(struct inode));
    new_file_inode->type = 1; 
    new_file_inode->links = 1;
    new_file_inode->size = 0;
    new_file_inode->ctime = time(NULL);
    new_file_inode->mtime = time(NULL);

    entries[free_dirent_idx].inode = free_inode_idx;
    strncpy(entries[free_dirent_idx].name, filename, NAME_LEN);
    

    if (target_inode_blk_idx == INODE_START_IDX) {
        struct inode *root = &target_block_inodes[ROOT_INODE_NUM];
        uint32_t new_size = (free_dirent_idx + 1) * sizeof(struct dirent);
        if (root->size < new_size) {
            root->size = new_size;
        }
    }


    
    off_t current_offset = JOURNAL_BLOCK_IDX * BLOCK_SIZE + jh.nbytes_used;

    void append_data_record(uint32_t target_blk, void *data) {
        struct data_record rec;
        rec.hdr.type = REC_DATA;
        rec.hdr.size = sizeof(struct rec_header) + sizeof(uint32_t) + BLOCK_SIZE;
        rec.block_no = target_blk;
        memcpy(rec.data, data, BLOCK_SIZE);
        
        if (pwrite(fd, &rec, rec.hdr.size, current_offset) != rec.hdr.size)
            die("write journal data");
        current_offset += rec.hdr.size;
        jh.nbytes_used += rec.hdr.size;
    }

    append_data_record(INODE_BMAP_IDX, inode_bitmap);
    append_data_record(target_inode_blk_idx, target_inode_block);
    append_data_record(root_data_blk_idx, root_data_block);

    struct commit_record cm;
    cm.hdr.type = REC_COMMIT;
    cm.hdr.size = sizeof(struct rec_header);
    
    if (pwrite(fd, &cm, cm.hdr.size, current_offset) != cm.hdr.size)
        die("write journal commit");
    
    jh.nbytes_used += cm.hdr.size;

    if (pwrite(fd, &jh, sizeof(jh), JOURNAL_BLOCK_IDX * BLOCK_SIZE) != sizeof(jh))
        die("update journal header");

    printf("Successfully logged creation of file '%s' (inode %d) to journal.\n", filename, free_inode_idx);
    printf("Run './journal install' to commit changes to disk.\n");
}



void cmd_install(int fd) {
    struct journal_header jh;
    if (pread(fd, &jh, sizeof(jh), JOURNAL_BLOCK_IDX * BLOCK_SIZE) != sizeof(jh)) 
        die("read journal header");

    if (jh.magic != JOURNAL_MAGIC) {
        printf("Journal not initialized or corrupt. Nothing to install.\n");
        return;
    }

    if (jh.nbytes_used == sizeof(struct journal_header)) {
        printf("Journal is empty.\n");
        return;
    }

    uint8_t *journal_mem = malloc(JOURNAL_BLOCKS * BLOCK_SIZE);
    if (!journal_mem) die("malloc");

    if (pread(fd, journal_mem, JOURNAL_BLOCKS * BLOCK_SIZE, JOURNAL_BLOCK_IDX * BLOCK_SIZE) != JOURNAL_BLOCKS * BLOCK_SIZE)
        die("read full journal");

    size_t scan_offset = sizeof(struct journal_header);
    int committed_txns = 0;
    
    struct pending_write {
        uint32_t target_block;
        uint8_t *data_ptr; 
    };
    
    struct pending_write pending[200]; 
    int pending_count = 0;

    printf("Replaying journal...\n");

    while (scan_offset < jh.nbytes_used) {
        struct rec_header *rh = (struct rec_header *)(journal_mem + scan_offset);
        
        if (scan_offset + sizeof(struct rec_header) > jh.nbytes_used) break;
        if (scan_offset + rh->size > jh.nbytes_used) break;

        if (rh->type == REC_DATA) {
            uint32_t *blk_ptr = (uint32_t *)(journal_mem + scan_offset + sizeof(struct rec_header));
            uint8_t *data_ptr = (uint8_t *)(journal_mem + scan_offset + sizeof(struct rec_header) + sizeof(uint32_t));
            
            pending[pending_count].target_block = *blk_ptr;
            pending[pending_count].data_ptr = data_ptr;
            pending_count++;

        } else if (rh->type == REC_COMMIT) {
            for (int i = 0; i < pending_count; i++) {
                printf("  Writing block %d...\n", pending[i].target_block);
                if (pwrite(fd, pending[i].data_ptr, BLOCK_SIZE, pending[i].target_block * BLOCK_SIZE) != BLOCK_SIZE)
                    die("install pwrite");
            }
            committed_txns++;
            pending_count = 0; 
        } else {
            fprintf(stderr, "Unknown record type at offset %zu\n", scan_offset);
            break;
        }

        scan_offset += rh->size;
    }

    if (pending_count > 0) {
        printf("Warning: Found incomplete transaction at end of journal (discarded).\n");
    }

    jh.nbytes_used = sizeof(struct journal_header);
    if (pwrite(fd, &jh, sizeof(jh), JOURNAL_BLOCK_IDX * BLOCK_SIZE) != sizeof(jh))
        die("reset journal header");

    free(journal_mem);
    printf("Install complete. %d transactions replayed.\n", committed_txns);
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <command> [args...]\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    const char *command = argv[1];
    int fd = open(DEFAULT_IMAGE, O_RDWR);
    if (fd < 0) {
        perror("Could not open " DEFAULT_IMAGE);
        fprintf(stderr, "Make sure you run './mkfs' first to create the disk image.\n");
        exit(EXIT_FAILURE);
    }

    if (strcmp(command, "create") == 0) {
        if (argc != 3) {
            fprintf(stderr, "Usage: %s create <filename>\n", argv[0]);
            close(fd);
            exit(EXIT_FAILURE);
        }
        cmd_create(fd, argv[2]);
    } else if (strcmp(command, "install") == 0) {
        cmd_install(fd);
    } else {
        fprintf(stderr, "Unknown command: %s\n", command);
        close(fd);
        exit(EXIT_FAILURE);
    }

    close(fd);
    return 0;
}
