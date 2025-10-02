// demo_bzip2_parallel.c
// SPDX-License-Identifier: MIT
#define _GNU_SOURCE
#include "include/parallel.h"
#include <bzlib.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

// 타이머
static inline double sec_now(void){
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec*1e-9;
}

// 파일 읽기
static unsigned char *read_file(const char *path, size_t *len_out){
    FILE *f = fopen(path, "rb");
    if (!f) { perror("fopen"); return NULL; }
    fseek(f, 0, SEEK_END); long sz = ftell(f);
    if (sz < 0) { perror("ftell"); fclose(f); return NULL; }
    fseek(f, 0, SEEK_SET);
    unsigned char *buf = (unsigned char*)malloc((size_t)sz);
    if (!buf) { fclose(f); return NULL; }
    if (fread(buf, 1, (size_t)sz, f) != (size_t)sz) { perror("fread"); free(buf); fclose(f); return NULL; }
    fclose(f);
    *len_out = (size_t)sz;
    return buf;
}

// 파일 쓰기
static int write_file(const char *path, const unsigned char *data, size_t len){
    FILE *f = fopen(path, "wb");
    if (!f) { perror("fopen"); return -1; }
    if (fwrite(data, 1, len, f) != len) { perror("fwrite"); fclose(f); return -1; }
    fclose(f);
    return 0;
}

// 블록 메타
typedef struct {
    const unsigned char *in;
    unsigned int in_len;
    unsigned char *out;         // 압축 출력 버퍼
    unsigned int out_cap;       // 할당 크기
    unsigned int out_len;       // 실제 압축된 길이
    int ok;                     // 0=fail, 1=success
} Block;

typedef struct {
    Block *blocks;
    int level; // 1..9 (bzip2 압축 레벨)
} CompressCtx;

// bzip2 블록 압축 (한 블록 = 하나의 .bz2 스트림 생성)
static void compress_block(long bi, void *ud){
    CompressCtx *C = (CompressCtx*)ud;
    Block *b = &C->blocks[bi];

    bz_stream strm; memset(&strm, 0, sizeof(strm));
    int rc = BZ2_bzCompressInit(&strm, C->level, 0, 30); // level, verbosity=0, workFactor=30(기본)
    if (rc != BZ_OK) { b->ok = 0; return; }

    strm.next_in  = (char*)b->in;
    strm.avail_in = b->in_len;
    strm.next_out = (char*)b->out;
    strm.avail_out = b->out_cap;

    // 한 번에 FINISH (블록 단위라 가능)
    rc = BZ2_bzCompress(&strm, BZ_FINISH);
    if (rc != BZ_STREAM_END) { BZ2_bzCompressEnd(&strm); b->ok = 0; return; }

    b->out_len = (unsigned int)((char*)strm.next_out - (char*)b->out);
    b->ok = 1;
    BZ2_bzCompressEnd(&strm);
}

static void usage(const char *prog){
    fprintf(stderr, "Usage: %s <input_file> [threads=8] [chunk=2048] [blockKB=900] [level=9] [pinning=1]\n", prog);
}

int main(int argc, char **argv){
    if (argc < 2) { usage(argv[0]); return 1; }
    const char *in_path = argv[1];
    int nthreads = (argc>2)? atoi(argv[2]) : 8;
    long chunk   = (argc>3)? atol(argv[3]) : 2048;
    int blockKB  = (argc>4)? atoi(argv[4]) : 900; // bzip2 기본 블록 900KB
    int level    = (argc>5)? atoi(argv[5]) : 9;   // 1..9
    int pinning  = (argc>6)? atoi(argv[6]) : 1;

    if (level < 1) level = 1;
    if (level > 9) level = 9;
    if (blockKB < 100) blockKB = 100;

    // 입력 로드
    size_t in_len = 0;
    unsigned char *in = read_file(in_path, &in_len);
    if (!in) { fprintf(stderr, "failed to read input\n"); return 1; }
    printf("Input: %s (%.2f MB)\n", in_path, in_len/1048576.0);

    // 블록화
    const unsigned int BLK = (unsigned int)blockKB * 1024;
    size_t nb = (in_len + BLK - 1) / BLK;
    Block *blocks = (Block*)calloc(nb, sizeof(Block));
    if (!blocks) { free(in); fprintf(stderr,"alloc blocks failed\n"); return 1; }

    // bzip2 worst-case out size ~= in + in/100 + 600 (여유를 넉넉히)
    for (size_t i=0;i<nb;++i){
        size_t off = i * (size_t)BLK;
        size_t len = (off + BLK <= in_len) ? BLK : (in_len - off);
        blocks[i].in = in + off;
        blocks[i].in_len = (unsigned int)len;
        size_t cap = len + len/100 + 1000;
        blocks[i].out = (unsigned char*)malloc(cap);
        blocks[i].out_cap = (unsigned int)cap;
        blocks[i].out_len = 0;
        blocks[i].ok = 0;
        if (!blocks[i].out) { fprintf(stderr,"alloc out failed @%zu\n", i); return 1; }
    }

    // 단일 스레드 참조 속도
    double t0 = sec_now();
    for (size_t i=0;i<nb;++i) {
        CompressCtx C = { .blocks = blocks, .level = level };
        compress_block((long)i, &C);
        if (!blocks[i].ok) { fprintf(stderr,"single compress fail @%zu\n", i); return 1; }
    }
    double t1 = sec_now();
    double single_s = t1 - t0;
    double single_MBps = (in_len / 1048576.0) / single_s;
    printf("single-thread: %.3fs  (%.2f MB/s)\n", single_s, single_MBps);

    // 블록 출력 합치기(옵션) — single 결과 파일
    {
        size_t out_tot = 0;
        for (size_t i=0;i<nb;++i) out_tot += blocks[i].out_len;
        unsigned char *concat = (unsigned char*)malloc(out_tot);
        size_t pos = 0;
        for (size_t i=0;i<nb;++i) { memcpy(concat+pos, blocks[i].out, blocks[i].out_len); pos += blocks[i].out_len; }
        write_file("out_single_concat.bz2", concat, out_tot);
        free(concat);
    }

    // 병렬 벤치 위해 출력 버퍼 초기화(다시 압축하도록 ok reset)
    for (size_t i=0;i<nb;++i){ blocks[i].ok = 0; blocks[i].out_len = 0; }

    // 병렬
    CompressCtx C = { .blocks = blocks, .level = level };
    double p0 = sec_now();
    int rc = parallel_for(0, (long)nb, chunk, nthreads, pinning, compress_block, &C);
    double p1 = sec_now();
    if (rc != 0) { fprintf(stderr, "parallel_for failed: %d\n", rc); return 1; }

    // 성공 여부 체크
    for (size_t i=0;i<nb;++i){
        if (!blocks[i].ok) { fprintf(stderr,"parallel compress fail @%zu\n", i); return 1; }
    }

    double par_s = p1 - p0;
    double par_MBps = (in_len / 1048576.0) / par_s;
    printf("parallel(%d thr, pin=%d, chunk=%ld): %.3fs  (%.2f MB/s)\n",
           nthreads, pinning, chunk, par_s, par_MBps);
    printf("speedup: %.2fx\n", single_s / par_s);

    // 병렬 결과도 합쳐서 기록
    {
        size_t out_tot = 0;
        for (size_t i=0;i<nb;++i) out_tot += blocks[i].out_len;
        unsigned char *concat = (unsigned char*)malloc(out_tot);
        size_t pos = 0;
        for (size_t i=0;i<nb;++i) { memcpy(concat+pos, blocks[i].out, blocks[i].out_len); pos += blocks[i].out_len; }
        write_file("out_parallel_concat.bz2", concat, out_tot);
        free(concat);
    }

    // 정리
    for (size_t i=0;i<nb;++i) free(blocks[i].out);
    free(blocks); free(in);
    return 0;
}
