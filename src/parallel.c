/* parallel.c */
#define _GNU_SOURCE
#include "parallel.h"
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>

#if !defined(__has_attribute)
#  define __has_attribute(x) 0
#endif
#ifndef LIKELY
#  define LIKELY(x)   __builtin_expect(!!(x), 1)
#endif
#ifndef UNLIKELY
#  define UNLIKELY(x) __builtin_expect(!!(x), 0)
#endif


typedef struct
{
    long begin, end, chunk;
    volatile long *next;
    pfor_body_fn body;
    void *userdata;
    int thr_idx, nthreads, ncores, pinning;
} worker_ctx;


/* 코어 핀닝 */
static void pin_to_core(int core)
{
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    if(sched_setaffinity(0, sizeof(set), &set) != 0)
    {
        perror("sched_setaffinity");
    }
}

static inline __attribute__((always_inline))
long atomic_fetch_add_long(volatile long *ptr, long inc)
{
    #if defined(__x86_64__)
        asm volatile
        (
            "lock xaddq %0, %1"
            : "+r"(inc), "+m"(*ptr)
            :
            :"memory"
        );
        return inc;
    #else
        return __sync_fetch_and_add(ptr, inc);
    #endif
}

static inline __attribute__((always_inline))
long branchless_min_long(long a, long b)
{
    #if defined(__x86_64__)
        long r = a;
        asm volatile
        (
            "cmp %1, %0\n\t"
            "cmovg %1, %0\n\t"
            :"+r"(r)
            :"r"(b)
            :"cc"
        );
        return r;
    #else
        return (a < b) ? a : b;
    #endif
}

static void *
#if __has_attribute(hot)
__attribute__((hot))
#endif
worker_main(void *arg)
{
    worker_ctx *ctx = (worker_ctx *)arg;

    if (ctx -> pinning && ctx -> ncores > 0)
    {
        int core = ctx -> thr_idx % ctx -> ncores;
        pin_to_core(core);
    }

    pfor_body_fn      body      = ctx->body;
    void             *userdata  = ctx->userdata;
    volatile long    *nextptr   = ctx->next;
    const long        end       = ctx->end;
    const long        chunk     = ctx->chunk;

    for(;;)
    {
        long start = atomic_fetch_add_long(nextptr, chunk);
        if (start >= end) break;
        long stop  = branchless_min_long(start + chunk, end);
        for (long i = start; i < stop; ++i) body(i, userdata);
    }
    return NULL;
}

int parallel_for
(
    long begin, long end, long chunk,
    int nthreads, int pinning,
    pfor_body_fn body,
    void *userdata
)
{
    if (UNLIKELY(!body || end <= begin)) return -1;
    if (chunk <= 0) chunk = 1;

    long next = begin;
    volatile long *shared_next = &next;

    long ncores = sysconf(_SC_NPROCESSORS_ONLN);
    if (ncores < 1) ncores = 1;
    if (nthreads <= 0) nthreads = (int)ncores;

    pthread_t  *ths  = (pthread_t *)calloc(nthreads, sizeof(*ths));
    worker_ctx *ctxs = (worker_ctx *)calloc(nthreads, sizeof(*ctxs));
    if (UNLIKELY(!ths || !ctxs)) { free(ths); free(ctxs); return -2; }

    for (int t = 0; t < nthreads; ++t)
    {
        ctxs[t] = (worker_ctx)
        {
            .begin=begin, .end=end, .chunk=chunk, .next=shared_next,
            .body=body, .userdata=userdata,
            .thr_idx=t, .nthreads=nthreads, .ncores=(int)ncores, .pinning=pinning
        };
        if (UNLIKELY(pthread_create(&ths[t], NULL, worker_main, &ctxs[t]) != 0))
        {
            perror("pthread_create");
            for (int j = 0; j < t; ++j) pthread_join(ths[j], NULL);
            free(ths); free(ctxs);
            return -3;
        }
    }
    for (int t = 0; t < nthreads; ++t) pthread_join(ths[t], NULL);
    free(ths); free(ctxs);
    return 0;
}
