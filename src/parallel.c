/* parallel.c */
#define _GNU_SOURCE
#include "parallel.h"
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>

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
    int thr_idx, nthreads, ncores, flags;
    int assigned_core;
} worker_ctx;


/* 코어 핀닝 */
static void pin_to_core(int core)
{
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
    if (rc != 0)
    {
        errno = rc;
        perror("pthread_setaffinity_np");
    }
}

static void warn_realtime_failure(const char *msg)
{
    int err = errno;
    static int warned = 0;
    if (!warned && __sync_bool_compare_and_swap(&warned, 0, 1))
    {
        fprintf(stderr, "%s: %s\n", msg, strerror(err));
        if (err == EPERM)
        {
            fprintf(stderr, "hint: realtime scheduling requires CAP_SYS_NICE (root privileges).\n");
        }
    }
}

static void try_enable_realtime(void)
{
#if defined(_POSIX_PRIORITY_SCHEDULING)
    const int policy = SCHED_FIFO;
    int max_prio = sched_get_priority_max(policy);
    if (max_prio < 0)
    {
        warn_realtime_failure("sched_get_priority_max");
        return;
    }

    struct sched_param sp = { .sched_priority = max_prio };
    if (pthread_setschedparam(pthread_self(), policy, &sp) != 0)
    {
        warn_realtime_failure("pthread_setschedparam");
        return;
    }
#else
    (void)0;
#endif
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
    #elif defined(__aarch64__)
        long old, newv;
        unsigned int tmp;
        asm volatile
        (
            "1:\n\t"
            "ldaxr %0, [%3]\n\t"
            "add %1, %0, %4\n\t"
            "stlxr %w2, %1, [%3]\n\t"
            "cbnz %w2, 1b\n\t"
            : "=&r"(old), "=&r"(newv), "=&r"(tmp)
            : "r"(ptr), "r"(inc)
            : "memory"
        );
        return old;
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
    #elif defined(__aarch64__)
        long r = a;
        asm volatile
        (
            "cmp %0, %1\n\t"
            "csel %0, %0, %1, lt\n\t"
            : "+r"(r)
            : "r"(b)
            : "cc"
        );
        return r;
    #else
        return (a < b) ? a : b;
    #endif
}

static inline __attribute__((always_inline))
void execute_chunk(long start, long stop, pfor_body_fn body, void *userdata)
{
    if (UNLIKELY(start >= stop)) return;
    for (long i = start; i < stop; ++i) body(i, userdata);
}

static void *
#if __has_attribute(hot)
__attribute__((hot))
#endif
worker_main(void *arg)
{
    worker_ctx *ctx = (worker_ctx *)arg;

    if ((ctx->flags & PARALLEL_OPT_PIN_CORE) && ctx->assigned_core >= 0)
    {
        pin_to_core(ctx->assigned_core);
    }

    if (ctx->flags & PARALLEL_OPT_REALTIME)
    {
        try_enable_realtime();
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
        execute_chunk(start, stop, body, userdata);
    }
    return NULL;
}

int parallel_for
(
    long begin, long end, long chunk,
    int nthreads, int options,
    pfor_body_fn body,
    void *userdata
)
{
    if (UNLIKELY(!body || end <= begin)) return -1;
    if (chunk <= 0) chunk = 1;

    volatile long __attribute__((aligned(64))) next = begin;
    volatile long *shared_next = &next;

    long sys_ncores = sysconf(_SC_NPROCESSORS_ONLN);
    if (sys_ncores < 1) sys_ncores = 1;

    cpu_set_t affinity_mask;
    int *core_ids = NULL;
    int core_count = 0;
    if (options & PARALLEL_OPT_PIN_CORE)
    {
        CPU_ZERO(&affinity_mask);
        if (sched_getaffinity(0, sizeof(affinity_mask), &affinity_mask) == 0)
        {
            // Limit pinning choices to the CPUs currently available to this process.
            int available = CPU_COUNT(&affinity_mask);
            if (available > 0)
            {
                core_ids = (int *)malloc((size_t)available * sizeof(*core_ids));
                if (core_ids)
                {
                    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu)
                    {
                        if (!CPU_ISSET(cpu, &affinity_mask)) continue;
                        core_ids[core_count++] = cpu;
                        if (core_count == available) break;
                    }
                }
                else
                {
                    core_count = 0;
                }
            }
        }
    }

    int active_cores = core_count > 0 ? core_count : (int)sys_ncores;
    if (active_cores < 1) active_cores = 1;
    if (nthreads <= 0) nthreads = active_cores;
    if ((options & PARALLEL_OPT_PIN_CORE) && nthreads > active_cores)
    {
        nthreads = active_cores;
    }

    pthread_t  *ths  = (pthread_t *)calloc(nthreads, sizeof(*ths));
    worker_ctx *ctxs = (worker_ctx *)calloc(nthreads, sizeof(*ctxs));
    if (UNLIKELY(!ths || !ctxs)) { free(ths); free(ctxs); free(core_ids); return -2; }

    for (int t = 0; t < nthreads; ++t)
    {
        int chosen_core = -1;
        if (options & PARALLEL_OPT_PIN_CORE)
        {
            if (core_count > 0)
                chosen_core = core_ids[t % core_count];
            else if (active_cores > 0)
                chosen_core = t % active_cores;
        }

        ctxs[t] = (worker_ctx)
        {
            .begin=begin, .end=end, .chunk=chunk, .next=shared_next,
            .body=body, .userdata=userdata,
            .thr_idx=t, .nthreads=nthreads, .ncores=active_cores,
            .flags=options, .assigned_core=chosen_core
        };

        pthread_attr_t attr;
        pthread_attr_t *attr_ptr = NULL;
        if (chosen_core >= 0)
        {
            if (pthread_attr_init(&attr) == 0)
            {
                cpu_set_t set;
                CPU_ZERO(&set);
                CPU_SET(chosen_core, &set);
                if (pthread_attr_setaffinity_np(&attr, sizeof(set), &set) == 0)
                {
                    attr_ptr = &attr;
                }
                else
                {
                    pthread_attr_destroy(&attr);
                }
            }
        }

        int create_rc = pthread_create(&ths[t], attr_ptr, worker_main, &ctxs[t]);
        if (attr_ptr)
        {
            pthread_attr_destroy(&attr);
        }
        if (UNLIKELY(create_rc != 0))
        {
            perror("pthread_create");
            for (int j = 0; j < t; ++j) pthread_join(ths[j], NULL);
            free(ths); free(ctxs); free(core_ids);
            return -3;
        }
    }
    for (int t = 0; t < nthreads; ++t) pthread_join(ths[t], NULL);
    free(ths); free(ctxs); free(core_ids);
    return 0;
}
