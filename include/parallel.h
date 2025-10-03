#pragma once
#include <stddef.h> 

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*pfor_body_fn)(long i, void *userdata);

enum parallel_options
{
    PARALLEL_OPT_PIN_CORE = 1 << 0,
    PARALLEL_OPT_REALTIME = 1 << 1
};

int parallel_for(long begin, long end, long chunk,
                 int nthreads, int options,
                 pfor_body_fn body, void *userdata);

#ifdef __cplusplus
} /* extern "C" */
#endif
