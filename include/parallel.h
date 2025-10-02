#pragma once
#include <stddef.h> 

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*pfor_body_fn)(long i, void *userdata);

int parallel_for(long begin, long end, long chunk,
                 int nthreads, int pinning,
                 pfor_body_fn body, void *userdata);

#ifdef __cplusplus
} /* extern "C" */
#endif
