/*
 * Copyright 2014, Incubaid BVBA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <time.h>
#include <errno.h>

#include <caml/alloc.h>
#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/unixsupport.h>

#define Val_none Val_int(0)
static value Val_some(value v) {
        CAMLparam1(v);
        CAMLlocal1(res);

        res = caml_alloc(1, 0);
        Store_field(res, 0, v);

        CAMLreturn(res);
}

enum clocks {
        REALTIME = 1,
        REALTIME_COARSE = 2,
        MONOTONIC = 3,
        MONOTONIC_COARSE = 4,
        MONOTONIC_RAW = 5,
        BOOTTIME = 6,
        PROCESS_CPUTIME_ID = 7,
        THREAD_CPUTIME_ID = 8
};

CAMLprim value arakoon_clocks_getclockid(value clock) {
        CAMLparam1(clock);
        CAMLlocal1(res);

        switch(Int_val(clock)) {
                case REALTIME:
                        res = Val_some(Val_int(CLOCK_REALTIME));
                        break;
#ifdef CLOCK_REALTIME_COARSE
                case REALTIME_COARSE:
                        res = Val_some(Val_int(CLOCK_REALTIME_COARSE));
                        break;
#endif
#ifdef CLOCK_MONOTONIC
                case MONOTONIC:
                        res = Val_some(Val_int(CLOCK_MONOTONIC));
                        break;
#endif
#ifdef CLOCK_MONOTONIC_COARSE
                case MONOTONIC_COARSE:
                        res = Val_some(Val_int(CLOCK_MONOTONIC_COARSE));
                        break;
#endif
#ifdef CLOCK_MONOTONIC_RAW
                case MONOTONIC_RAW:
                        res = Val_some(Val_int(CLOCK_MONOTONIC_RAW));
                        break;
#endif
#ifdef CLOCK_BOOTTIME
                case BOOTTIME:
                        res = Val_some(Val_int(CLOCK_BOOTTIME));
                        break;
#endif
#ifdef CLOCK_PROCESS_CPUTIME_ID
                case PROCESS_CPUTIME_ID:
                        res = Val_some(Val_int(CLOCK_PROCESS_CPUTIME_ID));
                        break;
#endif
#ifdef CLOCK_THREAD_CPUTIME_ID
                case THREAD_CPUTIME_ID:
                        res = Val_some(Val_int(CLOCK_THREAD_CPUTIME_ID));
                        break;
#endif
                default:
                        res = Val_none;
        }

        CAMLreturn(res);
}

CAMLprim value arakoon_clocks_gettime(value clock) {
        CAMLparam1(clock);
        CAMLlocal1(res);

        struct timespec ts;
        int rc = 0;

        rc = clock_gettime(Int_val(clock), &ts);

        if(rc != 0) {
                unix_error(errno, "clock_gettime", Nothing);
        }

        res = caml_alloc(2, 0);
        Store_field(res, 0, caml_copy_int64(ts.tv_sec));
        Store_field(res, 1, caml_copy_int64(ts.tv_nsec));

        CAMLreturn(res);
}

CAMLprim value arakoon_clocks_getres(value clock) {
        CAMLparam1(clock);
        CAMLlocal1(res);

        struct timespec ts;
        int rc = 0;

        rc = clock_getres(Int_val(clock), &ts);

        if(rc != 0) {
                unix_error(errno, "clock_getres", Nothing);
        }

        res = caml_alloc(2, 0);
        Store_field(res, 0, caml_copy_int64(ts.tv_sec));
        Store_field(res, 1, caml_copy_int64(ts.tv_nsec));

        CAMLreturn(res);
}
