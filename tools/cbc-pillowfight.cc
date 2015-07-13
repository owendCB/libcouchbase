/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011-2012 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include "config.h"
#include <sys/types.h>
#include <libcouchbase/couchbase.h>
#include <errno.h>
#include <iostream>
#include <map>
#include <sstream>
#include <queue>
#include <list>
#include <cstring>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <signal.h>
#ifndef WIN32
#include <pthread.h>
#else
#define usleep(n) Sleep(n/1000)
#endif
#include <cstdarg>
#include "common/options.h"
#include "common/histogram.h"
#include <libcouchbase/views.h>

static const char alpha[] =
"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
"abcdefghijklmnopqrstuvwxyz";

static const char num[] =
"123456789";

int alphaLength = sizeof(alpha) - 1;
int numLength = sizeof(num) - 1;


using namespace std;
using namespace cbc;
using namespace cliopts;
using std::vector;
using std::string;



char getRandomChar() {
    return alpha[rand()% alphaLength];
}

string getRandomString(const int stringLength) {
    string str;
    for (int ii = 0; ii < stringLength; ++ii) {
        str += getRandomChar();
    }
    return str;
}

char getRandomNum() {
    return num[rand() % numLength];
}

string getRandomNumber(const int numberLength) {
    string str;
    for (int ii = 0; ii < numberLength; ++ii) {
        str += getRandomNum();
    }
    return str;
    
}

static const char* salesperson[] = {"bob", "john", "peter", "paul", "david", "helen", "sarah", "adam", "carl", "daniel", "mary", "rick", "paula", "mark", "chris", "ian", "patrick", "rob", "tom", "jane", "jason", "james", "claire", "jackson", "aiden", "liam", "lucas", "noah", "mason", "ethan", "archie", "caden", "jacob", "logan", "ellie", "sophia", "emma", "olivia", "ava", "isabella", "mia", "zoe", "lily", "emily", "madelyn"};

static const char* geography[] = {"alabama", "alaska", "arizona", "arkansas", "california", "colorado", "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky", "lousisana", "maine", "maryland", "massachusetts", "michigan", "minnesota", "mississippi", "missouri", "montana", "nebraska", "nevada", "new hampshire", "new jersey", "new mexico", "new york", "north carolina", "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina", "south dakota", "tennessee", "texas", "utah", "vermont", "virginia", "washington", "west virginia", "wisconsin", "wyoming"};

string create_view_data(uint32_t minsz, uint32_t maxsz) {
    if (minsz > maxsz) {
        minsz = maxsz;
    }
    /**
     * {"salesperson" : "10RANDOMCHAR",
     *  "sales" : 2RANDOMNUM,
     *  "product" : "10RANDOMCHAR",
     *  "geography" : "10RANDOMCHAR"
     *  "filler" : "XRANDOMCHAR"
     * }
     */
    stringstream ss;
    ss << "{\"salesperson\" : \"" << salesperson[rand() % sizeof(salesperson)/sizeof(char*)] << "\", ";
    ss << "\"cost\" : " << getRandomNumber(3) << ", ";
    ss << "\"product\" : \"" << getRandomString(17) << "\", ";
    ss << "\"geography\" : \"" << geography[rand() % sizeof(geography)/sizeof(char*)] << "\", ";
    ss << "\"filler\" : \"";

    size_t size = ss.str().size();
    ss  << getRandomString(1024-(size+2)) << "\"}";
   //std::cout << ss.str() << endl << endl;
    return ss.str();
}


struct DeprecatedOptions {
    UIntOption iterations;
    UIntOption instances;
    BoolOption loop;

    DeprecatedOptions() :
        iterations("iterations"), instances("num-instances"), loop("loop")
    {
        iterations.abbrev('i').hide().setDefault(1000);
        instances.abbrev('Q').hide().setDefault(1);
        loop.abbrev('l').hide().setDefault(false);
    }

    void addOptions(Parser &p) {
        p.addOption(instances);
        p.addOption(loop);
        p.addOption(iterations);
    }
};

class Configuration
{
public:
    Configuration() :
        o_multiSize("batch-size"),
        o_numItems("num-items"),
        o_numTokens("num-tokens"),
        o_numQueries("num-queries"),
        o_keyPrefix("key-prefix"),
        o_numThreads("num-threads"),
        o_randSeed("random-seed"),
        o_setPercent("set-pct"),
        o_minSize("min-size"),
        o_maxSize("max-size"),
        o_noPopulate("no-population"),
        o_pauseAtEnd("pause-at-end"),
        o_numCycles("num-cycles"),
        o_sequential("sequential"),
        o_startAt("start-at"),
        o_rateLimit("rate-limit")
    {
        o_multiSize.setDefault(100).abbrev('B').description("Number of operations to batch");
        o_numItems.setDefault(1000).abbrev('I').description("Number of items to operate on");
        o_numTokens.setDefault(0).abbrev('O').description("Number of ops in flight at once");
        o_numQueries.setDefault(0).abbrev('Q').description("Number of queries per second");
        o_keyPrefix.abbrev('p').description("key prefix to use");
        o_numThreads.setDefault(1).abbrev('t').description("The number of threads to use");
        o_randSeed.setDefault(0).abbrev('s').description("Specify random seed").hide();
        o_setPercent.setDefault(33).abbrev('r').description("The percentage of operations which should be mutations");
        o_minSize.setDefault(50).abbrev('m').description("Set minimum payload size");
        o_maxSize.setDefault(5120).abbrev('M').description("Set maximum payload size");
        o_noPopulate.setDefault(false).abbrev('n').description("Skip population");
        o_pauseAtEnd.setDefault(false).abbrev('E').description("Pause at end of run (holding connections open) until user input");
        o_numCycles.setDefault(-1).abbrev('c').description("Number of cycles to be run until exiting. Set to -1 to loop infinitely");
        o_sequential.setDefault(false).description("Use sequential access (instead of random)");
        o_startAt.setDefault(0).description("For sequential access, set the first item");
        o_rateLimit.setDefault(0).description("Set operations per second limit (per thread)");
    }

    void processOptions() {
        opsPerCycle = o_multiSize.result();
        prefix = o_keyPrefix.result();
        setprc = o_setPercent.result();
        shouldPopulate = !o_noPopulate.result();
        setPayloadSizes(o_minSize.result(), o_maxSize.result());

        if (depr.loop.passed()) {
            fprintf(stderr, "The --loop/-l option is deprecated. Use --num-cycles\n");
            maxCycles = -1;
        } else {
            maxCycles = o_numCycles.result();
        }

        if (depr.iterations.passed()) {
            fprintf(stderr, "The --num-iterations/-I option is deprecated. Use --batch-size\n");
            opsPerCycle = depr.iterations.result();
        }
    }

    void addOptions(Parser& parser) {
        parser.addOption(o_multiSize);
        parser.addOption(o_numItems);
        parser.addOption(o_numTokens);
        parser.addOption(o_numQueries);
        parser.addOption(o_keyPrefix);
        parser.addOption(o_numThreads);
        parser.addOption(o_randSeed);
        parser.addOption(o_setPercent);
        parser.addOption(o_noPopulate);
        parser.addOption(o_minSize);
        parser.addOption(o_maxSize);
        parser.addOption(o_pauseAtEnd);
        parser.addOption(o_numCycles);
        parser.addOption(o_sequential);
        parser.addOption(o_startAt);
        parser.addOption(o_rateLimit);
        params.addToParser(parser);
        depr.addOptions(parser);
    }

    ~Configuration() {
        delete []static_cast<char *>(data);
    }

    
    void setPayloadSizes(uint32_t minsz, uint32_t maxsz) {
        if (minsz > maxsz) {
            minsz = maxsz;
        }

        minSize = minsz;
        maxSize = maxsz;

        if (data) {
            delete []static_cast<char *>(data);
        }

        data = static_cast<void *>(new char[maxSize]);
        /* fill data array with pattern */
        uint32_t *iptr = static_cast<uint32_t *>(data);
        for (uint32_t ii = 0; ii < maxSize / sizeof(uint32_t); ++ii) {
            iptr[ii] = 0xdeadbeef;
        }
        /* pad rest bytes with zeros */
        size_t rest = maxSize % sizeof(uint32_t);
        if (rest > 0) {
            char *cptr = static_cast<char *>(data) + (maxSize / sizeof(uint32_t));
            memset(cptr, 0, rest);
        }
    }

    uint32_t getNumInstances(void) {
        if (depr.instances.passed()) {
            return depr.instances.result();
        }
        return o_numThreads.result();
    }

    bool isTimings(void) { return params.useTimings(); }

    bool isLoopDone(size_t niter) {
        if (maxCycles == -1) {
            return false;
        }
        return niter >= (size_t)maxCycles;
    }

    void setDGM(bool val) {
        dgm = val;
    }

    void setWaitTime(uint32_t val) {
        waitTime = val;
    }

    uint32_t getRandomSeed() { return o_randSeed; }
    uint32_t getNumThreads() { return o_numThreads; }
    string& getKeyPrefix() { return prefix; }
    bool shouldPauseAtEnd() { return o_pauseAtEnd; }
    bool sequentialAccess() { return o_sequential; }
    unsigned firstKeyOffset() { return o_startAt; }
    uint32_t getNumItems() { return o_numItems; }
    uint32_t getNumTokens() { return o_numTokens; }
    uint32_t getRateLimit() { return o_rateLimit; }
    uint32_t getMinSize() { return o_minSize.result();}
    uint32_t getMaxSize() { return o_maxSize.result();}
    uint32_t getNumQueries() {return o_numQueries;}

    void *data;

    uint32_t opsPerCycle;
    unsigned setprc;
    string prefix;
    uint32_t maxSize;
    uint32_t minSize;
    volatile int maxCycles;
    bool dgm;
    bool shouldPopulate;
    uint32_t waitTime;
    ConnParams params;

private:
    UIntOption o_multiSize;
    UIntOption o_numItems;
    UIntOption o_numTokens;
    UIntOption o_numQueries;
    StringOption o_keyPrefix;
    UIntOption o_numThreads;
    UIntOption o_randSeed;
    UIntOption o_setPercent;
    UIntOption o_minSize;
    UIntOption o_maxSize;
    BoolOption o_noPopulate;
    BoolOption o_pauseAtEnd; // Should pillowfight pause execution (with
                             // connections open) before exiting?
    IntOption o_numCycles;
    BoolOption o_sequential;
    UIntOption o_startAt;
    UIntOption o_rateLimit;
    DeprecatedOptions depr;
} config;

void log(const char *format, ...)
{
    char buffer[512];
    va_list args;

    va_start(args, format);
    vsprintf(buffer, format, args);
    if (config.isTimings()) {
        std::cerr << "[" << std::fixed << lcb_nstime() / 1000000000.0 << "] ";
    }
    std::cerr << buffer << std::endl;
    va_end(args);
}



extern "C" {
static void operationCallback(lcb_t, int, const lcb_RESPBASE*);
static void viewCallback(lcb_t instance, int ign, const lcb_RESPVIEWQUERY *rv);
}

class InstanceCookie {
public:
    InstanceCookie(lcb_t instance) {
        lcb_set_cookie(instance, this);
        lastPrint = 0;
        if (config.isTimings()) {
            hg.install(instance, stdout);
        }
    }

    static InstanceCookie* get(lcb_t instance) {
        return (InstanceCookie *)lcb_get_cookie(instance);
    }


    static void dumpTimings(lcb_t instance, const char *header, bool force=false) {
        time_t now = time(NULL);
        InstanceCookie *ic = get(instance);

        if (now - ic->lastPrint > 0) {
            ic->lastPrint = now;
        } else if (!force) {
            return;
        }

        Histogram &h = ic->hg;
        printf("[%f %s]\n", lcb_nstime() / 1000000000.0, header);
        printf("              +---------+---------+---------+---------+\n");
        h.write();
        printf("              +----------------------------------------\n");
    }

private:
    time_t lastPrint;
    Histogram hg;
};

struct NextOp {
    NextOp() : seqno(0), valsize(0), isStore(false) {}

    string key;
    uint32_t seqno;
    size_t valsize;
    bool isStore;
};

class KeyGenerator {
public:
    KeyGenerator(int ix) :
        currSeqno(0), rnum(0), ngenerated(0), isSequential(false),
        isPopulate(config.shouldPopulate)
{
        srand(config.getRandomSeed());
        for (int ii = 0; ii < 8192; ++ii) {
            seqPool[ii] = rand();
        }
        if (isPopulate) {
            isSequential = true;
        } else {
            isSequential = config.sequentialAccess();
        }


        // Maximum number of keys for this thread
        maxKey = config.getNumItems() /  config.getNumThreads();

        offset = config.firstKeyOffset();
        offset += maxKey * ix;
        id = ix;
    }

    void setNextOp(NextOp& op) {
        bool store_override = false;

        if (isPopulate) {
            if (++ngenerated < maxKey) {
                store_override = true;
            } else {
                printf("Thread %d has finished populating.\n", id);
                isPopulate = false;
                isSequential = config.sequentialAccess();
            }
        }

        if (isSequential) {
            rnum++;
            rnum %= maxKey;
        } else {
            rnum += seqPool[currSeqno];
            currSeqno++;
            if (currSeqno > 8191) {
                currSeqno = 0;
            }
        }

        op.seqno = rnum;

        if (store_override) {
            op.isStore = true;
        } else {
            op.isStore = shouldStore(op.seqno);
        }

        if (op.isStore) {
            size_t size;
            if (config.minSize == config.maxSize) {
                size = config.minSize;
            } else {
                size = config.minSize + op.seqno % (config.maxSize - config.minSize);
            }
            op.valsize = size;
        }
        generateKey(op);
    }

    bool shouldStore(uint32_t seqno) {
        if (config.setprc == 0) {
            return false;
        }

        float seqno_f = seqno % 100;
        float pct_f = seqno_f / config.setprc;
        return pct_f < 1;
    }

    void generateKey(NextOp& op) {
        uint32_t seqno = op.seqno;
        seqno %= maxKey;
        seqno += offset-1;

        char buffer[21];
        snprintf(buffer, sizeof(buffer), "%020d", seqno);
        op.key.assign(config.getKeyPrefix() + buffer);
    }
    const char *getStageString() const {
        if (isPopulate) {
            return "Populate";
        } else {
            return "Run";
        }
    }

private:
    uint32_t seqPool[8192];
    uint32_t currSeqno;
    uint32_t rnum;
    uint32_t offset;
    uint32_t maxKey;
    size_t ngenerated;
    int id;

    bool isSequential;
    bool isPopulate;
};

class ThreadContext
{
public:
    ThreadContext(lcb_t handle, int ix) : kgen(ix), niter(0), instance(handle), tokens(0), opCount(0) {

    }

    void singleLoop() {
        bool hasItems = false;
        lcb_sched_enter(instance);
        NextOp opinfo;

        for (size_t ii = 0; ii < config.opsPerCycle; ++ii) {
            kgen.setNextOp(opinfo);
            if (opinfo.isStore) {
                lcb_CMDSTORE scmd = { 0 };
                scmd.operation = LCB_SET;
                LCB_CMD_SET_KEY(&scmd, opinfo.key.c_str(), opinfo.key.size());
                if (config.getNumQueries() > 0) {
                    std::string s = create_view_data(config.getMinSize(), config.getMaxSize());
                    LCB_CMD_SET_VALUE(&scmd, s.c_str(), s.size());
                } else {
                    LCB_CMD_SET_VALUE(&scmd, config.data, opinfo.valsize);
                }
                error = lcb_store3(instance, this, &scmd);

            } else {
                lcb_CMDGET gcmd = { 0 };
                LCB_CMD_SET_KEY(&gcmd, opinfo.key.c_str(), opinfo.key.size());
                error = lcb_get3(instance, this, &gcmd);
            }
            if (error != LCB_SUCCESS) {
                hasItems = false;
                log("Failed to schedule operation: [0x%x] %s", error, lcb_strerror(instance, error));
            } else {
                hasItems = true;
            }
        }
        if (hasItems) {
            lcb_sched_leave(instance);
            lcb_wait(instance);
            if (error != LCB_SUCCESS) {
                log("Operation(s) failed: [0x%x] %s", error, lcb_strerror(instance, error));
            }
        } else {
            lcb_sched_fail(instance);
        }
    }
    
    bool scheduleNextOperation() {
        NextOp opinfo;
        kgen.setNextOp(opinfo);
        if (opinfo.isStore)
        {
            lcb_CMDSTORE scmd = { 0 };
            scmd.operation = LCB_SET;
            LCB_CMD_SET_KEY(&scmd, opinfo.key.c_str(), opinfo.key.size());
            if (config.getNumQueries() > 0) {
                std::string s = create_view_data(config.getMinSize(), config.getMaxSize());
                LCB_CMD_SET_VALUE(&scmd, s.c_str(), s.size());
            } else {
                LCB_CMD_SET_VALUE(&scmd, config.data, opinfo.valsize);
            }
            error = lcb_store3(instance, this, &scmd);
        }
        else
        {
            lcb_CMDGET gcmd = { 0 };
            LCB_CMD_SET_KEY(&gcmd, opinfo.key.c_str(), opinfo.key.size());
            error = lcb_get3(instance, this, &gcmd);
        }
        if (error != LCB_SUCCESS) {
            log("Failed to schedule operation: [0x%x] %s", error, lcb_strerror(instance, error));
            return false;
        } else {
            return true;
        }
    }

    
    void spoolOperations() {
        static lcb_U64 sleep_nsec = 0;
        lcb_sched_enter(instance);
        while (tokens > 0) {
            if (scheduleNextOperation()) {
                tokens--;
                usleep(sleep_nsec / 1000);
            }
        }
        lcb_sched_leave(instance);

        if (config.getRateLimit() > 0) {
            lcb_U64 now = lcb_nstime();
            static lcb_U64 previous_time = now;
            static lcb_U64 last_sleep_ns = 0;
            lcb_U64 elapsed_ns = 0;
            if ((uint32_t)opCount > config.getNumTokens() && (now != previous_time)) {
                elapsed_ns = (now - previous_time) / opCount;
                const lcb_U64 wanted_duration_ns = 1e9 / config.getRateLimit();
                if (elapsed_ns < wanted_duration_ns) {
                    sleep_nsec = last_sleep_ns + (wanted_duration_ns - elapsed_ns)/2;
                } else {
                    sleep_nsec = wanted_duration_ns;
                }
                last_sleep_ns = sleep_nsec;
                opCount = 0;
                previous_time = now;
            }
        }
    }

    bool run() {
        if (config.getNumTokens() > 0) {
            tokens = config.getNumTokens();
            spoolOperations();
            lcb_wait(instance);
        } else
        {
            do {
                singleLoop();

                if (config.isTimings()) {
                    InstanceCookie::dumpTimings(instance, kgen.getStageString());
                }
                if (config.params.shouldDump()) {
                    lcb_dump(instance, stderr, LCB_DUMP_ALL);
                }
                if (config.getRateLimit() > 0) {
                    rateLimitThrottle();
                }

            } while (!config.isLoopDone(++niter));

            if (config.isTimings()) {
                InstanceCookie::dumpTimings(instance, kgen.getStageString(), true);
            }
        }
        return true;
    }
    
    bool run_query() {
        return true;
    }


#ifndef WIN32
    pthread_t thr;
#endif

protected:
    // the callback methods needs to be able to set the error handler..
    friend void operationCallback(lcb_t, int, const lcb_RESPBASE*);
    friend void viewCallback(lcb_t instance, int ign, const lcb_RESPVIEWQUERY *rv);
    Histogram histogram;

    void setError(lcb_error_t e) { error = e; }

private:

    void rateLimitThrottle() {
        lcb_U64 now = lcb_nstime();
        static lcb_U64 previous_time = now;

        const lcb_U64 elapsed_ns = now - previous_time;
        const lcb_U64 wanted_duration_ns =
                config.opsPerCycle * 1e9 / config.getRateLimit();
        // On first invocation no previous_time, so skip attempting to sleep.
        if (elapsed_ns > 0 && elapsed_ns < wanted_duration_ns) {
            // Dampen the sleep time by averaging with the previous
            // sleep time.
            static lcb_U64 last_sleep_ns = 0;
            const lcb_U64 sleep_ns =
                    (last_sleep_ns + wanted_duration_ns - elapsed_ns) / 2;
            usleep(sleep_ns / 1000);
            now += sleep_ns;
            last_sleep_ns = sleep_ns;
        }
        previous_time = now;
    }

    KeyGenerator kgen;
    size_t niter;
    lcb_error_t error;
    lcb_t instance;
    int tokens;
    int opCount;
};

static void operationCallback(lcb_t, int, const lcb_RESPBASE *resp)
{
    ThreadContext *tc;

    tc = const_cast<ThreadContext *>(reinterpret_cast<const ThreadContext *>(resp->cookie));
    tc->setError(resp->rc);
    tc->tokens++;
    tc->opCount++;
#ifndef WIN32
    static volatile unsigned long nops = 1;
    static time_t start_time = time(NULL);
    static int is_tty = isatty(STDOUT_FILENO);
    if (is_tty) {
        if (++nops % 1000 == 0) {
            time_t now = time(NULL);
            time_t nsecs = now - start_time;
            if (!nsecs) { nsecs = 1; }
            unsigned long ops_sec = nops / nsecs;
            printf("OPS/SEC: %10lu\r", ops_sec);
            fflush(stdout);
        }
    }
#endif
    if (tc->tokens > 0) {
        tc->spoolOperations();
    }
}

static int cbCounter = 0;

static void viewCallback(lcb_t, int, const lcb_RESPVIEWQUERY *rv)
{
    if (rv->rflags & LCB_RESP_F_FINAL) {
        cbCounter++;

       // printf("*** META FROM VIEWS ***\n");
       // fprintf(stderr, "%.*s\n", (int)rv->nvalue, rv->value);
        return;
    }
   /*
    printf("Got row callback from LCB: RC=0x%X, DOCID=%.*s. KEY=%.*s VALUE=%.*s\n",
           rv->rc, (int)rv->ndocid, rv->docid, (int)rv->nkey, rv->key, (int)rv->nvalue, rv->value);
    
    if (rv->docresp) {
        printf("   Document for response. RC=0x%X. CAS=0x%lx\n",
               rv->docresp->rc, (unsigned long)rv->docresp->cas);
    }*/
    }


std::list<ThreadContext *> contexts;

extern "C" {
    typedef void (*handler_t)(int);
}

#ifndef WIN32
static void sigint_handler(int)
{
    static int ncalled = 0;
    ncalled++;

    if (ncalled < 2) {
        log("Termination requested. Waiting threads to finish. Ctrl-C to force termination.");
        signal(SIGINT, sigint_handler); // Reinstall
        config.maxCycles = 0;
        return;
    }

    std::list<ThreadContext *>::iterator it;
    for (it = contexts.begin(); it != contexts.end(); ++it) {
        delete *it;
    }
    contexts.clear();
    exit(EXIT_FAILURE);
}

static void setup_sigint_handler()
{
    struct sigaction action;
    sigemptyset(&action.sa_mask);
    action.sa_handler = sigint_handler;
    action.sa_flags = 0;
    sigaction(SIGINT, &action, NULL);
}

extern "C" {
static void* thread_worker(void*);
}

static void start_worker(ThreadContext *ctx)
{
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    int rc = pthread_create(&ctx->thr, &attr, thread_worker, ctx);
    if (rc != 0) {
        log("Couldn't create thread: (%d)", errno);
        exit(EXIT_FAILURE);
    }
}
static void join_worker(ThreadContext *ctx)
{
    void *arg = NULL;
    int rc = pthread_join(ctx->thr, &arg);
    if (rc != 0) {
        log("Couldn't join thread (%d)", errno);
        exit(EXIT_FAILURE);
    }
}

#else
static void setup_sigint_handler() {}
static void start_worker(ThreadContext *ctx) { ctx->run(); }
static void join_worker(ThreadContext *ctx) { (void)ctx; }
#endif

extern "C" {
static void *thread_worker(void *arg)
{
    ThreadContext *ctx = static_cast<ThreadContext *>(arg);
    ctx->run();
    return NULL;
}
}

int main(int argc, char **argv)
{
    int exit_code = EXIT_SUCCESS;
    setup_sigint_handler();
    Parser parser("cbc-pillowfight");
    config.addOptions(parser);
    parser.parse(argc, argv, false);
    config.processOptions();
    size_t nthreads = config.getNumThreads();
    log("Running. Press Ctrl-C to terminate...");

#ifdef WIN32
    if (nthreads > 1) {
        log("WARNING: More than a single thread on Windows not supported. Forcing 1");
        nthreads = 1;
    }
#endif

    struct lcb_create_st options;
    ConnParams& cp = config.params;
    lcb_error_t error;
    
    
 
    for (uint32_t ii = 0; ii < nthreads; ++ii) {
        cp.fillCropts(options);
        lcb_t instance = NULL;
        error = lcb_create(&instance, &options);
        if (error != LCB_SUCCESS) {
            log("Failed to create instance: %s", lcb_strerror(NULL, error));
            exit(EXIT_FAILURE);
        }
        lcb_install_callback3(instance, LCB_CALLBACK_STORE, operationCallback);
        lcb_install_callback3(instance, LCB_CALLBACK_GET, operationCallback);
        cp.doCtls(instance);

        new InstanceCookie(instance);

        lcb_connect(instance);
        lcb_wait(instance);
        error = lcb_get_bootstrap_status(instance);

        if (error != LCB_SUCCESS) {
            std::cout << std::endl;
            log("Failed to connect: %s", lcb_strerror(instance, error));
            exit(EXIT_FAILURE);
        }
        ThreadContext *ctx = new ThreadContext(instance, ii);
        contexts.push_back(ctx);
        start_worker(ctx);
    }
    
    if (config.getNumQueries() > 0) {
        cp.fillCropts(options);
        lcb_t instance = NULL;
        error = lcb_create(&instance, &options);
        if (error != LCB_SUCCESS) {
            log("Failed to create instance: %s", lcb_strerror(NULL, error));
            exit(EXIT_FAILURE);
        }

        cp.doCtls(instance);
        new InstanceCookie(instance);
        
        lcb_connect(instance);
        lcb_wait(instance);
        error = lcb_get_bootstrap_status(instance);
        
        if (error != LCB_SUCCESS) {
            std::cout << std::endl;
            log("Failed to connect: %s", lcb_strerror(instance, error));
            exit(EXIT_FAILURE);
        }
        // Nao, set up the views..
        lcb_CMDVIEWQUERY vq = { 0 };
        std::string dName = "1";
        std::string vName = "test";
        std::string options2 = "stale=OK&group=true";
        
        vq.callback = viewCallback;
        vq.ddoc = dName.c_str();
        vq.nddoc = dName.length();
        vq.view = vName.c_str();
        vq.nview = vName.length();
        vq.optstr = options2.c_str();
        vq.noptstr = options2.size();

        vq.cmdflags = LCB_CMDVIEWQUERY_F_INCLUDE_DOCS;

        static lcb_U64 sleep_nsec = 0;
        const lcb_U64 wanted_duration_ns = 1e9/10;
        static lcb_U64 last_sleep_ns = 0;
        lcb_U64 elapsed_ns = 0;

        while (1) {
            lcb_U64 now = lcb_nstime();
             static lcb_U64 previous_time = now;
            if (now != previous_time && cbCounter > 0) {
                elapsed_ns = (now-previous_time) / cbCounter;
                //printf("elapsed time = %llu count = %d \n", elapsed_ns, cbCounter);
                if (elapsed_ns < wanted_duration_ns) {
                    //printf("extend sleep %u %u\n", elapsed_ns, wanted_duration_ns);
                    sleep_nsec = last_sleep_ns + (wanted_duration_ns - elapsed_ns)/2;
                } else {
                   // printf("REDUCE sleep %u %u\n", elapsed_ns, wanted_duration_ns);
                    sleep_nsec = last_sleep_ns *.9;
                }
                last_sleep_ns = sleep_nsec;
                previous_time = now;
            }
            //printf("sleep = %d\n", sleep_nsec);
            usleep(sleep_nsec / 1000);
            error = lcb_view_query(instance, NULL, &vq);
            assert(rc == LCB_SUCCESS);
            lcb_wait(instance);
          // printf("Total Invocations=%d\n", cbCounter);
        }
        //ThreadContext *ctx = new ThreadContext(instance, nthreads);
        //contexts.push_back(ctx);
        //start_worker(ctx);
        
    }

    for (std::list<ThreadContext *>::iterator it = contexts.begin();
            it != contexts.end(); ++it) {
        join_worker(*it);
    }
    return exit_code;
}
