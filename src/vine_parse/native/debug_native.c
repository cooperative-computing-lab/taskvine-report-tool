#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdint.h>
#include <string.h>

#define BIT(n) ((uint32_t)1U << (n))

static const char *handler_names[] = {
    "busy_on", "puturl", "cache_update", "task_state_change", "unlink",
    "worker_removed", "complete", "worker_received", "receive_worker_info",
    "cache_invalid", "worker_resources", "stdout", "recovery_task", "put_file",
    "failed_to_send_task", "transfer_port", "exhausted_resources", "listening",
    "worker_connected", "manager_end", "remove_instances", "kill_task",
    "added_dependency", "checkpoint", "simulated_loss", "task_lost_worker", "fork_info",
    "send_task_to_worker", "worker_rejected"
};

#define TOKEN_PUTURL       BIT(0)
#define TOKEN_CACHE_UPDATE BIT(1)
#define TOKEN_UNLINK       BIT(2)
#define TOKEN_WORKER       BIT(3)
#define TOKEN_REMOVED      BIT(4)
#define TOKEN_COMPLETE     BIT(5)
#define TOKEN_RECEIVED     BIT(6)
#define TOKEN_CACHE_INVALID BIT(7)
#define TOKEN_RESOURCES    BIT(8)
#define TOKEN_STDOUT       BIT(9)
#define TOKEN_PUT          BIT(10)
#define TOKEN_TRANSFER_PORT BIT(11)
#define TOKEN_EXHAUSTED    BIT(12)
#define TOKEN_CONNECTED    BIT(13)

static int token_equals(const char *token, Py_ssize_t length, const char *value, size_t value_len)
{
    return length == (Py_ssize_t)value_len && memcmp(token, value, value_len) == 0;
}

#define TOKEN_IS(value) token_equals(start, length, value, sizeof(value) - 1)

static uint32_t scan_tokens(const char *text, Py_ssize_t text_len)
{
    uint32_t flags = 0;
    Py_ssize_t position = 0;
    while (position < text_len) {
        const char *start;
        Py_ssize_t length;
        while (position < text_len && text[position] == ' ') ++position;
        start = text + position;
        while (position < text_len && text[position] != ' ') ++position;
        length = (Py_ssize_t)((text + position) - start);
        if (length == 0) continue;
        switch (start[0]) {
        case 'c':
            if (TOKEN_IS("cache-update")) flags |= TOKEN_CACHE_UPDATE;
            else if (TOKEN_IS("cache-invalid")) flags |= TOKEN_CACHE_INVALID;
            else if (TOKEN_IS("complete")) flags |= TOKEN_COMPLETE;
            else if (TOKEN_IS("connected")) flags |= TOKEN_CONNECTED;
            break;
        case 'e': if (TOKEN_IS("exhausted")) flags |= TOKEN_EXHAUSTED; break;
        case 'p':
            if (TOKEN_IS("put")) flags |= TOKEN_PUT;
            else if (TOKEN_IS("puturl") || TOKEN_IS("puturl_now")) flags |= TOKEN_PUTURL;
            break;
        case 'r':
            if (TOKEN_IS("removed")) flags |= TOKEN_REMOVED;
            else if (TOKEN_IS("received")) flags |= TOKEN_RECEIVED;
            else if (TOKEN_IS("resources")) flags |= TOKEN_RESOURCES;
            break;
        case 's': if (TOKEN_IS("stdout")) flags |= TOKEN_STDOUT; break;
        case 't': if (TOKEN_IS("transfer-port")) flags |= TOKEN_TRANSFER_PORT; break;
        case 'u': if (TOKEN_IS("unlink")) flags |= TOKEN_UNLINK; break;
        case 'w': if (TOKEN_IS("worker")) flags |= TOKEN_WORKER; break;
        default: break;
        }
    }
    return flags;
}

#define PHRASE_BUSY_ON          BIT(0)
#define PHRASE_STATE_CHANGE     BIT(1)
#define PHRASE_INFO             BIT(2)
#define PHRASE_RECOVERY_TASK    BIT(3)
#define PHRASE_FAILED_SEND      BIT(4)
#define PHRASE_LISTENING        BIT(5)
#define PHRASE_MANAGER_END      BIT(6)
#define PHRASE_REMOVE_INSTANCES BIT(7)
#define PHRASE_KILL             BIT(8)
#define PHRASE_DEPENDENCY       BIT(9)
#define PHRASE_TX_TO            BIT(10)
#define PHRASE_CHECKPOINT       BIT(11)
#define PHRASE_SIMULATED_LOSS   BIT(12)
#define PHRASE_FORK_INFO        BIT(13)
#define PHRASE_LOST_WORKER      BIT(14)
#define PHRASE_WORKER_REJECTED  BIT(15)

#define MATCH_AT(value) \
    (i + (Py_ssize_t)(sizeof(value) - 1) <= text_len && \
     memcmp(text + i, value, sizeof(value) - 1) == 0)

static uint32_t scan_phrases(const char *text, Py_ssize_t text_len)
{
    uint32_t flags = 0;
    Py_ssize_t i;
    for (i = 0; i < text_len; ++i) {
        switch (text[i]) {
        case ' ':
            if (!(flags & PHRASE_INFO) && MATCH_AT(" info ")) flags |= PHRASE_INFO;
            if (!(flags & PHRASE_KILL) && MATCH_AT(" kill ")) flags |= PHRASE_KILL;
            if (!(flags & PHRASE_SIMULATED_LOSS) && MATCH_AT(" lost its state")) flags |= PHRASE_SIMULATED_LOSS;
            if (!(flags & PHRASE_LOST_WORKER) && MATCH_AT(" lost worker ")) flags |= PHRASE_LOST_WORKER;
            break;
        case ')':
            if (!(flags & PHRASE_SIMULATED_LOSS) && (MATCH_AT("): lose ") || MATCH_AT("): lost "))) flags |= PHRASE_SIMULATED_LOSS;
            if (!(flags & PHRASE_FORK_INFO) && MATCH_AT("): connect-back ")) flags |= PHRASE_FORK_INFO;
            break;
        case 'L': if (!(flags & PHRASE_SIMULATED_LOSS) && MATCH_AT("Losing the state of worker")) flags |= PHRASE_SIMULATED_LOSS; break;
        case 'd': if (!(flags & PHRASE_FORK_INFO) && MATCH_AT("dropped a stale message")) flags |= PHRASE_FORK_INFO; break;
        case 'r': if (!(flags & PHRASE_FORK_INFO) && MATCH_AT("round summary:")) flags |= PHRASE_FORK_INFO; break;
        case 'F': if (!(flags & PHRASE_FAILED_SEND) && MATCH_AT("Failed to send task")) flags |= PHRASE_FAILED_SEND; break;
        case 'R': if (!(flags & PHRASE_REMOVE_INSTANCES) && MATCH_AT("Removing instances of worker")) flags |= PHRASE_REMOVE_INSTANCES; break;
        case 'S': if (!(flags & PHRASE_RECOVERY_TASK) && MATCH_AT("Submitted recovery task")) flags |= PHRASE_RECOVERY_TASK; break;
        case 'a': if (!(flags & PHRASE_DEPENDENCY) && MATCH_AT("added dependency")) flags |= PHRASE_DEPENDENCY; break;
        case 'b': if (!(flags & PHRASE_BUSY_ON) && MATCH_AT("busy on")) flags |= PHRASE_BUSY_ON; break;
        case 'l': if (!(flags & PHRASE_LISTENING) && MATCH_AT("listening on port")) flags |= PHRASE_LISTENING; break;
        case 'o': if (!(flags & PHRASE_WORKER_REJECTED) && MATCH_AT("opened its probe link from")) flags |= PHRASE_WORKER_REJECTED; break;
        case 'p': if (!(flags & PHRASE_WORKER_REJECTED) && MATCH_AT("presented the wrong password")) flags |= PHRASE_WORKER_REJECTED; break;
        case 'm': if (!(flags & PHRASE_MANAGER_END) && MATCH_AT("manager end")) flags |= PHRASE_MANAGER_END; break;
        case 's': if (!(flags & PHRASE_STATE_CHANGE) && MATCH_AT("state change:")) flags |= PHRASE_STATE_CHANGE; break;
        case 't': if (!(flags & PHRASE_TX_TO) && MATCH_AT("tx to")) flags |= PHRASE_TX_TO; break;
        case 'v':
            if (!(flags & PHRASE_CHECKPOINT) && MATCH_AT("vine: checkpoint ")) flags |= PHRASE_CHECKPOINT;
            if (!(flags & PHRASE_FORK_INFO) && MATCH_AT("vault hit: ")) flags |= PHRASE_FORK_INFO;
            break;
        default: break;
        }
    }
    return flags;
}

static uint32_t classify(const char *line, Py_ssize_t length)
{
    uint32_t mask = 0;
    uint32_t tokens = scan_tokens(line, length);
    uint32_t phrases = scan_phrases(line, length);
    int worker = tokens & TOKEN_WORKER;
    int resources = tokens & TOKEN_RESOURCES;

    if (phrases & PHRASE_BUSY_ON) mask |= BIT(0);
    if (tokens & TOKEN_PUTURL) mask |= BIT(1);
    if (tokens & TOKEN_CACHE_UPDATE) mask |= BIT(2);
    if (phrases & PHRASE_STATE_CHANGE) mask |= BIT(3);
    if (tokens & TOKEN_UNLINK) mask |= BIT(4);
    if (worker && (tokens & TOKEN_REMOVED)) mask |= BIT(5);
    if (tokens & TOKEN_COMPLETE) mask |= BIT(6);
    if (tokens & TOKEN_RECEIVED) mask |= BIT(7);
    if (phrases & PHRASE_INFO) mask |= BIT(8);
    if (tokens & TOKEN_CACHE_INVALID) mask |= BIT(9);
    if (resources) mask |= BIT(10);
    if (tokens & TOKEN_STDOUT) mask |= BIT(11);
    if (phrases & PHRASE_RECOVERY_TASK) mask |= BIT(12);
    if (tokens & TOKEN_PUT) mask |= BIT(13);
    if (phrases & PHRASE_FAILED_SEND) mask |= BIT(14);
    if (tokens & TOKEN_TRANSFER_PORT) mask |= BIT(15);
    if (resources && (tokens & TOKEN_EXHAUSTED)) mask |= BIT(16);
    if (phrases & PHRASE_LISTENING) mask |= BIT(17);
    if (worker && (tokens & TOKEN_CONNECTED)) mask |= BIT(18);
    if (phrases & PHRASE_MANAGER_END) mask |= BIT(19);
    if (phrases & PHRASE_REMOVE_INSTANCES) mask |= BIT(20);
    if (phrases & PHRASE_KILL) mask |= BIT(21);
    if (phrases & PHRASE_DEPENDENCY) mask |= BIT(22);
    if (phrases & PHRASE_CHECKPOINT) mask |= BIT(23);
    if (phrases & PHRASE_SIMULATED_LOSS) mask |= BIT(24);
    if (phrases & PHRASE_LOST_WORKER) mask |= BIT(25);
    if (phrases & PHRASE_FORK_INFO) mask |= BIT(26);
    if (phrases & PHRASE_TX_TO) mask |= BIT(27);
    if (phrases & PHRASE_WORKER_REJECTED) mask |= BIT(28);
    return mask;
}

static PyObject *classify_batch_packed(PyObject *self, PyObject *argument)
{
    PyObject *sequence = PySequence_Fast(argument, "lines must be a sequence");
    PyObject *result;
    char *masks;
    Py_ssize_t i, count;
    (void)self;
    if (sequence == NULL) return NULL;
    count = PySequence_Fast_GET_SIZE(sequence);
    result = PyBytes_FromStringAndSize(NULL, count * (Py_ssize_t)sizeof(uint32_t));
    if (result == NULL) {
        Py_DECREF(sequence);
        return NULL;
    }
    masks = PyBytes_AS_STRING(result);
    for (i = 0; i < count; ++i) {
        PyObject *item = PySequence_Fast_GET_ITEM(sequence, i);
        Py_ssize_t length;
        const char *line;
        if (!PyUnicode_Check(item)) {
            PyErr_SetString(PyExc_TypeError, "every line must be str");
            Py_DECREF(result);
            Py_DECREF(sequence);
            return NULL;
        }
        line = PyUnicode_AsUTF8AndSize(item, &length);
        if (line == NULL) {
            Py_DECREF(result);
            Py_DECREF(sequence);
            return NULL;
        }
        {
            uint32_t mask = classify(line, length);
            memcpy(masks + i * (Py_ssize_t)sizeof(uint32_t), &mask, sizeof(mask));
        }
    }
    Py_DECREF(sequence);
    return result;
}

static PyMethodDef methods[] = {
    {"classify_batch_packed", classify_batch_packed, METH_O, "Classify lines into packed native-endian uint32 masks."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_debug_native",
    .m_doc = "Native stateless TaskVine debug candidate classifier.",
    .m_size = -1,
    .m_methods = methods,
};

PyMODINIT_FUNC PyInit__debug_native(void)
{
    PyObject *result = PyModule_Create(&module);
    PyObject *names;
    Py_ssize_t i;
    if (result == NULL) return NULL;
    names = PyTuple_New((Py_ssize_t)(sizeof(handler_names) / sizeof(handler_names[0])));
    if (names == NULL) {
        Py_DECREF(result);
        return NULL;
    }
    for (i = 0; i < PyTuple_GET_SIZE(names); ++i) {
        PyObject *name = PyUnicode_FromString(handler_names[i]);
        if (name == NULL) {
            Py_DECREF(names);
            Py_DECREF(result);
            return NULL;
        }
        PyTuple_SET_ITEM(names, i, name);
    }
    if (PyModule_AddObject(result, "HANDLER_NAMES", names) < 0 ||
        PyModule_AddIntConstant(result, "RULESET_VERSION", 3) < 0) {
        Py_DECREF(result);
        return NULL;
    }
    return result;
}
