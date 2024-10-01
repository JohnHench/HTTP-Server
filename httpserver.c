#include "asgn2_helper_funcs.h"
#include "connection.h"
#include "debug.h"
#include "response.h"
#include "request.h"
#include "rwlock.h"
#include "queue.h"

#include <getopt.h>
#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/stat.h>

pthread_mutex_t lock;
void worker_thread();
void insert_rwlock(const char *uri, rwlock_t *rwlock);
queue_t *queue;
struct rwlockHTNodeObj;
//Start Mitchell Pseudocode
typedef struct rwlockHTNodeObj {
    rwlock_t *rwlock;
    char *uri;
    struct rwlockHTNodeObj *next;
    //rwlockHTNode *next;

} rwlockHTNodeObj;

typedef rwlockHTNodeObj *rwlockHTNode;

typedef struct rwlockHTObj {
    size_t size;
    rwlockHTNode **buckets;
} rwlockHTObj;

typedef rwlockHTObj *rwlockHT;
rwlockHT rwlock_ht;

typedef struct ThreadObj {
    pthread_t thread;
    int id; //FOR DEBUGGING
    rwlockHTObj *hashtable;
    //queue_t *queue;
} ThreadObj;

typedef ThreadObj *Thread;

rwlockHTObj *init_rwlock_ht(size_t size);

void handle_connection(int);

void handle_get(conn_t *);
void handle_put(conn_t *);
void handle_unsupported(conn_t *);

int main(int argc, char **argv) {
    if (argc < 2) {
        warnx("wrong arguments: %s port_num", argv[0]);
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        return EXIT_FAILURE;
    }
    //Need to use getopt to get the number of threads
    int opt;
    int t = 4; //Thread count number, DEFAULT = 4
    while ((opt = getopt(argc, argv, "t:")) != -1) {
        switch (opt) {
        case 't':
            t = atoi(optarg);
            if (t < 0) {
                fprintf(stderr, "Invalid number of threads: %s\n", optarg);
                return 1;
            }
        }
    }

    char *endptr = NULL;
    size_t port = (size_t) strtoull(argv[optind], &endptr, 10);
    if (endptr && *endptr != '\0') {
        warnx("invalid port number: %s", argv[optind]);
        return EXIT_FAILURE;
    }

    queue = queue_new(t);
    pthread_mutex_init(&lock, NULL);
    Thread threads[t];
    size_t hash_table_size = 100; 
    rwlockHTObj *rwHashtable = init_rwlock_ht(hash_table_size);
    pthread_t *ThreadIDs = malloc(sizeof(pthread_t) * t);
    for (int i = 0; i < t; i++) {
        threads[i] = malloc(sizeof(ThreadObj));
        threads[i]->id = 1;
        threads[i]->hashtable = rwHashtable;
        pthread_create(
            &ThreadIDs[i], NULL, (void *(*) (void *) ) worker_thread, (void *) threads[i]);
    }

    signal(SIGPIPE, SIG_IGN);
    Listener_Socket sock;
    listener_init(&sock, port);

    while (1) {
        uintptr_t connfd = listener_accept(&sock);
        queue_push(queue, (void *) connfd);
    }

    return EXIT_SUCCESS;
}

rwlockHTObj *init_rwlock_ht(size_t size) {
    rwlockHTObj *rwlock_ht = malloc(sizeof(rwlockHTObj));
    rwlock_ht->size = size;
    rwlock_ht->buckets = malloc(sizeof(rwlockHTNode) * size);

    // Initialize each bucket to NULL
    for (size_t i = 0; i < size; ++i) {
        rwlock_ht->buckets[i] = NULL;
    }
    return rwlock_ht;
}

unsigned int hash_function(const char *uri, size_t size) {
    // Simple hash function to map URIs to bucket indices
    unsigned int hash = 0;
    for (size_t i = 0; i < strlen(uri); ++i) {
        hash = hash * 31 + uri[i];
    }
    return hash % size;
}

rwlock_t *get_rwlock(const char *uri) {
    // Hash the URI to get the bucket index
    unsigned int index = hash_function(uri, rwlock_ht->size);

    // Traverse the linked list in the bucket to find the matching URI
    rwlockHTNode current = *rwlock_ht->buckets[index];
    while (current != NULL) {
        if (strcmp(current->uri, uri) == 0) {
            return current->rwlock;
            //fprintf(stderr, "found it");
        }
        current = current->next;
    }
    return NULL; // URI not found
}

void insert_rwlock(const char *uri, rwlock_t *rwlock) {
    // Hash the URI to get the bucket index
    unsigned int index = hash_function(uri, rwlock_ht->size);

    // Create a new node
    rwlockHTNodeObj *newNode = malloc(sizeof(rwlockHTNodeObj));
    newNode->uri = strdup(uri);
    newNode->rwlock = rwlock;
    newNode->next = NULL;

    // Insert the new node at the beginning of the linked list in the bucket
    newNode->next = *rwlock_ht->buckets[index];
    *rwlock_ht->buckets[index] = newNode;
}

void audit_log(conn_t *connfd, const Response_t *response) {
    if (connfd == NULL) {
        return;
    }
    if (response == NULL) {
        return;
    }
    const char *request_method = request_get_str(conn_get_request(connfd));
    const char *uri = conn_get_uri(connfd);
    int status_code = response_get_code(response);
    const char *request_id = conn_get_header(connfd, "Request-Id");
    if (request_id == NULL) {
        request_id = "0";
    }
    fprintf(stderr, "%s, %s, %d, %s\n", request_method, uri, status_code, request_id);
}

void worker_thread() {
    while (1) {
        uintptr_t connfd = 0;
        queue_pop(queue, (void **) &connfd);
        handle_connection(connfd);
        close(connfd);
    }
}

void handle_connection(int connfd) {
    conn_t *conn = conn_new(connfd);

    const Response_t *res = conn_parse(conn);

    if (res != NULL) {
        conn_send_response(conn, res);
        audit_log(conn, res);
    } else {
        //debug("%s", conn_str(conn));
        const Request_t *req = conn_get_request(conn);
        if (req == &REQUEST_GET) {
            handle_get(conn);
        } else if (req == &REQUEST_PUT) {
            handle_put(conn);
        } else {
            handle_unsupported(conn);
        }
    }

    conn_delete(&conn);
    close(connfd);
}

void handle_get(conn_t *conn) {
    //lock the file
    //wnat to use mutex lock and rwlock

    char *uri = conn_get_uri(conn);
    const Response_t *res = NULL;
    int fd = open(uri, O_RDONLY);
    if (fd < 0) {
        if (errno == EACCES) {
            res = &RESPONSE_FORBIDDEN;
            goto out;
        }
        if (errno == ENOENT) {
            res = &RESPONSE_NOT_FOUND;
            goto out;
        } else {
            res = &RESPONSE_INTERNAL_SERVER_ERROR;
            goto out;
        }
    }

    struct stat file_info;
    fstat(fd, &file_info);
    off_t file_size = file_info.st_size;

    if (S_ISDIR(file_info.st_mode)) {
        res = &RESPONSE_FORBIDDEN;
        goto out;
    }

    res = conn_send_file(conn, fd, file_size);
    if (res == NULL) {
        res = &RESPONSE_OK;
    }

out:
    close(fd);
    //conn_send_response(conn, res);
    audit_log(conn, res);
}

void handle_unsupported(conn_t *conn) {
    //debug("handling unsupported request");

    // send responses
    conn_send_response(conn, &RESPONSE_NOT_IMPLEMENTED);
    audit_log(conn, &RESPONSE_NOT_IMPLEMENTED);
}

void handle_put(conn_t *conn) {
    char *uri = conn_get_uri(conn);
    const Response_t *res = NULL;
    //debug("handling put request for %s", uri);

    // Check if file already exists before opening it.
    bool existed = access(uri, F_OK) == 0;
    //debug("%s existed? %d", uri, existed);

    // Open the file..
    int fd = open(uri, O_CREAT | O_TRUNC | O_WRONLY, 0600);
    if (fd < 0) {
        //debug("%s: %d", uri, errno);
        if (errno == EACCES || errno == EISDIR) {
            res = &RESPONSE_FORBIDDEN;
            goto out;
        } else if (errno == ENOENT) {
            res = &RESPONSE_NOT_FOUND;
            goto out;
        } else {
            res = &RESPONSE_INTERNAL_SERVER_ERROR;
            goto out;
        }
    }

    res = conn_recv_file(conn, fd);

    if (res == NULL && existed) {
        res = &RESPONSE_OK;
    } else if (res == NULL && !existed) {
        res = &RESPONSE_CREATED;
    }

out:
    close(fd);
    conn_send_response(conn, res);
    audit_log(conn, res);
}
