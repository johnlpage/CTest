/* MongoDB Load Generator for performance testing */
/* (c) John Page 2014 */

#include <sys/wait.h>
#include <sys/time.h>
#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include <stdlib.h>
#include <mongoc.h>

#define DATA_DB "ctest"
#define DATA_COLLECTION "data"
#define STATS_DB "testresults"
#define DEFAULT_URI "mongodb://10.14.0.198:27017"
#define INSERT_THREADS 500
#define QUERY_THREADS 200
#define SAMPLER_THREADS 1
#define REPORT_SLOW 2000
#define SAMPLER_DELAY 2

int run_inserter();
int run_sampler();
int run_query();

int main(int argc, char **argv) {
	int t;
	int status;

	for (t = 0; t < INSERT_THREADS; t++) {
		if (fork() == 0) {
			run_inserter();
			exit(0);
		}
	}
	for (t = 0; t < QUERY_THREADS; t++) {
			if (fork() == 0) {
				run_query();
				exit(0);
			}
		}
	for (t = 0; t < SAMPLER_THREADS; t++) {
		if (fork() == 0) {
			run_sampler();
			exit(0);
		}
	}

	/* Reap children */
	while (wait(&status) != -1) {
		printf("Child Quit\n");
	}
}

int run_query() {
	bson_t query;
	mongoc_client_t *conn;
	mongoc_collection_t *collection;
	bson_error_t error;
	struct timeval before_time;
	struct timeval after_time;
	mongoc_cursor_t *cursor;

	const bson_t *doc;
	char *str;

	mongoc_init();

	conn = mongoc_client_new(DEFAULT_URI);

	if (!conn) {

		fprintf(stderr, "Failed to parse URI.\n");
		exit(1);
	}

	collection = mongoc_client_get_collection(conn, DATA_DB,
	DATA_COLLECTION);

	bson_t *index1;

	index1 = BCON_NEW("sensor", BCON_INT32(1));

	mongoc_collection_create_index(collection, index1, NULL, NULL);

	bson_destroy(index1);

	while (1) {

		bson_init(&query);
		long long sensor = lrand48();

		bson_append_int64(&query, "sensor", -1, sensor);

		gettimeofday(&before_time, NULL);

		cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, 0, 0,
				&query, NULL, NULL);
		int count = 5;

		while (count-- && mongoc_cursor_more(cursor)
				&& mongoc_cursor_next(cursor, &doc)) {
			str = bson_as_json(doc, NULL);
			//printf ("%s\n", str);
			bson_free(str);
		}

		if (mongoc_cursor_error(cursor, &error)) {
			fprintf(stderr, "An error occurred: %s\n", error.message);
			exit(0);
		}

		mongoc_cursor_destroy(cursor);

		gettimeofday(&after_time, NULL);
		unsigned long before_millis;
		unsigned long after_millis;
		unsigned long taken;

		before_millis = (before_time.tv_sec * 1000)
				+ (before_time.tv_usec / 1000);
		after_millis = (after_time.tv_sec * 1000) + (after_time.tv_usec / 1000);

		taken = after_millis - before_millis;

		if (taken > REPORT_SLOW) {
			printf("Query took %lu millieconds\n", taken);
		}

		bson_destroy(&query);
	}

	mongoc_collection_destroy(collection);

}

int run_inserter() {
	bson_t record;
	mongoc_client_t *conn;
	mongoc_collection_t *collection;
	bson_error_t error;
	struct timeval before_time;
	struct timeval after_time;

	mongoc_init();

	conn = mongoc_client_new(DEFAULT_URI);

	if (!conn) {

		fprintf(stderr, "Failed to parse URI.\n");
		exit(1);
	}

	collection = mongoc_client_get_collection(conn, DATA_DB,
	DATA_COLLECTION);

	while (1) {

		bson_init(&record);
		long long sensor = lrand48();
		long value = lrand48() % 65535;

		static bson_oid_t oid;

		bson_oid_init(&oid, NULL);

		bson_append_oid(&record, "_id", -1, &oid);
		bson_append_int64(&record, "sensor", -1, sensor);
		bson_append_int32(&record, "value", -1, value);

		gettimeofday(&before_time, NULL);
		int rval = mongoc_collection_insert(collection, MONGOC_INSERT_NONE,
				&record, NULL, &error);
		gettimeofday(&after_time, NULL);
		unsigned long before_millis;
		unsigned long after_millis;
		unsigned long taken;

		before_millis = (before_time.tv_sec * 1000)
				+ (before_time.tv_usec / 1000);
		after_millis = (after_time.tv_sec * 1000) + (after_time.tv_usec / 1000);

		taken = after_millis - before_millis;

		if (taken > REPORT_SLOW) {
			printf("Append took %lu millieconds\n", taken);
		}
		if (!rval) {
			printf("Error : %s\n", error.message);
			mongoc_collection_destroy(collection);
			exit(1);
		}
		bson_destroy(&record);
	}

	mongoc_collection_destroy(collection);

}

int run_sampler() {
	bson_t record;
	mongoc_client_t *conn;
	mongoc_collection_t *collection;
	bson_error_t error;
	int nrecs;
	bson_t query;

	mongoc_init();

	while (1) {
		conn = mongoc_client_new(DEFAULT_URI);
		if (!conn) {

			fprintf(stderr, "Failed to parse URI.\n");
			exit(1);
		}
		collection = mongoc_client_get_collection(conn, DATA_DB,
		DATA_COLLECTION);
		bson_init(&query);
		nrecs = mongoc_collection_count(collection, MONGOC_QUERY_NONE, &query,
				0, 0, NULL, &error);
		if (nrecs < 0) {
			printf("Error : %s\n", error.message);
			mongoc_collection_destroy(collection);
			exit(1);

		}
		printf("%d Records in collection\n", nrecs);
		mongoc_collection_destroy(collection);
		bson_destroy(&query);
		mongoc_client_destroy(conn);
		sleep(SAMPLER_DELAY);
	}

}
