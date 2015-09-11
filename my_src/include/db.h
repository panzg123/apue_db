/*
 * db.h
 *
 *  Created on: 2015年9月11日
 *      Author: panzg
 */

#ifndef MY_SRC_INCLUDE_DB_H_
#define MY_SRC_INCLUDE_DB_H_

typedef void* DBHANDLE;

DBHANDLE db_open(const char*,int,...);
void db_close(DBHANDLE);
void *db_fetch(DBHANDLE,const char*);
int db_store(DBHANDLE,const char*,const char*,int);
int db_delete(DBHANDLE,const char*);
void db_rewind(DBHANDLE);
char* db_nextrec(DBHANDLE,char*);

/*
 * flags for db_store
 */
#define DB_INSERT 1
#define DB_REPLACE 2
#define DB_STORE 3

/*
 * implementation limits
 */
#define IDXLEN_MIN 6
#define IDXLEN_MAX 1024
#define DATLEN_MIN 2
#define DATLEN_MAX 2014



#endif /* MY_SRC_INCLUDE_DB_H_ */
