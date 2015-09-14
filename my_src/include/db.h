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

/*内部索引文件常量*/
#define IDXLEN_SZ	   4	/* index record length (ASCII chars) */
#define SEP         ':'	/* separator char in index record */
#define SPACE       ' '	/* space character */
#define NEWLINE     '\n'	/* newline character */

#define PTR_SZ        6	/* size of ptr field in hash chain */
#define PTR_MAX  999999	/* max file offset = 10**PTR_SZ - 1 */
#define NHASH_DEF	 137	/* default hash table size */
#define FREE_OFF      0	/* free list offset in index file */
#define HASH_OFF PTR_SZ	/* hash table offset in index file */


typedef unsigned long DBHASH;    /*hash value*/
typedef unsigned long COUNT;    /*unsigned counter*/

/*db结构记录一个打开数据库的所有信息*/
typedef struct DB{
	int idxfd;     /*索引文件指针*/
	int datfd;    /*数据文件指针*/
	char *idxbuf;    /*索引记录缓冲区*/
	char *datbuf;      /*指针记录缓冲区*/
	char *name;
	off_t idxoff;		/*索引偏移量*/
	size_t idxlen;   /*索引长度*/

	off_t datoff;   /*数据偏移量*/
	size_t datlen; /*数据长度*/

	off_t ptrval;
	off_t ptroff;
	off_t chainoff;
	off_t hashoff;
	DBHASH nhash;
	COUNT nhash;
	 COUNT  cnt_delok;    /* delete OK */
	  COUNT  cnt_delerr;   /* delete error */
	  COUNT  cnt_fetchok;  /* fetch OK */
	  COUNT  cnt_fetcherr; /* fetch error */
	  COUNT  cnt_nextrec;  /* nextrec */
	  COUNT  cnt_stor1;    /* store: DB_INSERT, no empty, appended */
	  COUNT  cnt_stor2;    /* store: DB_INSERT, found empty, reused */
	  COUNT  cnt_stor3;    /* store: DB_REPLACE, diff len, appended */
	  COUNT  cnt_stor4;    /* store: DB_REPLACE, same len, overwrote */
	  COUNT  cnt_storerr;  /* store error */
}DB;


#endif /* MY_SRC_INCLUDE_DB_H_ */
