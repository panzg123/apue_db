/*
 * db.c
 *
 *  Created on: 2015年9月11日
 *      Author: panzg
 */

#include"db.h"
#include"head.h"

#include<fcntl.h>
#include<stdarg.h>
#include<errno.h>
#include<sys/uio.h>

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


typedef unsigned long BDHASH;    /*hash value*/
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
	BDHASH nhash;
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

/*
 * internal functions
 */
static DB *_db_alloc(int);
static void _db_dodelete(DB*);
static int _db_find_and_lock(DB*,const char*,int);
static int _db_findfree(DB*,int,int);
static void _db_free(DB*);
static BDHASH  _db_hash(DB*,const char*);
static char* _db_readdat(DB*);
static off_t *_db_readidx(DB*,off_t);
static off_t _db_readptr(DB*,off_t);
static void _db_writedat(DB*,const char*,off_t,int);
static void _db_writeidx(DB*,const char*,off_t,int,off_t);
static void    _db_writeptr(DB *, off_t, off_t);


/*
 * open or create a database
 */
DBHANDLE db_open(const char* pathname,int oflags,...)
{
	DB *db;
	int len,mode;
	size_t i;
	char asciiptr[PTR_SZ+1],
		hash[(NHASH_DEF+1)*PTR_SZ+2];
	struct stat statbuf;

	len = strlen(pathname);
	if((db = _db_alloc(len))==NULL)
		err_dump("db_open: __db_alloc error for DB");

	//初始化db结构体
	db->nhash = NHASH_DEF;
	db->hashoff = HASH_OFF;
	strcpy(db->name,pathname);
	strcat(db->name,".idx");

	//创建数据库文件
	if(oflags & O_CREAT)
	{
		va_list ap;
		va_start(ap,oflags);
		mode = va_arg(ap,int);
		va_end(ap);

		/*打开索引文件和数据文件*/
		db->idxfd = open(db->name,oflags,mode);
		strcpy(db->name+len,".dat");
		db->datfd = open(db->name,oflags,mode);
	}
	else
	{
		/*打开索引文件和数据文件*/
				db->idxfd = open(db->name,oflags);
				strcpy(db->name+len,".dat");
				db->datfd = open(db->name,oflags);
	}
	/*打开出错*/
	if(db->idxfd <0 || db->datfd <0 )
	{
		_db_free(db);
		return (NULL);
	}
	if((oflags & (O_CREAT | O_TRUNC))==(O_CREAT|O_TRUNC))
	{
		/*常见数据库成功，进行设置，加锁等*/
		if(writew_lock(db->idxfd,0,SEEK_SET,0)<0)
			err_dump("db_open: writew_lock error");
		if(fstat(db->idxfd,&statbuf)<0)
			err_sys("db_open:fstat error");
		/*索引记录的长度为０，表示刚刚创建，需要进行初始化*/
		if(statbuf.st_size==0)
		{
			sprintf(asciiptr, "%*d", PTR_SZ, 0);
			hash[0] = 0;
			for (i = 0; i < NHASH_DEF + 1; i++)
				strcat(hash, asciiptr);
			strcat(hash, "\n");
			i = strlen(hash);
			if (write(db->idxfd, hash, i) != i)
				err_dump("db_open: index file init write error");
		}
		if(un_lock(db->idxfd,0,SEEK_SET,0)<0)
			err_dump("db_open: unlock error");
	}
	db_rewind(db);
	return (db);
}


static DB* _db_alloc(int namelen)
{
	DB *db;
	if((db=calloc(1,sizeof(DB)))==NULL)
		err_dump("_db_alloc:calloc error for DB");
	db->idxfd = db->datfd = -1;
	/*为名称分配空间，包括.idx , .dat, 和一个null*/
	if((db->name = malloc(namelen+5))==NULL)
		err_dump("_db_alloc:malloc error for name");
	/*为索引缓冲区和数据缓冲区分配内存*/
	if ((db->idxbuf = malloc(IDXLEN_MAX + 2)) == NULL)
		err_dump("_db_alloc: malloc error for index buffer");
	if ((db->datbuf = malloc(DATLEN_MAX + 2)) == NULL)
		err_dump("_db_alloc: malloc error for data buffer");
	return (db);
}
void db_close(DBHANDLE h)
{

}
