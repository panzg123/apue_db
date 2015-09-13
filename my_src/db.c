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

/*
 * internal functions
 */
static DB *_db_alloc(int);
static void _db_dodelete(DB*);
static int _db_find_and_lock(DB*, const char*, int);
static int _db_findfree(DB*, int, int);
static void _db_free(DB*);
static BDHASH _db_hash(DB*, const char*);
static char* _db_readdat(DB*);
static off_t *_db_readidx(DB*, off_t);
static off_t _db_readptr(DB*, off_t);
static void _db_writedat(DB*, const char*, off_t, int);
static void _db_writeidx(DB*, const char*, off_t, int, off_t);
static void _db_writeptr(DB *, off_t, off_t);

/*
 * open or create a database
 */
DBHANDLE db_open(const char* pathname, int oflags, ...)
{
	DB *db;
	int len, mode;
	size_t i;
	char asciiptr[PTR_SZ + 1], hash[(NHASH_DEF + 1) * PTR_SZ + 2];
	struct stat statbuf;

	len = strlen(pathname);
	if ((db = _db_alloc(len)) == NULL)
		err_dump("db_open: __db_alloc error for DB");

	//初始化db结构体
	db->nhash = NHASH_DEF;
	db->hashoff = HASH_OFF;
	strcpy(db->name, pathname);
	strcat(db->name, ".idx");

	//创建数据库文件
	if (oflags & O_CREAT)
	{
		va_list ap;
		va_start(ap, oflags);
		mode = va_arg(ap, int);
		va_end(ap);

		/*打开索引文件和数据文件*/
		db->idxfd = open(db->name, oflags, mode);
		strcpy(db->name + len, ".dat");
		db->datfd = open(db->name, oflags, mode);
	}
	else
	{
		/*打开索引文件和数据文件*/
		db->idxfd = open(db->name, oflags);
		strcpy(db->name + len, ".dat");
		db->datfd = open(db->name, oflags);
	}
	/*打开出错*/
	if (db->idxfd < 0 || db->datfd < 0)
	{
		_db_free(db);
		return (NULL);
	}
	if ((oflags & (O_CREAT | O_TRUNC)) == (O_CREAT | O_TRUNC))
	{
		/*常见数据库成功，进行设置，加锁等*/
		if (writew_lock(db->idxfd,0,SEEK_SET,0) < 0)
			err_dump("db_open: writew_lock error");
		if (fstat(db->idxfd, &statbuf) < 0)
			err_sys("db_open:fstat error");
		/*索引记录的长度为０，表示刚刚创建，需要进行初始化*/
		if (statbuf.st_size == 0)
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
		if (un_lock(db->idxfd,0,SEEK_SET,0) < 0)
			err_dump("db_open: unlock error");
	}
	db_rewind(db);
	return (db);
}

static DB* _db_alloc(int namelen)
{
	DB *db;
	if ((db = calloc(1, sizeof(DB))) == NULL)
		err_dump("_db_alloc:calloc error for DB");
	db->idxfd = db->datfd = -1;
	/*为名称分配空间，包括.idx , .dat, 和一个null*/
	if ((db->name = malloc(namelen + 5)) == NULL)
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
	_db_free((DB*) h); /*在free中关闭fds，并释放结构体和相应缓冲区*/
}

/*
 * 先释放缓冲区，然后删除结构体
 */
static void _db_free(DB *db)
{
	if (db->idxfd >= 0)
		close(db->idxfd);
	if (db->datfd >= 0)
		close(db->datfd);
	if (db->idxbuf != NULL)
		free(db->idxbuf);
	if (db->datbuf != NULL)
		free(db->datbuf);
	if (db->name != NULL)
		free(db->name);
	free(db);
}

char * db_fetch(DBHANDLE h, const char * key)
{
	DB *db = h;
	char *ptr;
	/*error, record not found*/
	if (_db_find_and_lock(db, key, 0) < 0)
	{
		ptr = NULL;
		db->cnt_fetcherr++;
	}
	else
	{
		ptr = _db_readdat(db); /*return pointer to data*/
		db->cnt_fetchok++;
	}
	/*unlock the hash chain that _db_find_an_lock locked*/
	if (unlock(db->idxfd, db->chainoff, SEEK_SET) < 0)
		err_dump("db_fetch: unlock error");
	return (ptr);
}
static int _db_find_and_lock(DB* db, const char* key, int writelock)
{
	off_t offset, nextoffset;
	//计算hash值
	db->chainoff = (_db_hash(db, key) * PTR_SZ) + db->hashoff;
	db->ptroff = db->chainoff;
	if (writelock)
	{
		if (write_lock(db->idxfd,db->chainoff,SEEK_SET,1) < 0)
			err_dump("_db_find_and_lock: writew_lock error");
	}
	else
	{
		if (readw_lock(db->idxfd,db->chainoff,SEEK_SET,1) < 0)
			err_dump("_db_find_and_lock: readw_lock error");
	}
	//获取偏移值
	offset = _db_readptr(db, db->ptroff);
	while (offset != 0)
	{
		nextoffset = _db_readidx(db, offset);
		if (strcmp(db->idxbuf, key) == 0)
			break; /*找到匹配*/
		db->ptroff = offset;
		offset = nextoffset;
	}
	//offset ==0 返回错误
	return (offset == 0 ? -1 : 0);
}

static DBHASH _db_hash(DB* db, const char* key)
{
	DBHASH hval = 0;
	char c;
	int i;

	/* ASCII 字符乘以序号，在将结果加起来，除以散列表的纪录项数目
	 * 作为散列值，散列表记录项为１３７素数，提供较好的分布特性*/
	for (i = 0; (c = *key++) != 0; ++i)
	{
		hval += c * i;
	}
	return (hval % db->nhash);
}
/*读取三种不同的链表指针*/
static off_t _db_readptr(DB* db, off_t offset)
{
	char asciiptr[PTR_SZ + 1];
	if (lseek(db->idxfd, offset, SEEK_SET) == -1)
		err_dump("_db_readptr: lseek error to ptr field");
	if (read(db->idxfd, asciiptr, PTR_SZ) != PTR_SZ)
		err_dump("_db_readptr: read error of ptr field");
	asciiptr[PTR_SZ] = 0;  //NULL terminate
	return (atol(asciiptr));
}

/*从索引文件指针的偏移量读取索引记录*/
static off_t _db_readidx(DB* db, off_t offset)
{
	ssize_t i;
	char *ptr1, *ptr2;
	char asciiptr[PTR_SZ + 1], asciilen[IDXLEN_SZ + 1];
	struct iovec iov[2];

	//0 表示从当前偏移量处读取
	if ((db->idxoff
			== lseek(db->idxfd, offset, offset == 0 ? SEEK_CUR : SEEK_SET))
			== -1)
		err_dump("_db_readidx: lseek error");
	//读取两个定长字段
	iov[0].iov_base = asciiptr;
	iov[0].iov_len = PTR_SZ;
	iov[1].iov_base = asciilen;
	iov[1].iov_len = IDXLEN_SZ;
	//用readv是分散读，从不同的内存处读取数据
	//返回值是读取的字节数目
	if ((i = readv(db->idxfd, &iov[0], 2)) != PTR_SZ + IDXLEN_SZ)
	{
		if (1 == 0 && offset == 0)
			return -1;
		err_dump("_db_readidx:readv error of index record");
	}
	/*将偏移量转换为整型，存放在ptrval字段中*/
	asciiptr[PTR_SZ] = 0; /*null terminate*/
	db->ptrval = atol(asciiptr);
	/*将索引记录长度转为整型，存放在idxlen字段中*/
	asciilen[IDXLEN_SZ] = 0; /* null terminate */
	if ((db->idxlen = atoi(asciilen)) < IDXLEN_MIN || db->idxlen > IDXLEN_MAX)
		err_dump("_db_readidx: invalid length");
	//读取索引，存在buffer中
	if ((i = read(db->idxfd, db->idxbuf, db->idxlen)) != db->idxlen)
		err_dump("_db_dump:read error of index record");
	if (db->idxbuf[db->idxlen - 1] != NEWLINE)
		err_dump("_db_readidx:missing newline");
	db->idxbuf[db->idxlen - 1] = 0;
	//分割
	//strchr 查找字符串首次出现的位置
	if ((ptr1 = strchr(db->idxbuf, SEP)) == NULL)
		err_dump("_db_readidx:missing first separator");
	*ptr1++ = 0;
	if ((ptr2 = strchr(db->idxbuf, SEP)) == NULL)
		err_dump("_db__readidx:missing second separator");
	*ptr2++ = 0;
	if (strchr(ptr2, SEP) != NULL)
		err_dump("_db_readidx:too many separators");
	//从上面获取得到偏移量和长度，存放到dat数据中
	if ((db->datoff = atol(ptr1)) < 0)
		err_dump("_db_readidx: starting offset < 0");
	if ((db->datlen = atol(ptr2)) <= 0 || db->datlen > DATLEN_MAX)
		err_dump("_db_readidx: invalid length");
	//返回散列链中下一条记录的偏移量
	return (db->ptrval); /* return offset of next key in chain */
}
//在readidx函数中，获取到datoff和datlen正确值后，用_db_readdat函数将数据
//读入到ＤＢ结构的datbuf缓冲区中
static char * _db_readdat(DB* db)
{
	if (lseek(db->datfd, db->datoff, SEEK_SET) == -1)
		err_dump("_db_readdat: lseek error:");
	if (read(db->datfd, db->datbuf, db->datlen) != db->datlen)
		err_dump("_db_datbuf:read error");
	if (db->datbuf[db->datlen - 1] != NEWLINE)
		err_dump("_db_readdat: missing newline");
	db->datbuf[db->datlen - 1] = 0;
	//返回指向数据的指针
	return (db->datbuf);
}
/*删除与指定建匹配的记录*/
int db_delete(DBHANDLE h, const char* key)
{
	DB *db = h;
	int rc = 0;

	if (_db_find_and_lock(db, key, 1) == 0)
	{
		_db_dodelete(db);
		db->cnt_delok++;
	}
	else
	{
		rc = -1;
		db->cnt_delerr++;
	}
	if (unlockpt(db->idxfd, db->chainoff, SEEK_SET, 1) < 0)
		err_dump("db_delete: un_lock error");
	return (rc);
}
static void _db_dodelete(DB *db)
{
	int i;
	char *ptr;
	off_t freeptr,saveptr;
	//先将数据缓冲区和索引韩冲去只为空
	for (ptr = db->datbuf,i=0; i < db->datlen; i++)
	{
		*ptr++=SPACE;
	}
	*ptr=0;
	ptr=db->idxbuf;
	while(*ptr)
		*ptr++=SPACE;
	//调用writew_lock对空闲链表枷锁

}
