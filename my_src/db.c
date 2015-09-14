/*
 * db.c
 *
 *  Created on: 2015年9月11日
 *      Author: panzg
 */

#include"include/db.h"
#include"include/head.h"

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
static DBHASH _db_hash(DB*, const char*);
static char* _db_readdat(DB*);
static off_t  _db_readidx(DB*, off_t);
static off_t _db_readptr(DB*, off_t);
static void _db_writedat(DB*, const char*, off_t, int);
static void _db_writeidx(DB*, const char*, off_t, int, off_t);
static void _db_writeptr(DB *, off_t, off_t);

/*
 * open or create a database
 */
DBHANDLE db_open_error(const char* pathname, int oflags, ...)
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
DBHANDLE db_open(const char *pathname, int oflag, ...)
{
	DB			*db;
	int			len, mode;
	size_t		i;
	char		asciiptr[PTR_SZ + 1],
				hash[(NHASH_DEF + 1) * PTR_SZ + 2];
					/* +2 for newline and null */
	struct stat	statbuff;

	/*
	 * Allocate a DB structure, and the buffers it needs.
	 */
	len = strlen(pathname);
	if ((db = _db_alloc(len)) == NULL)
		err_dump("db_open: _db_alloc error for DB");

	//初始化db
	db->nhash   = NHASH_DEF;/* hash table size */
	db->hashoff = HASH_OFF;	/* offset in index file of hash table */
	strcpy(db->name, pathname);
	strcat(db->name, ".idx");

	if (oflag & O_CREAT) {
		va_list ap;

		va_start(ap, oflag);
		mode = va_arg(ap, int);
		va_end(ap);

		/*
		 * Open index file and data file.
		 */
		db->idxfd = open(db->name, oflag, mode);
		strcpy(db->name + len, ".dat");
		db->datfd = open(db->name, oflag, mode);
	} else {
		/*
		 * Open index file and data file.
		 */
		db->idxfd = open(db->name, oflag);
		strcpy(db->name + len, ".dat");
		db->datfd = open(db->name, oflag);
	}

	if (db->idxfd < 0 || db->datfd < 0) {
		_db_free(db);
		return(NULL);
	}

	if ((oflag & (O_CREAT | O_TRUNC)) == (O_CREAT | O_TRUNC)) {
		/*
		 * If the database was created, we have to initialize
		 * it.  Write lock the entire file so that we can stat
		 * it, check its size, and initialize it, atomically.
		 */
		if (writew_lock(db->idxfd, 0, SEEK_SET, 0) < 0)
			err_dump("db_open: writew_lock error");

		if (fstat(db->idxfd, &statbuff) < 0)
			err_sys("db_open: fstat error");

		if (statbuff.st_size == 0) {
			/*
			 * We have to build a list of (NHASH_DEF + 1) chain
			 * ptrs with a value of 0.  The +1 is for the free
			 * list pointer that precedes the hash table.
			 */
			sprintf(asciiptr, "%*d", PTR_SZ, 0);
			hash[0] = 0;
			for (i = 0; i < NHASH_DEF + 1; i++)
				strcat(hash, asciiptr);
			strcat(hash, "\n");
			i = strlen(hash);
			if (write(db->idxfd, hash, i) != i)
				err_dump("db_open: index file init write error");
		}
		if (un_lock(db->idxfd, 0, SEEK_SET, 0) < 0)
			err_dump("db_open: un_lock error");
	}
	db_rewind(db);
	return(db);
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

char *  db_fetch(DBHANDLE h, const char * key)
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
	if (un_lock(db->idxfd, db->chainoff, SEEK_SET,1) < 0)
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
	if (un_lock(db->idxfd, db->chainoff, SEEK_SET, 1) < 0)
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
	if(writew_lock(db->idxfd,FREE_OFF,SEEK_SET,1)<0)
		err_dump("_db_dodelete:writew_lock error");
	//清空数据记录
	_db_writedat(db,db->datbuf,db->datoff,SEEK_SET);
	freeptr = _db_readptr(db,FREE_OFF);
	saveptr=db->ptrval;
	_db_writeidx(db,db->idxbuf,db->idxoff,SEEK_SET,freeptr);
	_db_writeptr(db,FREE_OFF,db->idxoff);
	//修改散列链中前一条记录的指针，使其正好指向删除之后的一条记录
	_db_writeptr(db,db->ptroff,saveptr);

	if(un_lock(db->idxfd,FREE_OFF,SEEK_SET,1)<0)
		err_dump("_db_dodelete : unlock error");
}

static void _db_writedat(DB *db,const char *data,off_t offset,int whence)
{
	struct iovec iov[2];
	static char newline = NEWLINE;
	//如果是增加append记录，需要枷锁；如果是重写记录，则不需要加锁
	if(whence==SEEK_END)
		if(writew_lock(db->datfd,0,SEEK_SET,0)<0)
			err_dump("_db_writedat:writew_lock error");

	if((db->datoff = lseek(db->datfd,offset,whence))==-1)
		err_dump("_db_writedat:lseek error");
	db->datlen = strlen(data)+1;

	iov[0].iov_base = (char*)data;
	iov[0].iov_len=db->datlen-1;
	iov[1].iov_base=&newline;
	iov[1].iov_len = 1;
	if(writev(db->datfd,&iov[0],2)!=db->datlen)
		err_dump("_db_writedat:writev error of data record");
	if(whence==SEEK_END)
		if(un_lock(db->datfd,0,SEEK_SET,0)<0)
			err_dump("_db_writedat:un_lock error");
}

static void _db_writeidx(DB *db,const char* key,off_t offset,int whence,off_t ptrval)
{
	struct iovec iov[2];
	char asciiptrlen[PTR_SZ+IDXLEN_SZ+1];
	int len;
	char *fmt;

	if((db->ptrval = ptrval)<0 || ptrval>PTR_MAX)
		err_quit("_db_writeidx: invalid ptr: %d", ptrval);
	if (sizeof(off_t) == sizeof(long long))
		fmt = "%s%c%lld%c%d\n";
	else
		fmt = "%s%c%ld%c%d\n";
	sprintf(db->idxbuf, fmt, key, SEP, db->datoff, SEP, db->datlen);
	//如果是appending,就得加锁
	if (whence == SEEK_END)		/* we're appending */
			if (writew_lock(db->idxfd, ((db->nhash+1)*PTR_SZ)+1,
			  SEEK_SET, 0) < 0)
				err_dump("_db_writeidx: writew_lock error");

	/*
	 * Position the index file and record the offset.
	 */
	if ((db->idxoff = lseek(db->idxfd, offset, whence)) == -1)
		err_dump("_db_writeidx: lseek error");

	iov[0].iov_base = asciiptrlen;
	iov[0].iov_len = PTR_SZ + IDXLEN_SZ;
	iov[1].iov_base = db->idxbuf;
	iov[1].iov_len = len;
	if (writev(db->idxfd, &iov[0], 2) != PTR_SZ + IDXLEN_SZ + len)
		err_dump("_db_writeidx: writev error of index record");

	if (whence == SEEK_END)
		if (un_lock(db->idxfd, ((db->nhash+1)*PTR_SZ)+1,
				SEEK_SET, 0) < 0)
			err_dump("_db_writeidx: un_lock error");
}
/*将一个链表指针写入到索引文件中*/
static void _db_writeptr(DB *db,off_t offset,off_t ptrval)
{
	char asciiptr[PTR_SZ+1];
	if(ptrval<0 || ptrval>PTR_MAX)
		err_quit("_Db_writeptr:invalid ptr:%d",ptrval);
	sprintf(asciiptr,"%*ld",PTR_SZ,ptrval);

	if(lseek(db->idxfd,offset,SEEK_SET)==-1)
		err_dump("_db_writeptr: lseek error to ptr field");
	if (write(db->idxfd, asciiptr, PTR_SZ) != PTR_SZ)
		err_dump("_db_writeptr: write error of ptr field");
}

/*
 * 在数据库中插入一条新的记录
 * return : 0代表正常，１代表存在但是DB_INSERT被制定，－１代表出错．
 */
int db_store(DBHANDLE h,const char* key,const char*data,int flag)
{
	DB *db =h;
	int rc,keylen,datlen;
	off_t ptrval;

	//先验证flag是够有效
	if(flag != DB_INSERT && flag!=DB_REPLACE && flag!=DB_STORE)
	{
		errno =EINVAL;
	    return (-1);
	}
	keylen =strlen(key);
	datlen = strlen(data)+1;
	if (datlen < DATLEN_MIN || datlen > DATLEN_MAX)
		err_dump("db_store: invalid data length");
	if(_db_find_and_lock(db,key,1)<0)          /*未找到*/
	{
		if(flag == DB_REPLACE)
		{
			rc = -1;
			db->cnt_storerr++;
			errno = ENOENT;
			goto doreturn;
		}
		ptrval = _db_readptr(db,db->chainoff);
		if(_db_findfree(db,keylen,datlen)<0)
		{
			//第一种情况
			//没有找到对应大小的记录，则需要查到尾部
			_db_writedat(db,data,0,SEEK_END);
			_db_writeidx(db,key,0,SEEK_END,ptrval);

			_db_writeptr(db,db->chainoff,db->idxoff);
			db->cnt_stor1++;
		}
		else
		{
			//第二种情况
			//找到了对应大小的空记录
			_db_writedat(db,data,db->datoff,SEEK_SET);
			_db_writeidx(db,key,db->idxoff,SEEK_SET,ptrval);
			_db_writeptr(db,db->chainoff,db->idxoff);
			db->cnt_stor2++;
		}
	}
	else
	{
		if(flag == DB_INSERT)   /*记录已经存在，flag不能为db_insert*/
		{
			rc=1;
			db->cnt_storerr++;
			goto doreturn;
		}
		//第三种情况,替换，且记录长度不一致，此时，需要先将老记录删除，然后写索引和数据
		if(datlen!=db->datlen)
		{
			_db_dodelete(db);
			ptrval = _db_readptr(db,db->chainoff);
			_db_writedat(db,data,0,SEEK_END);
			_db_writeidx(db,key,0,SEEK_END,ptrval);
			_db_writeptr(db,db->chainoff,db->idxoff);
			db->cnt_stor3++;
		}
		//第四种情况,长度恰好一致，只需要重写数据即可．
		else
		{
			_db_writedat(db,data,db->datoff,SEEK_SET);
			db->cnt_stor4++;
		}
	}
	//公共处理逻辑，包括解锁
	doreturn:
	if(un_lock(db->idxfd,db->chainoff,SEEK_SET,1)<0)
		err_dump("db_store: un_lock error");
	return (rc);
}
//找到一个指定的大小的空闲索引记录，以及相关联的数据记录
static int _db_findfree(DB *db,int keylen,int datlen)
{
	int rc;
	off_t offset,nextoffset,saveoffset;
	if(writew_lock(db->idxfd,FREE_OFF,SEEK_SET,1)<0)
		err_dump("_db_findfree: writew_lock error");
	saveoffset = FREE_OFF;
	offset = _db_readptr(db,saveoffset);

	while (offset != 0) {
		nextoffset = _db_readidx(db, offset);
		if (strlen(db->idxbuf) == keylen && db->datlen == datlen)
			break;		/* found a match */
		saveoffset = offset;
		offset = nextoffset;
	}

	if (offset == 0) {
		rc = -1;	/* no match found */
	} else {
		/*
		 * Found a free record with matching sizes.
		 * The index record was read in by _db_readidx above,
		 * which sets db->ptrval.  Also, saveoffset points to
		 * the chain ptr that pointed to this empty record on
		 * the free list.  We set this chain ptr to db->ptrval,
		 * which removes the empty record from the free list.
		 */
		_db_writeptr(db, saveoffset, db->ptrval);
		rc = 0;

		/*
		 * Notice also that _db_readidx set both db->idxoff
		 * and db->datoff.  This is used by the caller, db_store,
		 * to write the new index record and data record.
		 */
	}

	/*
	 * Unlock the free list.
	 */
	if (un_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
		err_dump("_db_findfree: un_lock error");
	return(rc);
}
//将数据库重置到初始状态，将索引文件的文件偏移量定位到索引文件的第一条记录
void db_rewind(DBHANDLE h)
{
	DB *db =h;
	off_t offset;
	offset = (db->nhash+1)*PTR_SZ;

	if((db->idxoff=lseek(db->idxfd,offset+1,SEEK_SET))==-1)
		err_dump("_db_rewind:lseek error");
}
//返回数据库的下一条记录，返回值是指向数据的指针
char *db_nextrec(DBHANDLE h,char*key)
{
	DB		*db = h;
		char	c;
		char	*ptr;

		/*
		 * We read lock the free list so that we don't read
		 * a record in the middle of its being deleted.
		 */
		if (readw_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
			err_dump("db_nextrec: readw_lock error");

		do {
			/*
			 * Read next sequential index record.
			 */
			if (_db_readidx(db, 0) < 0) {
				ptr = NULL;		/* end of index file, EOF */
				goto doreturn;
			}

			/*
			 * Check if key is all blank (empty record).
			 */
			ptr = db->idxbuf;
			while ((c = *ptr++) != 0  &&  c == SPACE)
				;	/* skip until null byte or nonblank */
		} while (c == 0);	/* loop until a nonblank key is found */

		if (key != NULL)
			strcpy(key, db->idxbuf);	/* return key */
		ptr = _db_readdat(db);	/* return pointer to data buffer */
		db->cnt_nextrec++;

	doreturn:
		if (un_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
			err_dump("db_nextrec: un_lock error");
		return(ptr);
}
