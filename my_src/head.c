/*
 * head.c
 *
 *  Created on: 2015年9月11日
 *      Author: panzg
 */

#include"./head.h"
#include<errno.h>
#include<stdarg.h>
#include<fcntl.h>
static void err_doit(int,int,const char*,va_list);

void err_ret(const char* fmt,...)
{
	va_list ap;
	va_start(ap,fmt);
	err_doit(1,errno,fmt,ap);
	va_end(ap);
}

void err_sys(const char* fmt,...)
{
	va_list ap;
	va_start(ap,fmt);
	err_doit(1,errno,fmt,ap);
	va_end(ap);
	exit(1);
}

void
err_exit(int error, const char *fmt, ...)
{
	va_list		ap;

	va_start(ap, fmt);
	err_doit(1, error, fmt, ap);
	va_end(ap);
	exit(1);
}

/*
 * Fatal error related to a system call.
 * Print a message, dump core, and terminate.
 */
void err_dump(const char  *fmt,...)
{
	va_list ap;
	va_start(ap,fmt);
	err_doit(1,errno,fmt,ap);
	va_end();
	abort(); /* dump core and terminate */
	exit(1); /* shouldn't get here */
}
/*
 * Nonfatal error unrelated to a system call.
 * Print a message and return.
 */
void err_msg(const char* fmt,...)
{
	va_list ap;
	va_start(ap,fmt);
	err_doit(0,0,fmt,ap);
	va_end();
}
/*
 * Fatal error unrelated to a system call.
 * Print a message and terminate.
 */
void err_quit(const char*fmt,...)
{
	va_list ap;
	va_start(ap,fmt);
	err_doit(0,0,fmt,ap);
	va_end(ap);
	exit(1);
}
/*
 * Print a message and return to caller.
 * Caller specifies "errnoflag".
 */
static void err_doit(int errnoflag,int error,const char* fmt,va_list ap)
{
	char buf[MAXLINE];
	vsnprintf(buf,MAXLINE,fmt,ap);
	if(errnoflag)
		snprinf(buf+strlen(buf),MAXLINE-strlen(buf),":%s",strerror(error));
	strcat(buf,"\n");
	fflush(stdout);  /*in case stdout and stderr are the same*/
	fputs(buf,stderr);
	fflush(NULL); /*flushes all stdio output streams*/
}

int lock_reg(int fd,int cmd,int type,off_t offset,int whence,off_t len)
{
	struct flock lock;
	lock.l_type=type;  /* F_RDLCK, F_WRLCK, F_UNLCK */
	lock.l_start=offset; /* byte offset, relative to l_whence */
	lock.l_whence=whence; /* SEEK_SET, SEEK_CUR, SEEK_END */
	lock.l_len=len; /* #bytes (0 means to EOF) */

	return (fcntl(fd,cmd,&lock));
}
