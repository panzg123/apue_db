/*
 * head.h
 *
 *  Created on: 2015年9月11日
 *      Author: panzg
 */

#ifndef MY_SRC_INCLUDE_HEAD_H_
#define MY_SRC_INCLUDE_HEAD_H_

#include<sys/types.h>
#include<sys/stat.h>
#include<sys/termios.h>

#ifndef TIOCGWINSZ
#include<sys/ioctl.h>
#endif

#include <stdio.h>		/* for convenience */
#include <stdlib.h>		/* for convenience */
#include <stddef.h>		/* for offsetof */
#include <string.h>		/* for convenience */
#include <unistd.h>		/* for convenience */
#include <signal.h>		/* for SIG_ERR */

#define MAXLINE 4096

#define FILE_MODE (S_IRUSR|S_IXUSR|S_IRGRP|S_IXOTH)

#define min(a,b) ((a)<(b)?(a):(b))
#define max(a,b) ((a)>(b)?(a):(b))


int		lock_reg(int, int, int, off_t, int, off_t); /* {Prog lockreg} */
#define read_lock(fd,offset,whence,len)  \
	lock_reg((fd),F_SETLK,F_RDLCK,(offset),(whence),(len))
#define	readw_lock(fd, offset, whence, len) \
			lock_reg((fd), F_SETLKW, F_RDLCK, (offset), (whence), (len))
#define	write_lock(fd, offset, whence, len) \
			lock_reg((fd), F_SETLK, F_WRLCK, (offset), (whence), (len))
#define	writew_lock(fd, offset, whence, len) \
			lock_reg((fd), F_SETLKW, F_WRLCK, (offset), (whence), (len))
#define	un_lock(fd, offset, whence, len) \
			lock_reg((fd), F_SETLK, F_UNLCK, (offset), (whence), (len))

pid_t	lock_test(int, int, off_t, int, off_t);		/* {Prog locktest} */

#define	is_read_lockable(fd, offset, whence, len) \
			(lock_test((fd), F_RDLCK, (offset), (whence), (len)) == 0)
#define	is_write_lockable(fd, offset, whence, len) \
			(lock_test((fd), F_WRLCK, (offset), (whence), (len)) == 0)

void err_dump(const char*,...);
void err_msg(const char*,...);
void err_quit(const char*,...);
void err_exit(int,const char*,...);
void err_ret(const char*,...);
void err_sys(const char*,...);



#endif /* MY_SRC_INCLUDE_HEAD_H_ */
