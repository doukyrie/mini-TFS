#include "mmap_file_op.h"
#include<iostream>

using namespace std;
using namespace qiniu;

int main()
{
    int ret = 0;
    const char * file_name = "mmap_file_op_test.txt";

    largefile::MMapFileOperation *mmap_file_op = new largefile::MMapFileOperation(file_name);

    int fd = mmap_file_op->open_file();
    if(fd<0)
    {
        fprintf(stderr,"open file failed: %s",strerror(-fd));
        return -1;
    }

    struct largefile::MMapOption mmap_option;
    mmap_option.max_mmap_size = 1024*1024;
    mmap_option.first_mmap_size = 4096;
    mmap_option.per_mmap_size = 4096;

    ret = mmap_file_op->mmap_file(mmap_option);
    if(ret<0)
    {
        mmap_file_op->close_file();
        return -1;
    }

    char read_buf[128];

    ret = mmap_file_op->pread_file(read_buf,sizeof(read_buf),0);
    if(ret < 0)
    {
        if(ret == largefile::EXIT_DISK_OP_INCOMPLETE)
        {
            printf("main: pread_file failed. res:EXIT_DISK_OP_INCOMPLETE\n");
        }
        else
        {
            fprintf(stderr,"main: pread_file failed. res:%s",strerror(-ret));
        }
    }
    printf("读到的内容：%s\n",read_buf);


    char write_buf[128];
    sprintf(write_buf,"fuck you\n");

    ret = mmap_file_op->pwrite_file(write_buf,strlen(write_buf),strlen(read_buf));
    if(ret < 0)
    {
        if(ret == largefile::EXIT_DISK_OP_INCOMPLETE)
        {
            printf("main: pwrite_file failed. res:EXIT_DISK_OP_INCOMPLETE\n");
        }
        else
        {
            fprintf(stderr,"main: pwrite_file failed. res:%s",strerror(-ret));
        }
    }

    ret = mmap_file_op->flush_file();
    if(ret < 0)
    {
        printf("flush_file failed.\n");
    }

    mmap_file_op->munmap_file();

    mmap_file_op->close_file();

    return 0;
}