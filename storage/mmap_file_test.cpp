#include<iostream>
#include "mmap_file.h"

using namespace std;
using namespace qiniu;

static const mode_t open_mode = 0644;

int open_file(string file_name, int open_flag)
{
    int fd = open(file_name.c_str(),open_flag,open_mode);
    if(fd<0)
    {
        /*linux报错后都会将错误信息返回给errno，这是个整数，可以加个-号返回
        返回值是负数一般表示出错，当看到返回值为负时就会知道报错了，然后将-errno再取正数
        再调用strerror查看*/
        return -errno;
    }

    return fd;
}


int main()
{
    string file_name = "./test.txt";

    int fd = open_file(file_name,O_RDWR | O_CREAT | O_LARGEFILE);   //加上O_LARGEFILE才能创建大文件

    if(fd<0)
    {
        fprintf(stderr, "open file failed, file name: %s, error description: %s",file_name.c_str(), strerror(-fd));
        return -1;
    }

    largefile::MMapOption mmap_option;
    mmap_option.max_mmap_size = 1024*1024;
    mmap_option.first_mmap_size = 4096;
    mmap_option.per_mmap_size = 4096;

    largefile::MMapFile *mmap_file = new largefile::MMapFile(mmap_option,fd);

    bool is_mapped = mmap_file->mmap_file(true);

    if(is_mapped)
    {
        mmap_file->remmap_file();

        memset(mmap_file->get_data(),'1',mmap_file->get_size());    //将内存中的那块大小中的内容全部设为8
        if(mmap_file->sync_file())  //写完后将内存中的文件同步到本地文件中
        {
            printf("sync_file success\n");
        }
        else printf("sync_file failed\n");
    }
    else
    {
        printf("mmap failed\n");
        return -1;
    }

    mmap_file->munmap_file();

    close(fd);

    return 0;
}