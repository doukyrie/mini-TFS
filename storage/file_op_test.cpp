#include "common.h"
#include "file_op.h"

using namespace std;
using namespace qiniu;

int main()
{
    string file_name = "./file_op_test.txt";

    largefile::FileOperation *file_op = new largefile::FileOperation(file_name,O_CREAT | O_RDWR | O_LARGEFILE);

    int fd = file_op->open_file();
    if(fd<0)
    {
        fprintf(stderr,"open file failed, des: %s\n",strerror(-fd));
        return -1;
    }

    char write_buf[BUFSIZ];
    sprintf(write_buf,"fuck you!\n");
    int ret = file_op->write_file(write_buf,strlen(write_buf));
    if(ret<0)
    {
        if(ret == largefile::EXIT_DISK_OP_INCOMPLETE)
        {
            printf("write_file failed: EXIT_DISK_OP_INCOMPLETE\n");
        }
        else
        {
            fprintf(stderr,"write_file failed: %s\n",strerror(-ret));
        }
    }

    int file_size = file_op->get_file_size();
    char read_buf[BUFSIZ];
    ret = file_op->pread_file(read_buf,file_size,0);
    if(ret<0)
    {
        if(ret == largefile::EXIT_DISK_OP_INCOMPLETE)
        {
            printf("read_file failed: EXIT_DISK_OP_INCOMPLETE\n");
        }
        else
        {
            fprintf(stderr,"read_file failed: %s\n",strerror(-ret));
        }
    }

    printf("read: %s\n",read_buf);

    file_op->close_file();

    return 0;
}