#include "file_op.h"

namespace qiniu
{
    namespace largefile
    {
        //构造函数
        FileOperation::FileOperation(const std::string& file_name, const int open_flag):
        fd(-1),open_flag(open_flag),size(0)
        {
            this->file_name = strdup(file_name.c_str());
        }

        //析构函数
        FileOperation::~FileOperation()
        {
            if(this->fd>0)
            {
                ::close(this->fd);  //::表示调用全局命名空间中的close函数，因为当前析构函数定义在largefile命名空间中，
                                    //如果该命名空间又定义了close方法，则会优先调用，而不是先调用全局的
                
                free(this->file_name);
                this->file_name = NULL;
            }
        }

        //打开文件
        int FileOperation::open_file()
        {
            if(this->fd>0)
            {
                printf("file already open\n");
                return this->fd;
            }

            int fd = ::open(file_name,this->open_flag,this->open_mode);
            if(fd<0)
            {
                return -errno;
            }

            this->fd = fd;
            return fd;
        }

        //关闭文件
        void FileOperation::close_file()
        {
            if(this->fd<0)
            {
                printf("no file open,no need to close.\n");
                return;
            }

            close(this->fd);
            this->fd = -1;
        }

        //读文件
        int FileOperation::pread_file(char *buf, const int32_t size, const int64_t offset)
        {
            int32_t left = size;    //剩余还要读的大小，起始是size
            int64_t read_offset = offset;   //相对于起始位置的偏移量，每读一次就会偏移一点，起始是offset，因为还没开始读，所以是从指定要读的位置开始
            int32_t read_len = 0;   //每次读取的长度
            char *p_temp = buf; //因为是多次读取，所以每一次读取的内容分段存入buf，起始是buf首地址，每读一次temp都会向后移

            int i = 0;  //标记，防止偶尔因意外读不了数据时还频繁申请读数据导致系统崩溃
            while(left>0)
            {
                i++;
                if(i>=this->max_disk)   //当尝试5次都没读取成功则跳出循环，不占用系统资源
                {
                    printf("pread_file failed. over max_disk.\n");
                    break;
                }

                int fd = check_file();
                if(fd<0) return -errno;

                read_len = pread64(fd,p_temp,left,read_offset);
                //多次读取时传入的是每次读取的参数
                if(read_len<0)
                {
                    read_len = -errno;
                    if(-read_len == EINTR || -read_len == EAGAIN)  //EINTR表示系统临时中断，EAGAIN表示系统希望你再次尝试
                    {
                        continue;
                    }
                    else if(-read_len == EBADF) //文件描述符损坏
                    {
                        this->fd = -1;
                        continue;   //下次循环用check_file再打开一次文件重新写
                    }
                    else return read_len;
                }
                else if(read_len == 0)  //pread时已经读完了，正好到文件尾部
                {
                    break;
                }
                else
                {
                    left -= read_len;
                    read_offset += read_len;
                    p_temp += read_len;
                }
            }

            //当跳出循环后还是没有读完
            if(left != 0)
            {
                return EXIT_DISK_OP_INCOMPLETE;
            }

            return OP_SUCCESS;
        }

        //写文件
        int FileOperation::pwrite_file(const char* buf, const int32_t size, const int64_t offset)
        {
            int32_t left = size;    //剩余还要写的大小，起始是size
            int64_t write_offset = offset;   //相对于起始位置的偏移量，每写一次就会偏移一点，起始是offset，因为还没开始写，所以是从指定要写的位置开始
            int32_t write_len = 0;   //每次写的长度
            const char *p_temp = buf; //因为是多次写，所以每一次写的内容分段存入buf，起始是buf首地址，每写一次temp都会向后移

            int i = 0;
            while(left>0)
            {
                i++;
                if(i>=this->max_disk)
                {
                    printf("pread_file failed. over max_disk.\n");
                    break;
                }

                int fd = check_file();
                if(fd<0) return -errno;

                write_len = pwrite64(fd,p_temp,left,write_offset);
                if(write_len<0)
                {
                    write_len = -errno;
                    if(-write_len == EINTR || -write_len == EAGAIN)
                    {
                        continue;
                    }
                    else if(-write_len == EBADF)
                    {
                        this->fd = -1;
                        continue;
                    }
                    else return -errno;
                }
                else if(write_len == 0)
                {
                    break;
                }
                else
                {
                    left -= write_len;
                    write_offset += write_len;
                    p_temp += write_len;
                }
            }

            if(left!=0)
            {
                return EXIT_DISK_OP_INCOMPLETE;
            }

            return OP_SUCCESS;
        }

        //不提供偏移量的write
        int FileOperation::write_file(const char* buf, const int32_t size)
        {
            int32_t left = size;    //剩余还要写的大小，起始是size
            int32_t write_len = 0;   //每次写的长度
            const char *p_temp = buf; //因为是多次写，所以每一次写的内容分段存入buf，起始是buf首地址，每写一次temp都会向后移

            int i = 0;
            while(left>0)
            {
                i++;
                if(i>=this->max_disk)
                {
                    printf("pread_file failed. over max_disk.\n");
                    break;
                }

                int fd = check_file();
                if(fd<0) return -errno;

                write_len = ::write(fd,p_temp,left);
                if(write_len<0)
                {
                    write_len = -errno;
                    if(-write_len == EINTR || -write_len == EAGAIN)
                    {
                        continue;
                    }
                    else if(-write_len == EBADF)
                    {
                        this->fd = -1;
                        continue;   //让下次循环再调用check_file
                    }
                    else return -errno;
                }
                else if(write_len == 0)
                {
                    break;
                }
                else
                {
                    left -= write_len;
                    p_temp += write_len;
                }
            }

            if(left!=0)
            {
                return EXIT_DISK_OP_INCOMPLETE;
            }

            return OP_SUCCESS;
        }

        //未打开文件时为了获取文件大小，打开文件
        int FileOperation::check_file()
        {
            if(this->fd < 0)
            {
                int fd = ::open(this->file_name,this->open_flag,this->open_mode);
                this->fd = fd;
                return fd;
            }
            
            return this->fd;
        }

        //获取文件大小
        int64_t FileOperation::get_file_size()
        {
            int fd = check_file();
            if(fd<0)
            {
                fprintf(stderr,"get file size failed, check_file failed.\n");
                return -1;
            }

            struct stat buf;
            if(fstat(fd,&buf)==-1)
            {
                fprintf(stderr,"get file size failed. fstat failed: %s",strerror(errno));
                return -1;
            }
            
            //if(this->fd<0) close(fd);   //如果文件本身没打开，用check_file打开后应当关闭

            return buf.st_size;
        }

        //截断文件
        int FileOperation::ftruncate_file(int64_t size)
        {
            int fd = check_file();
            if(fd<0)
            {
                fprintf(stderr,"ftruncate_file failed. check file failed.\n");
                return fd;
            }

            int result = ::ftruncate(fd,size);    //执行成功返回0，失败返回-1
            if(this->fd<0) close(fd);
            return result;
        }

        //对文件进行偏移
        int FileOperation::seek_file(int64_t offset)
        {
            int fd = check_file();
            if(fd<0) return fd;

            return ::lseek(fd,offset,SEEK_SET);
        }

        //将文件立即写入磁盘
        int FileOperation::flush_file()
        {
            if(this->open_flag & O_SYNC)    //如果打开文件的方式已经包含同步，就无需再立即写入
            {
                return 0;
            }

            int fd = check_file();
            if(fd<0) return fd;

            return ::fsync(fd);   //将缓冲区数据写入磁盘，成功返回0，失败返回-1
        }

        //删除文件
        int FileOperation::unlink_file()
        {
            close_file();
            return ::unlink(this->file_name);
        }

        int FileOperation::get_fd()
        {
            return this->fd;
        }
    }
}