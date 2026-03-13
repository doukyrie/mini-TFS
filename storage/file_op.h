#ifndef FILE_OP_H
#define FILE_OP_H

#include "common.h"

namespace qiniu
{
    namespace largefile
    {
        class FileOperation
        {
        public:
            FileOperation(const std::string& file_name, const int open_flag = O_RDWR | O_LARGEFILE);
            ~FileOperation();

            //打开和关闭文件
            int open_file();
            void close_file();

            //将文件立即写入磁盘
            int flush_file();

            //删除文件
            int unlink_file();

            //读文件
            virtual int pread_file(char *buf, const int32_t size, const int64_t offset);    //buf：将文件读到缓冲区 size：读文件的长度    offset：文件偏移量，读的位置

            //写文件
            virtual int pwrite_file(const char* buf, const int32_t size, const int64_t offset); //将buf写入文件，buf不能修改

            //直接提供偏移量，从当前位置开始写，不需要再传偏移量
            int write_file(const char* buf, const int32_t size);

            //获取文件大小
            int64_t get_file_size();

            //文件截断，修改文件大小
            int ftruncate_file(int64_t size);   //size：截断后的大小

            //设置文件偏移量
            int seek_file(int64_t offset);

            //获取文件描述符
            int get_fd();

        protected:
            //没有显式调用open_file，即文件还没打开时，仍然希望查看文件大小，则用check_file，先打开，获取文件大小后再关闭
            int check_file();


        private:
            int fd; //文件描述符
            int64_t size;   //文件大小
            char * file_name;  //文件名
            int open_flag;  //文件打开标志
        
        //子类当前类能调，类外不能调，private是子类不能调只有当前类能调
        protected:
            static const mode_t open_mode = 0644;   //文件权限
            static const int max_disk = 5;  //最大访问磁盘的次数，防止偶尔失败后仍频繁读写导致系统崩溃

        };
    }
}




#endif