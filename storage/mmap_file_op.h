#ifndef MMAP_FILE_OP_H
#define MMAP_FILE_OP_H

#include "common.h"
#include "file_op.h"
#include "mmap_file.h"

namespace qiniu
{
    namespace largefile
    {
        class MMapFileOperation:public FileOperation
        {
        public:
            //构造函数
            MMapFileOperation(const std::string &file_name, int open_flag = O_CREAT | O_LARGEFILE | O_RDWR);

            //析构函数
            ~MMapFileOperation();

            //含偏移的读文件
            int pread_file(char *buf, int32_t size,int64_t offset); //buf：存放读的数据 size：要读的大小    offset：偏移量（从哪里开始读）

            //含偏移的写文件
            int pwrite_file(const char *buf,int32_t size,int64_t offset);   //buf：要写的内容   size：写的大小  offset：从哪里开始写

            //将文件映射到内存
            int mmap_file(const MMapOption& mmap_option);

            //解除映射
            int munmap_file();

            //获取映射位置的首地址
            void *get_map_data() const;

            //将内存映射中的数据同步到本地
            int flush_file();

        private:
            MMapFile *file_mmap;
            bool isMapped;  //判断是否已经映射，如果已经映射就无需重复映射
        };
    }
}





#endif