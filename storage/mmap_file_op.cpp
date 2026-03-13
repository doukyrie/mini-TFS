#include "common.h"
#include "mmap_file_op.h"

namespace qiniu
{
    namespace largefile
    {
        //构造函数
        MMapFileOperation::MMapFileOperation(const std::string &file_name,int open_flag):
        FileOperation(file_name,open_flag),file_mmap(NULL),isMapped(false){}    //初始化列表中先构造一个FileOperation类

        //析构函数
        MMapFileOperation::~MMapFileOperation()
        {
            if(this->file_mmap != NULL)
            {
                delete file_mmap;
                file_mmap = NULL;
            }
        }

        //含偏移的读文件
        int MMapFileOperation::pread_file(char *buf, int32_t size,int64_t offset)
        {
            //当文件已经映射
            
            if(this->isMapped)
            {
                //要读的位置（偏移量）加上要读的大小超过了内存映射的大小，需要内存扩容
                //扩容一次后再去判断是否超过
                if(size+offset > this->file_mmap->get_size())
                {
                    this->file_mmap->remmap_file();
                }

                //要读的位置（偏移量）加上要读的大小没有超过内存映射的大小
                if(size+offset <= this->file_mmap->get_size())
                {
                    memcpy(buf,(char *)this->get_map_data()+offset,size);
                    return OP_SUCCESS;
                }
            }

            return FileOperation::pread_file(buf,size,offset);  //如果没有映射则调用父类的直接从磁盘里读文件
        }

        //含偏移的写文件
        int MMapFileOperation::pwrite_file(const char *buf,int32_t size,int64_t offset)
        {
            if(this->isMapped)
            {
                //要写的位置（偏移量）加上要写的大小超过了内存映射的大小，需要内存扩容
                //扩容一次后再去判断是否超过
                if(size+offset > this->file_mmap->get_size())
                {
                    this->file_mmap->remmap_file();
                }

                //要写的位置（偏移量）加上要写的大小没有超过内存映射的大小
                if(size+offset <= this->file_mmap->get_size())
                {
                    memcpy((char *)this->get_map_data()+offset,buf,size);
                    return OP_SUCCESS;
                }
            }

            return FileOperation::pwrite_file(buf,size,offset);  //如果没有映射则调用父类的直接从磁盘里读文件
        }

        //将文件映射到内存
        int MMapFileOperation::mmap_file(const MMapOption& mmap_option)
        {
            if(mmap_option.max_mmap_size < mmap_option.first_mmap_size)
            {
                fprintf(stderr,"MMapFileOperation::mmap_file failed. mmap_option.max_mmap_size < mmap_option.first_mmap_size\n");
                return OP_ERROR;
            }

            if(mmap_option.max_mmap_size <= 0)
            {
                fprintf(stderr,"MMapFileOperation::mmap_file failed. mmap_option.max_mmap_size <= 0\n");
                return OP_ERROR;
            }

            //确保文件打开
            int fd = check_file();
            if(fd<0)
            {
                fprintf(stderr,"MMapFileOperation::mmap_file check_file failed. fd<0\n");
                return OP_ERROR;
            }

            //当文件还没有映射时，进行映射
            if(!this->isMapped)
            {
                if(this->file_mmap != NULL)
                {
                    delete file_mmap;
                }

                file_mmap = new MMapFile(mmap_option,fd);
                this->isMapped = file_mmap->mmap_file(true);
            }

            if(!this->isMapped)
            {
                fprintf(stderr,"MMapFileOperation::mmap_file: file mmap failed.\n");
                return OP_ERROR;
            }
            else return OP_SUCCESS;
        }

        //解除映射
        int MMapFileOperation::munmap_file()
        {
            if(!this->isMapped)
            {
                return OP_SUCCESS;
            }

            if(this->file_mmap == NULL)
            {
                this->isMapped = false;
                return OP_SUCCESS;
            }

            this->isMapped = false;
            delete this->file_mmap;
            return OP_SUCCESS;
        }

        //获取映射位置的首地址
        void * MMapFileOperation::get_map_data() const
        {
            if(!this->isMapped || this->file_mmap == NULL)
            {
                return NULL;
            }

            return this->file_mmap->get_data();
        }

        //将内存映射中的数据同步到本地
        int MMapFileOperation::flush_file()
        {
            //没映射，也有可能写在本地，因此调用FileOperation的flush_file进行同步
            if(!this->isMapped)
            {
                return FileOperation::flush_file();
            }

            if(file_mmap->sync_file())
            {
                return OP_SUCCESS;
            }
            else return OP_ERROR;
        }

    }
}