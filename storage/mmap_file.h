#ifndef MMAP_FILE_H
#define MMAP_FILE_H

#include "common.h"

namespace qiniu
{
    namespace largefile
    {

        class MMapFile
        {
        public:
            MMapFile();
            explicit MMapFile(const int fd);    //explict避免只有一个参数的构造函数，使用=号就能构造
            MMapFile(const MMapOption & mmap_option,const int fd);
            
            ~MMapFile();    //析构函数
        
        public:
            //执行内存中的共享区和磁盘同步的操作；返回值为是否成功
            bool sync_file();

             //真正实现将文件映射到内存的函数，初始时设置访问权限不可写
            bool mmap_file(const bool write=false);

            //映射完后该读内存中的数据
            void *get_data() const;   //返回指向那块数据的内存的指针（还不知道是什么类型的数据，先写void）
                                    //加const是不让该函数内部修改成员变量

            //获取映射部分的内存大小
            int32_t get_size() const;

            //解除映射
            bool munmap_file();

            //重新再进行映射
            bool remmap_file();

        private:
            //重新调整文件大小
            bool reset_file_size(const int32_t size);

        private:
            //私有属性
            int32_t size;   //已经映射的大小
            int fd;
            void * data;    //指向数据的指针
            struct MMapOption mmap_option;  //有关内存大小的选项
        };
    }
}



#endif