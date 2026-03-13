#include "mmap_file.h"

static int debug = 1;   //设置是否为调试模式


namespace qiniu
{
    namespace largefile
    {
    //类内函数实现，用类名加::表示作用域，实现这个类里面的函数

        //构造和析构
        MMapFile::MMapFile():
        size(0),fd(-1),data(NULL){} //初始化列表

        MMapFile::MMapFile(const int fd):
        size(0),fd(fd),data(NULL){}

        MMapFile::MMapFile(const MMapOption & mmap_option,const int fd):
        size(0),fd(fd),data(NULL)
        {
            this->mmap_option.first_mmap_size = mmap_option.first_mmap_size;
            this->mmap_option.max_mmap_size = mmap_option.max_mmap_size;
            this->mmap_option.per_mmap_size = mmap_option.per_mmap_size;
        }

        MMapFile::~MMapFile()
        {
            if(this->data)
            {
                //打印相关信息
                if(debug) 
                {
                    printf("(~MMapFile)mmap file destruct, fd: %d, data_size: %d, data:%p\n",this->fd,this->size,this->data);
                }

                //析构前，先将数据同步到磁盘中
                msync(this->data,this->size,MS_SYNC);   //使用MS_SYNC同步方式，一直阻塞直到同步完成，防止析构紊乱

                //同步后解除映射
                this->munmap_file();
                
                //将数据初始化
                this->size = 0;
                this->data = NULL;
                fd = -1;
                this->mmap_option.first_mmap_size = 0;
                this->mmap_option.max_mmap_size = 0;
                this->mmap_option.per_mmap_size = 0;
            }
        }


        //将内存同步到文件
        bool MMapFile::sync_file()
        {
            if(this->data && this->size>0)
            {
                return msync(this->data,this->size,MS_SYNC)==0; //使用异步的同步方式，效率高
            }

            return true;
        }


        //内存映射
        bool MMapFile::mmap_file(const bool write)
        {
            //默认只读，write为true则读写
            int flags = PROT_READ;
            if(write)
            {
                flags = PROT_READ | PROT_WRITE;
            }

            //如果文件描述符没指定，也失败
            if(fd<0)
            {
                fprintf(stderr,"mmap_file failed, fd<0\n");
                return false;
            }
            
            //调整内存大小
            if(this->size < this->mmap_option.max_mmap_size)
            {
                this->size = this->mmap_option.first_mmap_size;
            }
            else
            {
                this->size = this->mmap_option.max_mmap_size;
            }

            //调整文件大小，使得和内存大小一样
            if(!reset_file_size(this->size))
            {
                fprintf(stderr,"reset_file_size failed in mmap_file: %s",strerror(errno));
                return false;
            }

            this->data = mmap(NULL,this->size,flags,MAP_SHARED,this->fd,0);
            if(this->data == MAP_FAILED)
            {
                fprintf(stderr,"mmap_file failed: %s\n",strerror(errno));

                this->size = 0;
                this->data = NULL;
                this->fd = -1;
                return false;
            }

            if(debug) printf("mmap file success.data: %p, fd: %d, size: %d\n",this->data,this->fd,this->size);
            return true;
        }


        //获取data
        void * MMapFile::get_data() const
        {
            return this->data;
        }


        //获取size
        int32_t MMapFile::get_size() const
        {
            return this->size;
        }


        //解除映射
        bool MMapFile::munmap_file()
        {
            //if(this->data==NULL) return true;
            if(munmap(this->data,this->size)==-1)
            {
                fprintf(stderr,"munmap file failed: %s\n",strerror(errno));
                return false;
            }

            if(debug) printf("munmap success\n");
            return true;
        }


        //重新分配（追加）内存并映射
        bool MMapFile::remmap_file()
        {
            if(this->fd<0 || this->data ==NULL)
            {
                fprintf(stderr,"fd <0 or data == NULL\n");
                return false;
            }

            if(this->size == this->mmap_option.max_mmap_size)
            {
                fprintf(stderr,"size already max\n");
                return false;
            }

            //调整内存大小
            int32_t new_size = this->size + this->mmap_option.per_mmap_size;
            if(new_size > this->mmap_option.max_mmap_size)  //当增加的内存超过了max，则设为max
            {
                new_size = this->mmap_option.max_mmap_size;
            }

            //内存扩容后将文件扩容
            if(!reset_file_size(new_size))
            {
                fprintf(stderr,"reset_file_size failed in remmap_file\n");
                return false;
            }


            //开始重新映射
            printf("mremap start: fd: %d, new_size: %d, old data: %p, old size: %d\n",this->fd,new_size,this->data, this->size);

            void * new_data = mremap(this->data,this->size,new_size,MREMAP_MAYMOVE);
            if(new_data == MAP_FAILED)
            {
                fprintf(stderr,"mremap failed: %s\n",strerror(errno));
                return false;
            }

            printf("mremap success, fd: %d, new_size: %d, new_data: %p\n",this->fd,new_size,new_data);

            this->data = new_data;
            this->size = new_size;
            
            return true;
        }


        //重新设置文件大小
        bool MMapFile::reset_file_size(const int32_t size)
        {
            struct stat s;  //存放文件状态
            if(fstat(this->fd,&s)<0)  //获取fd的文件状态，存在s里
            {
                fprintf(stderr,"fstat failed: %s\n",strerror(errno));
                return false;
            }

            if(s.st_size < size)  //文件比内存的空间小，则将文件扩容
            {  
                if(ftruncate(this->fd,size)<0)  //将文件扩大到和当前内存空间一样大
                {
                    fprintf(stderr,"ftruncate failed: %s",strerror(errno));
                    return false;
                }
            }

            return true;
        }
    }
}