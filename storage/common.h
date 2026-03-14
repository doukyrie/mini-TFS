#ifndef COMMON_H
#define COMMON_H

#include<unistd.h>
#include<sys/mman.h>
#include<cerrno>
#include<cstring>
#include <sys/stat.h>
#include <aio.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string>
#include <stdio.h>
#include <assert.h>
#include <sstream>

namespace qiniu
{
    namespace largefile
    {
        const int32_t OP_SUCCESS = 0; //操作成功
        const int32_t OP_ERROR = -1;   //操作失败
        const int32_t EXIT_DISK_OP_INCOMPLETE = -8012;  //读写长度小于要求的
        const int32_t EXIT_INDEX_ALREADY_LOAD_ERROR = -8011;    //在IndexHandle中，索引文件已经加载
        const int32_t EXIT_META_UNEXPECT_FOUND_ERROR = -8010;  //存在非预期的索引文件导致create失败
        const int32_t EXIT_INDEX_DESTROY_ERROR = -8009; //索引文件损坏
        const int32_t EXIT_BLOCKID_CONFLICT_ERROR = -8008;  //块id不相等（IndexHandle的load方法中）
        const int32_t EXIT_BUCKET_CONFLICT_ERROR = -8008;  //bucket_size不相等（IndexHandle的load方法中）
        const int32_t EXIT_META_NOT_FOUND_ERROR = -8007;    //查找哈希桶中的metainfo，未查找到
        const int32_t EXIT_BLOCKID_ZERO_ERROR = -8006;  //更新块信息时的块id为0的错误

        static const std::string MAINBLOCK_DIR_PREFIX = "/mainblock/"; //主块文件路径
        static const std::string INDEX_DIR_PREFIX = "/index/";   //索引文件路径
        static const mode_t DIR_MODE = 0755;    //目录权限


        enum OperType   //更新块信息时的操作类型
        {
            C_OPER_INSERT = 1,
            C_OPER_DELETE = 2
        };


        //内存映射选项
        struct MMapOption
        {
            int32_t max_mmap_size;  //设置内存映射的最大大小，int32_t表示不管系统是32位还是64位，统一用32位整数
            int32_t first_mmap_size;    //第一次分配的大小
            int32_t per_mmap_size;  //每次增加的大小
        };

        //块信息
        struct BlockInfo
        {
            uint32_t block_id;  //块信息
            int32_t version;    //块当前版本号（只要对文件进行一次修改就更新版本号）
            int32_t file_count; //当前已保存的文件数量
            int32_t current_size;     //当前已保存文件数据大小
            int32_t del_file_count; //已删除的文件数量
            int32_t del_size;   //已删除的文件数据总大小
            uint32_t seq_no;    //下一个可分配的文件编号

            //结构体的构造函数
            //结构体和类的区别就是权限，结构体都是public，且不用显式写出来
            BlockInfo()
            {
                //方便起见，不将每个字段都设为0，直接将整个结构体置0，相当于所有字段都是0
                memset(this,0,sizeof(BlockInfo));   //this：设置的对象为当前结构体（起始地址）  0：将所有内存都置为0    sizeof：设置的大小为整个结构体
            }

            inline bool operator==(const BlockInfo &rhs) const
            {
                return this->block_id == rhs.block_id && 
                        this->version == rhs.version && 
                        this->file_count == rhs.file_count && 
                        this->current_size == rhs.current_size &&
                        this->del_file_count == rhs.del_file_count &&
                        this->del_size == rhs.del_size &&
                        this->seq_no == rhs.seq_no;
            }
        };

        //文件哈希索引块
        struct MetaInfo
        {
        private:
            uint64_t file_id;    //文件编号

            struct
            {
                int32_t offset; //文件所在的偏移量
                int32_t size;   //文件大小
            }location;

            int32_t next_meta;  //下一个块的哈希地址
            uint32_t crc32;     //数据校验值

        public:
            //无参构造
            MetaInfo()
            {
                init();
            }

            //有参构造
            MetaInfo(const uint64_t file_id, const int32_t offset, const int32_t size, const int32_t next_meta)
            {
                this->file_id = file_id;
                this->location.offset = offset;
                this->location.size = size;
                this->next_meta = next_meta;
            }

            //拷贝构造
            MetaInfo(const MetaInfo &meta_info)
            {
                memcpy(this,&meta_info,sizeof(meta_info));
            }

            //重载=运算符，避免浅拷贝
            MetaInfo& operator=(const MetaInfo& meta_info)
            {
                //实现A=B=C这种连续赋值
                if(this==&meta_info)
                {
                    return *this;
                }

                this->file_id = meta_info.get_file_id();
                this->location.offset = meta_info.get_offset();
                this->location.size = meta_info.get_size();
                this->next_meta = meta_info.get_next_meta();
                this->crc32 = meta_info.get_crc32();

                return *this;
            }

            //克隆，将对方的属性全部克隆过来给自己（类似=）
            MetaInfo& clone(const MetaInfo& meta_info)
            {
                //断言this不和传进来的meta_info相等，否则抛出异常
                assert(this!=&meta_info);

                this->file_id = meta_info.get_file_id();
                this->location.offset = meta_info.get_offset();
                this->location.size = meta_info.get_size();
                this->next_meta = meta_info.get_next_meta();
                this->crc32 = meta_info.get_crc32();

                return *this;
            }

            //重载==，判断两个MetaInfo是否相等
            bool operator==(const MetaInfo& rhs) const
            {
                return this->file_id == rhs.get_file_id() &&
                        this->location.offset == rhs.get_offset() &&
                        this->location.size == rhs.get_size() &&
                        this->next_meta == rhs.get_next_meta();
            }

            //get、set方法
            uint64_t get_key() const
            {
                return this->file_id;
            }

            void set_key(const uint64_t key)
            {
                this->file_id = key;
            }

            uint64_t get_file_id() const
            {
                return this->file_id;
            }

            void set_file_id(const uint64_t file_id)
            {
                this->file_id = file_id;
            }

            int32_t get_offset() const
            {
                return this->location.offset;
            }

            void set_offset(int32_t offset)
            {
                this->location.offset = offset;
            }

            int32_t get_size() const
            {
                return this->location.size;
            }

            void set_size(int32_t size)
            {
                this->location.size = size;
            }

            int32_t get_next_meta() const
            {
                return this->next_meta;
            }

            void set_next_meta(int32_t next_meta)
            {
                this->next_meta = next_meta;
            }

            uint32_t get_crc32() const { return this->crc32; }
            void set_crc32(uint32_t v) { this->crc32 = v; }


        private:
            void init()
            {
                this->file_id = 0;
                this->location.offset = 0;
                this->location.size = 0;
                this->next_meta = 0;
                this->crc32 = 0;
            }
        };
    }
}


#endif