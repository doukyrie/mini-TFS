#ifndef INDEX_HANDLE_H
#define INDEX_HANDLE_H

#include "common.h"
#include "mmap_file.h"
#include "file_op.h"
#include "mmap_file_op.h"

namespace qiniu
{
    namespace largefile
    {
        //索引头部
        struct IndexHeader
        {
            IndexHeader()
            {
                memset(this,0,sizeof(IndexHeader));
            }


            BlockInfo block_info;   //块信息
            int32_t bucket_size;    //哈希桶的数量
            int32_t data_file_offset;    //数据文件的偏移（已经写入的大小，也就是下一次可以写入的起始位置）
            int32_t index_file_size;    //metainfo的偏移（已经存放的metainfo的大小，也就是下一次可以写入metainfo的位置）
            int32_t free_head_offset;   //可重用地址：之前用过的某个块如果删掉了，则用一个地址保存，下次可以重用（即空闲链表节点的地址）
        };

        //索引操作
        class IndexHandle
        {
        public:
            IndexHandle(const std::string & base_path, const uint32_t block_id);
            ~IndexHandle();

            //获取索引文件中indexheader的数据
            IndexHeader * get_index_header();

            //获取索引头部中BlockInfo的数据
            BlockInfo * get_block_info();

            //获取哈希桶的数量
            int32_t get_bucket_size() const;

            //获取哈希桶的偏移（即位置）
            int32_t* get_bucket_slot() const;

            //设置索引头部中的文件偏移（用于文件写成功后的更新）
            void set_index_header_offset(int32_t offset);

            //更新索引头部中的块信息
            int update_block_info(const OperType oper_type, const int32_t modify_size); //参数：1.修改类型（新增文件or删除文件） 2.修改的文件的大小

            //创建索引文件，初始化，并映射到内存
            int create(const uint32_t logic_block_id, const int32_t bucket_size, const MMapOption & mmap_option);
            //参数：logic_block_id：和主块id是一个意思  bucket_size：哈希桶大小 mmap_option：用于构造MMapFile

            //加载索引文件
            int load(const uint32_t logic_block_id, const int32_t bucket_size, const MMapOption & mmap_option);

            //解除内存映射（munmap）并删除文件（unlink）
            int remove(const int32_t logic_block_id);

            //同步索引文件（内存 -> 磁盘）
            int flush();

            //写MetaInfo
            int write_segment_meta(const uint64_t key,MetaInfo & meta);

            //读metainfo
            int read_segment_meta(const uint64_t key,MetaInfo & meta);  //将metainfo读到传入的meta里

            //删除metainfo
            int delete_segment_meta(const uint64_t key);

            //在哈希链表中查找（传入要查找的文件编号即key，若存在则获取当前要查的key的偏移和前一个节点的偏移）
            int hash_find(const uint64_t key, int32_t& current_offset, int32_t& previous_offset);

            //将meta插入哈希链表中
            int hash_insert(const uint64_t key,const int32_t previous_offset, MetaInfo & meta);

        private:
            bool hash_compare(const uint64_t key,const uint64_t meta_key)
            {
                if(key == meta_key) return true;
                else return false;
            }

        private:
            MMapFileOperation *mmap_file_op;
            bool is_load;   //索引文件是否被加载
        };
    }
}





#endif