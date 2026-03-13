#include "common.h"
#include "index_handle.h"

namespace qiniu
{
    namespace largefile
    {
        IndexHandle::IndexHandle(const std::string & base_path, const uint32_t main_block_id)
        {
            //获取索引文件路径
            std::stringstream temp_stream;  //临时字符串流
            temp_stream << base_path << INDEX_DIR_PREFIX << main_block_id;   //例：传入的base_path为/root/kyrie，后面拼上/index/
            std::string index_path;
            temp_stream >> index_path;

            //创建MMapFileOperation实例
            this->mmap_file_op = new MMapFileOperation(index_path);
            this->is_load = false;
        }

        IndexHandle::~IndexHandle()
        {
            if(this->mmap_file_op != NULL)
            {
                delete this->mmap_file_op;
                this->mmap_file_op = NULL;
            }
        }

        //获取索引文件中indexheader的数据
        IndexHeader * IndexHandle::get_index_header()
        {
            //拿到这块内存的起始地址后，强制转成IndexHeader *类型就会直接拿到这块内存中的IndexHeader *数据（因为IndexHeader数据就在这块内存的起始位置）
            return reinterpret_cast<IndexHeader *>(this->mmap_file_op->get_map_data());
        }

        //获取索引头部中BlockInfo的数据
        BlockInfo * IndexHandle::get_block_info()
        {
            //拿到这块内存的起始地址后，强制转成BlockInfo *类型就会直接拿到这块内存中的BlockInfo *数据（因为BlockInfo数据就在这块内存的起始位置）
            return reinterpret_cast<BlockInfo *>(this->mmap_file_op->get_map_data());
        }

        //获取哈希桶的数量
        int32_t IndexHandle::get_bucket_size() const
        {
            //转成IndexHeader *后直接去拿该结构体里面的bucket_size
            return reinterpret_cast<IndexHeader *>(this->mmap_file_op->get_map_data())->bucket_size;
        }

        //获取哈希桶的偏移（即位置）
        int32_t* IndexHandle::get_bucket_slot() const
        {
            return reinterpret_cast<int32_t *>(reinterpret_cast<char *>(this->mmap_file_op->get_map_data())+sizeof(IndexHeader));
        }

        //设置索引头部中的文件偏移（用于文件写成功后的更新）
        void IndexHandle::set_index_header_offset(int32_t offset)
        {
            this->get_index_header()->data_file_offset += offset;
        }

        //更新索引头部中的块信息
        int IndexHandle::update_block_info(const OperType oper_type, const int32_t modify_size)
        {
            if(this->get_block_info()->block_id == 0)
            {
                return EXIT_BLOCKID_ZERO_ERROR;
            }

            if(oper_type == C_OPER_INSERT)
            {
                this->get_block_info()->version++;
                this->get_block_info()->file_count++;
                this->get_block_info()->seq_no++;
                this->get_block_info()->current_size += modify_size;
            }
            else if(oper_type == C_OPER_DELETE)
            {
                this->get_block_info()->version++;
                this->get_block_info()->file_count--;
                //this->get_block_info()->seq_no++;
                this->get_block_info()->current_size -= modify_size;
                this->get_block_info()->del_file_count++;
                this->get_block_info()->del_size += modify_size;
            }

            printf("IndexHandle::update_block_info success!\nupdate:// version: %d, file_count: %d, seq_no: %d, current_size: %d, del_file_count: %d, del_size: %d\n",
                    this->get_block_info()->version,this->get_block_info()->file_count,
                    this->get_block_info()->seq_no,this->get_block_info()->current_size,
                    this->get_block_info()->del_file_count, this->get_block_info()->del_size);
            return OP_SUCCESS;
        }

        //创建索引文件，初始化，并映射到内存
        int IndexHandle::create(const uint32_t logic_block_id, const int32_t bucket_size, const MMapOption & mmap_option)
        {
            int ret = 0;

            if(this->is_load)
            {
                return EXIT_INDEX_ALREADY_LOAD_ERROR;
            }

            int64_t file_size = this->mmap_file_op->get_file_size();
            if(file_size < 0)
            {
                return OP_ERROR;
            }
            else if(file_size == 0) //说明是个空文件，作初始化
            {
                //初始化索引头部
                IndexHeader index_header;
                index_header.block_info.block_id = logic_block_id;
                index_header.block_info.seq_no = 1; //下一个可用的文件编号从1开始
                index_header.bucket_size = bucket_size;
                
                //整个块的头部大小 + 用来存真正块的索引桶，之后才是真正的块
                index_header.index_file_size = sizeof(IndexHeader) + sizeof(int32_t)*bucket_size;

                //创建一块缓存，包含index header和total buckets
                //类型是char是因为写进文件的类型是char
                char * init_data = new char[index_header.index_file_size];  //创建一个大小为头部大小 + 所有哈希桶的内存，目的就是用来存放头部和桶
                memcpy(init_data,&index_header,sizeof(IndexHeader));    //先将index_header放进来
                memset(init_data+sizeof(IndexHeader),0,sizeof(int32_t)*bucket_size);    //再将哈希桶放进来，起始位置是init_data + IndexHeader大小

                //将这块缓存写进索引文件（放进块的头部）
                ret = this->mmap_file_op->pwrite_file(init_data,index_header.index_file_size,0);

                delete[] init_data;
                init_data = NULL;

                if(ret != OP_SUCCESS)
                {
                    fprintf(stderr,"index_handle create failed.res : %s\n",strerror(-ret));
                    return ret; //将错误信息返回，让main函数调用错误打印将-ret打印出来
                }


                ret = this->mmap_file_op->flush_file(); //将内存文件立即同步到磁盘，成功返回0，失败返回-1
                if(ret != OP_SUCCESS)
                {
                    fprintf(stderr,"index_handle failed: flush_file() failed.\n");
                    return ret;
                }
            }
            else    //索引文件本身就存在，导致create失败
            {
                fprintf(stderr,"index_handle failed: index file is existed.\n");
                return EXIT_META_UNEXPECT_FOUND_ERROR;
            }

            //将创建好的索引文件映射到内存
            ret = this->mmap_file_op->mmap_file(mmap_option);
            if(ret != OP_SUCCESS)
            {
                fprintf(stderr,"index_handle failed: mmap_file() failed.\n");
                return ret;
            }

            //映射成功后说明索引文件已经加载
            this->is_load = true;
            printf("IndexHandle::create success!\n");
            return OP_SUCCESS;
        }

        //加载索引文件
        int IndexHandle::load(const uint32_t logic_block_id, const int32_t bucket_size, const MMapOption & mmap_option)
        {
            int ret = 0;

            if(this->is_load)
            {
                printf("index load failed. index file is already loaded.\n");
                return EXIT_INDEX_ALREADY_LOAD_ERROR;
            }

            int64_t file_size = this->mmap_file_op->get_file_size();
            if(file_size < 0)
            {
                return file_size;   //打开失败会返回-1，且get_file_size内部会打印错误信息
            }
            else if(file_size == 0) //说明是个空文件，不合理，应该先create
            {
                printf("index file load failed. file_size = 0, index file is empty.\n");
                return EXIT_INDEX_DESTROY_ERROR;
            }

            MMapOption temp_mmap_option = mmap_option;  //因为传进来的mmap_option是const类型，因此用一个临时变量接收，以便修改其中的参数

            //如果当前要加载的索引文件的大小大于初次要映射的内存大小，且小于最大映射大小时，将初次映射的内存大小改为file_size，使得文件能够映射
            if(file_size > temp_mmap_option.first_mmap_size && file_size <= temp_mmap_option.max_mmap_size)
            {
                temp_mmap_option.first_mmap_size = file_size;
            }
            //如果文件超过了max_size，则在mmap_file_op中的pread和pwrite都定义了不在内存映射中读，而是直接读磁盘

            ret = this->mmap_file_op->mmap_file(temp_mmap_option);
            if(ret < 0)
            {
                return ret;
            }

            //因为在create的时候已经给block_id和bucket_size赋值了，因此如果此时还为0则文件异常
            if(this->get_block_info()->block_id == 0 || this->get_bucket_size() == 0)
            {
                fprintf(stderr,"load index file failed. block_id and bucket_size = 0, file destroyed.\n");
                return EXIT_INDEX_DESTROY_ERROR;
            }

            //检查文件大小，不能小于IndexHeader和哈希桶加起来的大小
            if(file_size < sizeof(IndexHeader) + bucket_size*sizeof(int32_t))
            {
                fprintf(stderr,"failed in IndexHandle::load(). file_size < sizeof(IndexHeader) + bucket_size*sizeof(int32_t), file_size: %ld, index_file_size: %ld\n",
                        file_size,sizeof(IndexHeader) + bucket_size*sizeof(int32_t));

                return EXIT_INDEX_DESTROY_ERROR;
            }

            //检查传入的块id和索引文件中的是否相等
            if(logic_block_id != this->get_block_info()->block_id)
            {
                fprintf(stderr,"failed in IndexHandle::load(). logic_block_id != this->get_block_info()->block_id, logic_block_id: %d, this->get_block_info()->block_id: %d\n",
                        logic_block_id,this->get_block_info()->block_id);

                return EXIT_BLOCKID_CONFLICT_ERROR;
            }

            //检查传入的bucket_size和索引文件中的是否相等
            if(bucket_size != this->get_bucket_size())
            {
                fprintf(stderr,"failed in IndexHandle::load(). bucket_size != this->get_bucket_size(), bucket_size: %d, this->get_bucket_size(): %d\n",
                        bucket_size,this->get_bucket_size());

                return EXIT_BUCKET_CONFLICT_ERROR;
            }

            this->is_load = true;
            printf("index file load success!\n");

            return OP_SUCCESS;
        }

        //解除内存映射（munmap）并删除文件（unlink）
        int IndexHandle::remove(const int32_t logic_block_id)
        {
            if(!this->is_load)
            {
                printf("IndexHandle::remove : no need to remove. isn't load.\n");
                return OP_SUCCESS;
            }
            if(logic_block_id != this->get_block_info()->block_id)
            {
                fprintf(stderr,"IndexHandle::remove failed. logic_block_id conflict. logic_block_id: %d, index block_id: %d",logic_block_id , this->get_block_info()->block_id);
                return EXIT_BLOCKID_CONFLICT_ERROR;
            }

            int ret = this->mmap_file_op->munmap_file();
            if(ret < 0)
            {
                return ret;
            }
            printf("IndexHandle::remove : munmap_file success!\n");

            ret = this->mmap_file_op->unlink_file();
            if(ret < 0)
            {
                fprintf(stderr,"IndexHandle::remove unlink_file failed.\n");
                return ret;
            }

            this->is_load = false;

            return OP_SUCCESS;
        }

        //同步索引文件（内存 -> 磁盘）
        int IndexHandle::flush()
        {
            int ret = this->mmap_file_op->flush_file();
            if(ret < 0) fprintf(stderr,"IndexHandle::flush failed, ret: %d, des: %s",ret, strerror(errno));
            return ret;
        }

        //写MetaInfo
        int IndexHandle::write_segment_meta(const uint64_t key,MetaInfo & meta)
        {
            //从文件哈希表中查找key是否存在，hash_find()
            //不存在就写入meta到文件哈希表中 hash_insert()，存在则返回报错（因为此时是创建了新块，然后将块信息写到哈希里，因此不希望它存在）

            int ret = OP_SUCCESS;

            int current_offset = 0;
            int previous_offset = 0;

            ret = this->hash_find(key,current_offset,previous_offset);
            if(ret == OP_SUCCESS)   //存在（并不希望存在）
            {
                fprintf(stderr,"IndexHandle::write_segment_meta failed. meta existed.\n");
                return EXIT_META_UNEXPECT_FOUND_ERROR;
            }
            else if(ret != EXIT_META_NOT_FOUND_ERROR)   //出现不存在以外的其他错误
            {
                fprintf(stderr,"IndexHandle::write_segment_meta failed. other error: %s\n",strerror(errno));
                return ret;
            }

            //不存在就将meta写入文件哈希表
            ret = hash_insert(key,previous_offset,meta);
            if(ret < 0)
            {
                fprintf(stderr,"IndexHandle::write_segment_meta failed. hash_insert failed.\n");
                return ret;
            }

            return ret;
        }

        //读metainfo
        int IndexHandle::read_segment_meta(const uint64_t key,MetaInfo & meta)
        {
            int ret = OP_SUCCESS;

            int32_t current_offset = 0;
            int32_t previous_offset = 0;

            ret = hash_find(key,current_offset,previous_offset);
            if(ret < 0)
            {
                fprintf(stderr,"IndexHandle::read_segment_meta failed. hash_find failed. no found meta.\n");
                return ret;
            }

            ret = this->mmap_file_op->pread_file(reinterpret_cast<char *>(&meta), sizeof(MetaInfo), current_offset);
            if(ret < 0)
            {
                fprintf(stderr,"IndexHandle::read_segment_meta failed. pread_file failed. res: %s\n",strerror(errno));
                return ret;
            }

            return ret;
        }

        //删除metainfo
        int IndexHandle::delete_segment_meta(const uint64_t key)
        {
            int ret = OP_SUCCESS;

            int32_t current_offset = 0;
            int32_t previous_offset = 0;

            ret = hash_find(key,current_offset,previous_offset);
            if(ret < 0)
            {
                fprintf(stderr,"IndexHandle::delete_segment_meta failed. hash_find failed. no found meta.\n");
                return ret;
            }

            MetaInfo meta_info;
            //先把当前节点取出来
            ret = this->mmap_file_op->pread_file(reinterpret_cast<char *>(&meta_info),sizeof(MetaInfo),current_offset);
            if(ret < 0)
            {
                fprintf(stderr,"delete_segment_meta failed, pread_file(current_offset) failed.\n");
                return ret;
            }

            int32_t next_pos = meta_info.get_next_meta();
            //说明要删的节点是首节点，没有前节点，则哈希桶中存的则是待删节点的下一个节点的偏移（哈希桶存的是在该桶中的第一个节点的偏移，此时待删节点的下一个节点成为首节点）
            if(previous_offset == 0)
            {
                int32_t slot = static_cast<uint32_t>(key)%this->get_bucket_size();
                this->get_bucket_slot()[slot] = next_pos;
            }
            else
            {
                MetaInfo pre_meta_info;
                ret = this->mmap_file_op->pread_file(reinterpret_cast<char *>(&pre_meta_info),sizeof(MetaInfo),previous_offset);
                if(ret < 0)
                {
                    fprintf(stderr,"delete_segment_meta failed, pread_file(previous_offset) failed.\n");
                    return ret;
                }

                pre_meta_info.set_next_meta(next_pos);

                ret = this->mmap_file_op->pwrite_file(reinterpret_cast<char *>(&pre_meta_info),sizeof(MetaInfo),previous_offset);
                if(ret < 0)
                {
                    fprintf(stderr,"delete_segment_meta failed, pwrite_file(previous_offset) failed.\n");
                    return ret;
                }
            }

            //更新块信息
            ret = update_block_info(C_OPER_DELETE,meta_info.get_size());
            if(ret < 0)
            {
                fprintf(stderr,"in delete_segment_meta, update_block_info failed.\n");
                return ret;
            }

            //更新索引头部可重用节点（头插法）
            meta_info.set_next_meta(this->get_index_header()->free_head_offset);
            this->get_index_header()->free_head_offset = current_offset;
            ret = this->mmap_file_op->pwrite_file(reinterpret_cast<char *>(&meta_info),sizeof(MetaInfo),current_offset);
            if(ret < 0)
            {
                fprintf(stderr,"delete_segment_meta failed, pwrite_file(current_offset) failed.\n");
                return ret;
            }

            return OP_SUCCESS;
        }

        //在哈希链表中查找（传入要查找的文件编号即key，若存在则获取当前要查的key的偏移和前一个节点的偏移）
        int IndexHandle::hash_find(const uint64_t key, int32_t& current_offset, int32_t& previous_offset)
        {
            int ret = 0;
            MetaInfo meta_info;

            current_offset = 0;
            previous_offset = 0;

            //确定key存放在哪个桶（slot）
            int32_t slot = static_cast<uint32_t>(key)%(this->get_bucket_size());

            //读取该桶的第一个节点的偏移量
            int32_t pos = get_bucket_slot()[slot];

            //根据偏移量获取到metainfo节点，判断该节点存放的file_id和要查的key是否相等，若不想等，则根据next_meta再找链表下一个节点，以此类推直到找到key
            //若next_meta即下一个节点的偏移为0，说明没有下一个节点，也就说明key不存在，返回EXIT_META_NOT_FOUND_ERROR
            while(pos!=0)
            {
                ret = this->mmap_file_op->pread_file(reinterpret_cast<char *>(&meta_info),sizeof(MetaInfo),pos);
                if(ret < 0)
                {
                    return ret;
                }

                if(hash_compare(key,meta_info.get_key()))
                {
                    current_offset = pos;
                    return OP_SUCCESS;
                }

                previous_offset = pos;
                pos = meta_info.get_next_meta();
            }

            return EXIT_META_NOT_FOUND_ERROR;
        }

        //将meta插入哈希链表中
        int IndexHandle::hash_insert(const uint64_t key,const int32_t previous_offset, MetaInfo & meta)
        {
            int ret = OP_SUCCESS;

            //先计算出在哪个桶
            int32_t slot = static_cast<uint32_t>(key)%this->get_bucket_size();

            //确定meta要存放的位置
            int32_t current_offset;
            if(this->get_index_header()->free_head_offset != 0) //若有删除后待重用节点，优先使用
            {
                current_offset = this->get_index_header()->free_head_offset;
                MetaInfo temp_meta_info;
                //先读出空节点
                ret = this->mmap_file_op->pread_file(reinterpret_cast<char *>(&temp_meta_info),sizeof(MetaInfo),current_offset);
                if(ret < 0)
                {
                    fprintf(stderr,"hash_insert failed. read free meta failed.\n");
                    return ret;
                }

                this->get_index_header()->free_head_offset = temp_meta_info.get_next_meta();
                temp_meta_info.set_next_meta(0);

                printf("hash_insert: reuse free meta!\n");
            }
            else
            {
                current_offset = this->get_index_header()->index_file_size;
                //因为插入了新数据，因此更新偏移量
                this->get_index_header()->index_file_size += sizeof(MetaInfo);
                printf("hash_insert: use new meta!\n");
            }

            //将meta节点写入索引文件
            meta.set_next_meta(0);  //将指向下一节点的数据置0，表示这是尾节点
            
            ret = this->mmap_file_op->pwrite_file(reinterpret_cast<const char *>(&meta), sizeof(MetaInfo),current_offset);
            if(ret < 0)
            {
                this->get_index_header()->index_file_size -= sizeof(MetaInfo);
                fprintf(stderr,"hash_insert failed. write meta to index file failed. res: %s\n",strerror(errno));
                return ret;
            }

            //修改前置节点（如果存在），否则直接将current_offset写入哈希桶（因为当前节点是桶的首节点）
            if(previous_offset != 0)
            {
                MetaInfo temp_meta_info;
                ret = this->mmap_file_op->pread_file(reinterpret_cast<char *>(&temp_meta_info),sizeof(MetaInfo),previous_offset);
                if(ret < 0)
                {
                    fprintf(stderr,"hash insert failed. read previous meta failed. res: %s",strerror(errno));
                    //前置节点读失败则插入失败，偏移量回退，之前已经写入的meta也不必删除，下一次插入直接覆盖就行
                    this->get_index_header()->index_file_size -= sizeof(MetaInfo);
                    return ret;
                }

                temp_meta_info.set_next_meta(current_offset);
                ret = this->mmap_file_op->pwrite_file(reinterpret_cast<const char *>(&temp_meta_info),sizeof(MetaInfo),previous_offset);
                if(ret < 0)
                {
                    fprintf(stderr,"hash insert failed. after modifying previous meta, write back failed. res: %s\n",strerror(errno));
                    this->get_index_header()->index_file_size -= sizeof(MetaInfo);
                    return ret;
                }
            }
            else
            {
                this->get_bucket_slot()[slot] = current_offset;
            }

            return OP_SUCCESS;
        }
    }
}