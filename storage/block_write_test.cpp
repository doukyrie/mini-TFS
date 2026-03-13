//块初始化测试文件，对块进行初始化
#include "common.h"
#include "file_op.h"
#include "index_handle.h"
#include <iostream>

using namespace std;
using namespace qiniu;

const static largefile::MMapOption mmap_option = {1024*1024,4*1024,4*1024};
const static uint32_t main_block_size = 1024*1024*64;   //默认是64m大小
const static int32_t bucket_size = 1000;    //哈希桶长度默认设为1000
const static int32_t block_id = 1;  //块id，默认从1开始

//接收命令行参数
//如rm -f test.txt
//argc = 3（包括rm程序自己），argv[0]: rm , argv[1]: -f , argv[2]: test.txt
int main(int argc, char **argv) 
{
    int ret = 0;

    int32_t block_id;
    
    cout<<"请输入你的block_id："<<endl;
    cin>>block_id;

    if(block_id < 1)
    {
        cerr<<"invalid block_id"<<endl;
        return -1;
    }


    //生成索引文件路径
    //IndexHandle构造函数中，传入base_path后会自动生成索引文件路径，创建MMapFileOperation实例
    string index_base_path = ".";
    largefile::IndexHandle *index_handle = new largefile::IndexHandle(index_base_path,block_id);

    //加载索引文件
    ret = index_handle->load(block_id,bucket_size,mmap_option);
    if(ret < 0)
    {
        fprintf(stderr,"load index %d failed.\n",block_id);
        //delete main_block;
        delete index_handle;

        return ret;
    }

    //生成主块文件路径
    string main_block_path;

    stringstream temp_stream;
    temp_stream << "." << largefile::MAINBLOCK_DIR_PREFIX << block_id; //路径为当前目录下的mainblock文件夹，每个主块的名字为块id
    temp_stream >> main_block_path;

    //打开主块文件（如果没有则自动创建）
    largefile::FileOperation *main_block = new largefile::FileOperation(main_block_path,O_RDWR | O_LARGEFILE | O_CREAT);

    ret = main_block->ftruncate_file(main_block_size);  //修改文件大小，会自动创建并打开文件
    if(ret < 0)
    {
        fprintf(stderr,"main_block ftruncate_file failed. main_block_path: %s, reson: %s\n",(char *)&main_block_path,strerror(errno));
        delete main_block;
        index_handle->remove(block_id);
        return -1;
    }

    //要写的内容
    char buf[4096];
    sprintf(buf,"fuck you!fuck you!fuck you!");

    //将文件写入
    // int32_t offset = index_handle->get_block_data_offset();
    int32_t offset = index_handle->get_index_header()->data_file_offset;
    uint32_t file_no = index_handle->get_block_info()->seq_no;

    ret = main_block->pwrite_file(buf,strlen(buf),offset);
    if(ret < 0)
    {
        fprintf(stderr,"pwrite_file to main_block failed. ret = %d, res: %s\n",ret,strerror(errno));
        main_block->close_file();   //关闭打开的主块文件
        delete index_handle;
        delete main_block;

        return ret;
    }

    //索引文件中写入MetaInfo
    largefile::MetaInfo meta;
    meta.set_file_id(file_no);
    meta.set_offset(offset);
    meta.set_size(strlen(buf));

    ret = index_handle->write_segment_meta(meta.get_key(), meta);
    if(ret < 0)
    {
        return ret;
    }

    //写成功后更新信息（包括索引头部的信息和索引头部中的块信息）
    index_handle->set_index_header_offset(strlen(buf)); //更新块文件偏移
    ret = index_handle->update_block_info(largefile::C_OPER_INSERT,strlen(buf));  //更新块信息
    if(ret < 0)
    {
        printf("update_block_info failed.\n");
        return ret;
    }

    ret = index_handle->flush();
    if(ret < 0)
    {
        fprintf(stderr,"flush index_handle failed. block_id = %d, file_id = %u\n",block_id, file_no);
        return ret;
    }

    //关闭等操作
    main_block->close_file();   //关闭打开的主块文件
    delete index_handle;
    delete main_block;

    return 0;
}