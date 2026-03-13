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

    //读取索引文件的metainfo
    uint64_t file_id = 0;
    cout<<"请输入你的file_id："<<endl;
    cin>>file_id;

    if(file_id < 1)
    {
        cerr<<"invalid file_id"<<endl;
        return -1;
    }

    ret = index_handle->delete_segment_meta(file_id);
    if(ret < 0)
    {
        fprintf(stderr,"delete_segment_meta failed in test. file_id: %lu\n",file_id);
        return ret;
    }

    printf("delete block success! file_id: %lu\n",file_id);

    delete index_handle;

    return 0;
}