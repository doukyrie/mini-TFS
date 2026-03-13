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
// int main()
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

    //创建索引文件（初次创建会直接加载到内存）
    ret = index_handle->create(block_id,bucket_size,mmap_option);
    if(ret < 0)
    {
        fprintf(stderr,"create index %d failed.\n",block_id);
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

    //关闭等操作
    main_block->close_file();   //关闭打开的主块文件
    index_handle->flush();   //同步索引文件数据
    delete index_handle;
    delete main_block;

    return 0;
}