/*
    deletefiles.cpp
    删除文件的程序
    由调度程序定期执行，用以删除指定天数之前的文件
*/

#include "_public.h"

using namespace idc;

cpactive pactive; // 进程心跳

void EXIT(int sig); // 程序的退出函数

int main(int argc, char* argv[])
{
    if (argc != 4)
    {
        cout << "\n\nUsing:deletefiles pathname matchstr timeout\n\n"
                "Example:\n"
                // 在R"()"里面的写字符串，特殊符号不需要转移转义，同时转义字符如\n也不会生效
                R"(      /MDC/bin/tools/deletefiles /log/idc "*.log.20*" 0.02)"
                "\n      /MDC/bin/tools/deletefiles /tmp/idc/surfdata \"*.xml,*.json\" 0.01\n\n"

                "这是一个工具程序，用于删除历史的数据文件或日志文件\n"
                "本程序把pathname目录及子目录中timeout天之前的匹配matchstr文件全部删除，timeout可以是小数\n"
                "本程序不写日志文件，也不会在控制台输出任何信息\n\n";              

        return -1;
	}

    // 关闭io和信号，设置信号处理函数
    closeioandsignal(true);
    signal(SIGINT, EXIT);
    signal(SIGTERM, EXIT);

    // 配置进程心跳
    pactive.addpinfo(30, "deletefiles");

    // 获取被定义为历史数据文件的时间点
    string timeout = ltime1("yyyymmddhh24miss", 0 - (int)(atof(argv[2]) * 24 * 60 * 60));

    // 打开目录
    // 每次最多删除10000个文件，因此不需要更新心跳
    cdir dir;
    if (dir.opendir(argv[1], argv[2], 10000, true, false) == false)
    {
        printf("[open directory failed] dir.opendir(%s, %s)\n", argv[1], argv[2]);
    }

    // 遍历目的中的文件，如果是历史数据文件，删除它
    while (dir.readdir())
    {
        // m_mtime是dir当前读取的文件的修改时间
        if (dir.m_mtime < timeout)
        {
            if (remove(dir.m_ffilename.c_str()) == 0)
                printf("remove %s success\n", dir.m_ffilename.c_str());
            else
                printf("remove %s failed\n", dir.m_ffilename.c_str());
        }
    }

    return 0;
}

void EXIT(int sig)
{
    cout << "process exit, sig=" << sig << endl;

    exit(0);
}