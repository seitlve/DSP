/*
    gzipfiles.cpp
    压缩文件的程序
    由调度程序定期执行，用以压缩指定天数之前的文件
*/

#include "_public.h"

using namespace idc;

cpactive pactive; // 进程心跳

void EXIT(int sig); // 程序的退出函数

int main(int argc, char* argv[])
{
    if (argc != 4)
    {
        cout << "\n\nUsing:gzipfiles pathname matchstr timeout\n\n"
                "Example:\n"
             // 在R"()"里面的写字符串，特殊符号不需要转移转义，同时转义字符如\n也不会生效
                R"(      /MDC/bin/tools/gzipfiles /log/idc "*.log.20*" 0.02)"
                "\n      /MDC/bin/tools/gzipfiles /tmp/idc/surfdata \"*.xml,*.json\" 0.01\n\n"
                  
                "这是一个工具程序，用于压缩历史的数据文件或日志文件\n"
                "本程序把pathname目录及子目录中timeout天之前的匹配matchstr并且未被压缩的文件全部压缩，timeout可以是小数\n"
                "本程序调用/usr/bin/gzip命令压缩文件，压缩后的文件存放在原目录中\n"
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
    cdir dir;
    if (dir.opendir(argv[1], argv[2], 10000, true, false) == false)
    {
        printf("[open directory failed] dir.opendir(%s, %s)\n", argv[1], argv[2]);
    }

    // 遍历目的中的文件，如果是历史数据文件，删除它
    while (dir.readdir())
    {
        // 如果文件是历史数据文件，并且不是压缩文件
        if ((dir.m_mtime < timeout) && (matchstr(dir.m_filename, "*.gz") == false))
        {
            // 压缩文件，调用操作系统的gzip命令
            // 1>/dev/null 2>/dev/null 表示将执行命令后的输出重定向到空，即终端不显示命令执行后的信息
            string strcmd="/usr/bin/gzip -f " + dir.m_ffilename + " 1>/dev/null 2>/dev/null";
            if (system(strcmd.c_str()) == 0)
                printf("gzip %s success\n", dir.m_ffilename.c_str());
            else
                printf("gzip %s failed\n", dir.m_ffilename.c_str());

            // 如果压缩的文件比较大，可能会很耗费时间，需要更新心跳
            pactive.uptatime();
        }
    }

    return 0;
}

void EXIT(int sig)
{
    cout << "process exit, sig=" << sig << endl;

    exit(0);
}