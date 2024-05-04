/*
    procctl.cpp
    本程序是系统程序的调度程序，周期性启动系统程序或shell脚本
*/

#include <iostream>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>

using namespace std;

int main(int argc, char* argv[])
{
    if (argc<3)
    {
        cout << "Using:procctl <time> <program> <parameters>\n"
             << "Example:/DSP/bin/tools/procctl 10 ls -l\n\n"

             << "本程序是系统程序的调度程序，周期性启动系统程序或shell脚本\n"
             << "参数说明：\n"
             << "time：运行周期，单位为秒\n"  
             << "      在被调度的程序执行结束后，等待time秒再次启动\n"
             << "      如果是周期执行的程序，调度程序每隔time秒启动一次\n"
             << "      如果是常驻内存的程序，调度程序负责在程序异常终止后重启\n"
             << "program：要启动的程序或shell脚本，必须使用绝对路径\n"
             << "parameters：程序的参数\n"
             << "注意，本程序不会被kill杀死，但可以用kill -9强行杀死\n\n\n";

        return -1;
    }

    // 关闭io和信号
    close(0); close(1); close(2);
    for (int i = 0; i < 63; ++i)
        signal(i, SIG_IGN);
    
    // 生成子进程，父进程退出，让系统接管子进程，目的是不影响shell终端
    if (fork() != 0) return 0;

    // 把子进程退出的信号SIGCHLD恢复为默认行为，让父进程可以调用wait()函数等待子进程退出
    // 父进程必须处理子进程退出的信号，否则子进程退出后会变成僵尸进程
    // 注意，父子进程是相对的概念，这里的父进程指的是上面fork出来的子进程
    // 子进程是下面fork出来执行execv()函数的进程
    signal(SIGCHLD, SIG_DFL);

    // 存放程序名和参数
    char* pargs[argc];
    for (int i = 2; i < argc; ++i)
        pargs[i - 2] = argv[i];
    pargs[argc - 2] = NULL;

    // 循环执行程序
    while (true)
    {
        // 生成子进程，执行程序
        if (fork() == 0)
        {
            // 执行程序
            execv(argv[2], pargs);
            return 0; // 如果execv()执行失败，子进程退出
        }

        wait(NULL); // 等待子进程退出
        sleep(atoi(argv[1])); // 等待time秒
    }
}