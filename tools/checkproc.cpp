/*
    checkproc.cpp
    守护程序：检查共享内存中进程的心跳，如果超时，则终止进程
    进程心跳是检查进程是否正常运行的一种方法
    在其它程序中，通过cpactive类操作进程心跳
    每隔一段时间，进程会向共享内存中写入心跳记录
    如果进程卡住，心跳记录不会被更新，就会被本程序终止
    另外，在cpactive类的析构函数中，会从共享内存中删除心跳记录
    如果进程异常退出，心跳记录可能会残留在共享内存中，需要由本程序清理
*/

#include "_public.h"

using namespace idc;

clogfile logfile;

int main(int argc, char* argv[]) 
{
    if (argc != 2)
    {
        cout << "\n\nUsing:checkproc logfilename\n"
                "Example:/MDC/bin/tools/procctl 10 /MDC/bin/tools/checkproc /MDC/log/tools/checkproc.log\n\n"
                
                "本程序用于检查后台服务程序是否超时，如果已超时，就终止它\n"
                "注意：\n"
                "  1）本程序由procctl启动，运行周期建议为10秒\n"
                "  2）为了避免被普通用户误杀，本程序应该用root用户启动\n"
                "  3）如果要停止本程序，只能用killall -9 终止\n\n";

        return -1;
    }

    // 关闭io和信号
    closeioandsignal(true);

    // 打开日志文件
    if (logfile.open(argv[1]) == false)
    {
        cout << "logfile.open(" << argv[1] << ") failed\n";
        return -1;
    }

    // 创建/获取共享内存
    // shmid：shared memory id，共享内存标识符
    // shmget()函数创建一个新的共享内存区段或获取一个已经存在的共享内存区段
    // (key_t)SHMKEYP：共享内存的键值
    // MAXNUMP*sizeof(struct st_procinfo)：共享内存的大小，MAXNUMP个st_procinfo
    // 0666：权限，允许创建者和同组用户读写，其他用户只读；IPC_CREAT：如果不存在则创建
    // st_procinfo是进程心跳的结构体，定义在_public.h中
    int shmid = 0;
    if ((shmid = shmget((key_t)SHMKEYP, MAXNUMP * sizeof(struct st_procinfo), 0666 | IPC_CREAT)) == -1)
    {
        logfile.write("[get shared memory failed] shmget(%d)\n",SHMKEYP);
        return false;
    }

    // 将共享内存连接到当前进程的地址空间
    // shmat()函数返回由shmid标识的共享内存的地址
    // shmid：共享内存标识符
    // 0：系统自动选择共享内存的地址
    // 0：SHM_RDONLY：只读，SHM_RND：随机
    struct st_procinfo *shm=(struct st_procinfo *)shmat(shmid, 0, 0);

    // 遍历共享内存中全部的记录，如果进程已超时，终止它
    for (int i = 0; i < MAXNUMP; ++i)
    {
        // pid==0表示空记录
        if (shm[i].pid == 0) continue;

        // 如果进程已经不存在了，共享内存中是残留的心跳信息。
        // 向进程发送信号0，判断它是否还存在，如果不存在，从共享内存中删除该记录
        if (kill(shm[i].pid, 0) == -1)
        {
            logfile.write("[process not exist] pid=%d(%s)\n",shm[i].pid, shm[i].pname);
            memset(&shm[i], 0, sizeof(struct st_procinfo));
            continue;
        }

        // 如果进程未超时
        if (time(NULL) - shm[i].atime < shm[i].timeout) continue;

        // 如果进程已超时，则终止它
        // 向pid发送信号以终止进程，但要注意如果pid=0，会终止本程序
        // 不能直接使用共享内存中的值，因为它随时可能被修改为0（进程退出）
        // 所以一定要先把进程的结构体备份出来
        struct st_procinfo tmp=shm[i];
		if (tmp.pid==0) continue;

        logfile.write("[process timeout] pid=%d(%s)\n",tmp.pid, tmp.pname);

        // 先尝试正常终止进程，即发送SIGTERM信号（15）
        kill(tmp.pid, SIGTERM);

        // 每隔1秒判断一次进程是否存在，累计5秒，一般来说，5秒的时间足够让进程退出
        int ret = 0;
        for (int j = 0; j < 5; ++j)
        {
            sleep(1);
            ret = kill(tmp.pid, 0);
            if (ret == -1) break;
        }

        // 如果进程正常终止
        if (ret == -1)
            logfile.write("[process terminated] pid=%d(%s)\n",tmp.pid, tmp.pname);
        else
        {
            // 如果进程未正常终止，则强制终止进程，即发送SIGKILL信号（9）
            kill(tmp.pid, SIGKILL);
            logfile.write("[process killed] pid=%d(%s)\n",tmp.pid, tmp.pname);
            memset(&shm[i], 0, sizeof(struct st_procinfo)); // 从共享内存中删除记录
        }
    }

    // 断开共享内存连接
    shmdt(shm);

    return 0;
}