/*
    fileserver.cpp
    tcp文件传输的服务端

    文件传输流程：
    1.客户端连接服务端并发送登录报文（包含客户端类型等信息）
    2.发送端执行发送文件的任务
        a)如果成功发送了文件，则继续发送
        b)如果没有文件，进入休眠状态，即每隔timetvl执行一次发送任务
          同时，为了保持与对端的联系，发送一次心跳报文
    3.发送文件的流程
        a)发送端先发送文件信息（文件名、大小、修改时间）
        b)发送端发送文件（接收端根据文件名和大小接收文件，接收完后再设置文件修改时间）
        c)接收端收到文件后返回确认报文（文件名+接收结果），发送端接收确认报文
          如果接收失败，接收端不退出，接收结果为false，发送端不处理文件（删除、备份）
          如果发送端断开连接，接收端发送确保报文时会失败，届时再退出
*/

#include "_public.h"

using namespace idc;

// 程序运行的参数
struct st_arg
{
    int clienttype;     // 客户端类型，1-下载，2-上传
    int ptype;
    char srvpath[256];
    char srvpathbak[256]; 
    bool andchild;
    char matchname[256]; 
    char clientpath[256];  
    int timetvl;
    int timeout;
    char pname[64];
}starg;

clogfile logfile;       // 日志
cpactive pactive;       // 进程心跳
ctcpserver tcpserver;   // tcp客户端

string sendbuffer;      // 发送报文
string recvbuffer;      // 接收报文

bool clientlogin();     // 处理登录客户端的登录报文
bool activetest();      // 发送心跳报文的函数

void sendfilesmain();   // 发送文件的主函数
bool _sendfiles(bool& bcontinue); // 执行一次发送任务的函数，bcontinue表示本次任务是否发送了文件
bool sendfile(const string& filename, const int filesize); // 发送一个文件的函数，文件名用绝对路径
bool ackmessage(const string& recvbuffer); // 处理确认报文

void recvfilesmain();   // 接收文件的主函数
bool recvfile(const string& filename, const string& mtime, const int filesize); // 执行一次接收任务的函数，文件名用绝对路径

void FathEXIT(int sig); // 父进程退出函数
void ChldEXIT(int sig); // 子进程退出函数

int main(int argc, char* argv[])
{
    if (argc != 3)
    {
        cout << "\n\n"
        "Using:fileserver logfilename port\n"
        "Example:\n"
        "/MDC/bin/tools/procctl 10 /MDC/bin/tools/fileserver /log/idc/fileserver.log 5005\n\n";

        return -1;
    }

    // 关闭io和信号，设置信号处理函数
    closeioandsignal(true);
    signal(SIGINT, FathEXIT);
    signal(SIGTERM, FathEXIT);

    // 打开日志文件
    if (logfile.open(argv[1]) == false)
    {
        cout << "logfile.open(" << argv[1] << ") failed\n";
        return -1;
    }

    // 配置心跳信息
    pactive.addpinfo(starg.timeout, starg.pname);

    // 初始化监听端口
    if (tcpserver.initserver(atoi(argv[2])) == false)
    {
        logfile.write("[init listen port failed] tcpserver.initserver(%d)\n", atoi(argv[2]));
        return -1;
    }

    while (true)
    {
        if (tcpserver.accept() == false)
        {
            logfile.write("[accept client failed] tcpserver.accept()\n");
            
        }
        logfile.write("accept client(%s) success\n", tcpserver.getip());

        if (fork() != 0) // 父进程负责监听
        {
            tcpserver.closeclient();
            continue;
        }

        // 子进程负责处理业务
        tcpserver.closelisten();

        // 子进程重新设置处理函数
        signal(SIGINT,ChldEXIT); signal(SIGTERM,ChldEXIT);

        // 客户端登录
        if (clientlogin() == false)
        {
            logfile.write("[client login failed] clientlogin()\n");
            ChldEXIT(-1);
        }
        logfile.write("[client login success]\n");

        if (starg.clienttype == 1) sendfilesmain();

        if (starg.clienttype == 2) recvfilesmain();

        ChldEXIT(0);
    }
}

bool clientlogin()
{
    memset(&starg, 0, sizeof(struct st_arg));

    logfile.write("[clientlogin] recv ... ");
    if (tcpserver.read(recvbuffer) == false)
    {
        logfile << "failed\n";
        return false;
    }
    logfile << "success\n";

    // 不需要对参数做合法性检验，因为客户端已经做过了
    getxmlbuffer(recvbuffer, "clienttype", starg.clienttype);
    getxmlbuffer(recvbuffer, "ptype", starg.ptype);
    getxmlbuffer(recvbuffer, "srvpath", starg.srvpath, 255);
    getxmlbuffer(recvbuffer, "srvpathbak", starg.srvpathbak, 255);
    getxmlbuffer(recvbuffer, "andchild", starg.andchild);
    getxmlbuffer(recvbuffer, "matchname", starg.matchname, 255);
    getxmlbuffer(recvbuffer, "clientpath", starg.clientpath, 255);
    getxmlbuffer(recvbuffer, "timetvl", starg.timetvl);
    getxmlbuffer(recvbuffer, "timeout", starg.timeout);
    getxmlbuffer(recvbuffer, "pname", starg.pname, 63);

    // 判断客户端类型是否合法
    if ((starg.clienttype != 1) && (starg.clienttype != 2))
        sendbuffer = "failed";
    else
        sendbuffer = "success";

    // 发送报文
    if (tcpserver.write(sendbuffer) == false)
    {
        logfile.write("[clientlogin: send buffer failed] tcpserver.write()\n");
        return false;
    }

    // 如果是非法客户端
    if (strcmp(sendbuffer.c_str(), "false"))
    {
        logfile.write("[clientlogin] clienttype is illegal\n");
        return false;
    }

    return true;
}

//在没有文件可发时向对端发送心跳报文，以保持tcp连接
bool activetest()
{
    sendbuffer = "<activetest>ok</activetest>";

    logfile.write("[activetest] send %s ... ", sendbuffer);
    if (tcpserver.write(sendbuffer) == false)
    {
        logfile << "failed\n";
        return false;
    }
    logfile << "success\n";

    // 接收对端的心跳报文
    logfile.write("[activetest] recv ... ", sendbuffer);
    if (tcpserver.read(recvbuffer, 20) == false)
    {
        logfile << "failed\n";
        return false;
    }
    logfile << "success\n";

    return true;
}

void sendfilesmain()
{
    bool bcontinue = true;

    while (true)
    {
        if (_sendfiles(bcontinue) == false) 
        {
            logfile.write("[sendfilesmain: send files failed] _sendfiles()\n");
            return;
        }

        if (bcontinue == false) // 如果未发送文件，就休眠，并发送心跳报文
        {
            sleep(starg.timetvl);

            if (activetest() == false) return;
        }
    }
}

bool _sendfiles(bool& bcontinue)
{
    bcontinue = false;

    cdir dir;
    if (dir.opendir(starg.srvpath, starg.matchname, 10000, starg.andchild, false) == false)
    {
        logfile.write("[_sendfiles: open directory failed] dir.opendir(%s)\n", starg.srvpath);
        return false;
    }

    int delayed = 0; // 未收到对端确认报文的文件数量

    // 同步方式：一问一答，发送文件后等待对方的确认报文，收到确认报文后再发送下一个文件
    // 异步方式：问答分离，发送文件后查看接收缓冲区，如果有确认报文就处理，没有就继续发送文件
    // 在文件发送完后，继续处理剩余的确认报文
    // 异步的方式效率远高于同步，这里采用异步方式
    while (dir.readdir())
    {
        bcontinue = true;

        // 先向对端发送文件信息
        sformat(sendbuffer, "<filename>%s</filename><filesize>%d</filesize><mtime>%s</mtime>", 
            dir.m_filename.c_str(), dir.m_filesize, dir.m_mtime.c_str());
        
        logfile.write("[_sendfiles] send %s ... ");
        if (tcpserver.write(sendbuffer) == false)
        {
            logfile << "failed\n";
            return false;
        }
        logfile << "success\n";

        // 再发送文件
        logfile.write("[_sendfiles] send %s(%d) ... ", dir.m_filename.c_str(), dir.m_filesize);
        if (sendfile(dir.m_ffilename, dir.m_filesize) == false)
        {
            logfile << "failed\n";
            return false; 
        }
        logfile << "success\n";
        ++delayed;

        while (delayed > 0)
        {
            // 接收对端的确认报文，时间设置为-1，表示不等待，如果接收缓冲区没有报文就继续发送文件
            if (tcpserver.read(recvbuffer, -1) == false) break;

            ackmessage(recvbuffer);
            --delayed;
        }  
    }

    // 接收剩余的确认报文
    while (delayed > 0)
    {
        if (tcpserver.read(recvbuffer, 10) == false) break;

        ackmessage(recvbuffer);
        --delayed;
    }

    return true;
}

// 以二进制的形式发送文件
bool sendfile(const string& filename, const int filesize)
{
    int onread;         // 将要读取的字节数
    int totalbytes;     // 已经读取的总字节数
    char buffer[1024];  // 存放读取的数据
    cifile ifile;

    if (ifile.open(filename, ios::in | ios::binary) ==false) return false;

    while (totalbytes < filesize)
    {
        memset(buffer, 0, sizeof(buffer));

        onread = (filesize - totalbytes >= 1024) ? 1024 : (filesize - totalbytes);

        ifile.read(buffer, onread);

        if (tcpserver.write(buffer) == false) return false;

        totalbytes += onread;
    }

    return true;
}

bool ackmessage(const string& recvbuffer)
{
    string filename;
    string result;

    getxmlbuffer(recvbuffer, "filename", filename);
    getxmlbuffer(recvbuffer, "result", result);

    if (result != "success") return false;

    // 如果接收端成功收到文件，则删除或备份发送端文件
    if (starg.ptype == 1)
    {
        string removefile = sformat("%s/%s", starg.srvpath, filename);
        if (remove(removefile.c_str()) != 0)
        {
            logfile.write("[ackmessage: remove file failed] remove(%s)\n", removefile.c_str());
            return false;
        }
    }
    else if (starg.ptype == 2)
    {
        string rscfile = sformat("%s/%s", starg.srvpath, filename);
        string dstfile = sformat("%s/%s", starg.srvpathbak, filename);
        if (rename(rscfile.c_str(), dstfile.c_str()) != 0)
        {
            logfile.write("[ackmessage: bak file failed] rename(%s, %s)\n", rscfile.c_str(), dstfile.c_str());
            return false;
        }
    }   

    return true;
}

void recvfilesmain()
{
    // 作为接收文件的一端，来自对端的报文有两种
    // 心跳报文：返回确认报文
    // 上传文件的请求报文（包含文件信息），准备接收文件，接收文件后返回确认报文

    while (true)
    {
        if (tcpserver.read(recvbuffer) == false)
        {
            logfile.write("[recvfilesmain: recv buffer failed]\n");
            return;
        }
        logfile.write("[recvfilesmain] recv %s\n", recvbuffer.c_str());

        // 处理心跳报文
        if (recvbuffer == "<activetest>ok</activetest>")
        {
            sendbuffer = "ok";
            logfile.write("[recvfilesmain] send %s ... ", sendbuffer.c_str());
            if (tcpserver.write(sendbuffer) == false)
            {
                logfile << "failed\n";
                return;
            }
            logfile << "success\n";
        }

        // 处理上传文件的请求报文
        if (recvbuffer.find("<filename>") != string::npos)
        {   
            string filename;
            string mtime;
            int filesize;

            getxmlbuffer(recvbuffer, "filename", filename);
            getxmlbuffer(recvbuffer, "mtime", mtime);
            getxmlbuffer(recvbuffer, "filesize", filesize);

            string localfile = sformat("%s/%s", starg.srvpath, filename.c_str());
            sendbuffer = sformat("<filename>%s</filename>", filename.c_str());
            if (recvfile(localfile, mtime, filesize) == false)
                sendbuffer.append("<result>failed</result>");
            else
                sendbuffer.append("<result>success</result>");

            // 返回确认报文
            logfile.write("[recvfilesmain] send %s ... ", sendbuffer.c_str());
            if (tcpserver.write(sendbuffer) == false)
            {
                logfile << "failed\n";
                return;
            }
            logfile << "success\n";
        }
    }
}

bool recvfile(const string& filename, const string& mtime, const int filesize)
{
    int onread = 0;
    int totalbytes = 0;
    char buffer[1024];

    cofile ofile;
    if (ofile.open(filename, true, ios::out | ios::binary) == false) return false;

    while (totalbytes < filesize)
    {
        memset(buffer, 0, sizeof(buffer));

        onread = (filesize - totalbytes) > 0 ? 1024 : (filesize - totalbytes);

        if (tcpserver.read(buffer, onread) == false) return false;

        ofile.write(buffer, onread);

        totalbytes += onread;
    }
    ofile.closeandrename();

    // 文件时间用当前时间没有意义，应该与对端的文件时间保持一致
    setmtime(filename, mtime);

    return true;
}

void FathEXIT(int sig)
{
    // 防止信号处理函数在执行的过程中被信号中断。
    signal(SIGINT,SIG_IGN); signal(SIGTERM,SIG_IGN);

    logfile.write("[father process exit] sig=%d", sig);

    tcpserver.closelisten();

    kill(0, 15); // 通知全部的子进程退出

    exit(0);
}

void ChldEXIT(int sig)
{
    // 防止信号处理函数在执行的过程中被信号中断。
    signal(SIGINT,SIG_IGN); signal(SIGTERM,SIG_IGN);

    logfile.write("[child process exit] sig=%d", sig);

    tcpserver.closeclient();

    exit(0);
}