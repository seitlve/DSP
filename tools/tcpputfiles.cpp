/*
    tcpputfiles.cpp
    本程序是数据中心的公共功能模块，采用tcp协议向服务端上传文件
*/

#include "_public.h"

using namespace idc;

// 程序运行的参数
struct st_arg
{
    int clienttype;     // 客户端类型，1-下载，2-上传，本程序为2
    char ip[32];
    int port;
    int ptype;
    char srvpath[256]; 
    char clientpath[256]; 
    bool andchild;
    char matchname[256]; 
    char clientpathbak[256]; 
    int timetvl;
    int timeout;
    char pname[64];
}starg;

clogfile logfile;       // 日志
cpactive pactive;       // 进程心跳
ctcpclient tcpclient;   // tcp客户端

string sendbuffer;      // 发送报文
string recvbuffer;      // 接收报文

bool login(const char* argv); // 登录函数，向服务端发送本程序的信息（运行参数）
bool activetest();      // 发送心跳报文的函数

void _tcpputfiles();    // 上传文件的主函数 
bool _sendfiles(bool& bcontinue); // 执行一次发送任务的函数，bcontinue表示本次任务是否发送了文件
bool sendfile(const string& filename, const int filesize); // 发送一次文件的函数，使用绝对路径
bool ackmessage(const string& recvbuffer); // 处理确认报文

void EXIT(int sig);     // 退出函数
void _help();           // 帮助文档
bool _xmltoarg(const string& xmlbuffer); // 解析xml到starg中

int main(int argc, char* argv[])
{
    if (argc != 3)
    {
        _help();
        return -1;
    }

    // 关闭io和信号，设置信号处理函数
    closeioandsignal(true);
    signal(SIGINT, EXIT);
    signal(SIGTERM, EXIT);

    // 打开日志文件
    if (logfile.open(argv[1]) == false)
    {
        cout << "logfile.open(" << argv[1] << ") failed\n";
        return -1;
    }

    // 解析xml
    if (_xmltoarg(argv[2]) == false) return -1;

    // 配置心跳信息
    pactive.addpinfo(starg.timeout, starg.pname);

    // 连接tcp服务端
    if (tcpclient.connect(starg.ip, starg.port) == false)
    {
        logfile.write("[connect failed] tcpclient.connect(%s, %d)\n", starg.ip, starg.port);
        EXIT(-1);
    }

    // 登录
    if (login(argv[2]) == false) 
    {
        logfile.write("[login failed]\n");
        EXIT(-1);
    }
    logfile.write("[login success]\n");

    _tcpputfiles();

    return 0;
}

bool login(const char* argv)
{
    // 向服务端发送登录报文
    // 登录报文包含客户端的类型以及其它服务端所需的信息，这里直接将整个argv[2]传过去更方便
    sformat(sendbuffer, "%s<clienttype>2</clienttype>", argv);

    logfile.write("[login] send %s ... ", sendbuffer.c_str());
    if (tcpclient.write(sendbuffer) == false) 
    {
        logfile << "failed\n";
        return false;
    }
    logfile << "success\n";

    // 接收服务端的报文
    logfile.write("[login] recv ... ");
    if (tcpclient.read(recvbuffer) == false)
    {
        logfile << "failed\n";
        return false;
    }
    logfile << sformat("success, buffer=%s\n", recvbuffer.c_str());

    return true;
}

bool activetest()
{
    sendbuffer = "<activetest>ok</activetest>";

    logfile.write("[activetest] send %s ... ", sendbuffer.c_str());
    if (tcpclient.write(sendbuffer) == false)
    {
        logfile << "failed\n";
        return false;
    }
    logfile << "success\n";

    // 接收对端的心跳报文
    logfile.write("[activetest] recv ... ");
    if (tcpclient.read(recvbuffer, 20) == false)
    {
        logfile << "failed\n";
        return false;
    }
    logfile << "success\n";

    return true;
}

void _tcpputfiles()
{
    bool bcontinue = true;

    while (true)
    {
        if (_sendfiles(bcontinue) == false)
        {
            logfile.write("[_tcpputfiles: send files failed] _sendfiles()\n");
            return;
        }

        if (bcontinue == false)
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
    if (dir.opendir(starg.clientpath, starg.matchname, 10000, starg.andchild, false) == false)
    {
        logfile.write("[_sendfiles: open directory failed] dir.opendir(%s)\n", starg.srvpath);
        return false;
    }

    int delayed = 0;

    while (dir.readdir())
    {
        bcontinue = true;

        // 先向对端发送文件信息
        sendbuffer = sformat("<filename>%s</filename><filesize>%d</filesize><mtime>%s</mtime>", 
            dir.m_filename.c_str(), dir.m_filesize, dir.m_mtime.c_str());

        logfile.write("[_sendfiles] send %s ... ", sendbuffer.c_str());
        if (tcpclient.write(sendbuffer) == false)
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
            if (tcpclient.read(recvbuffer, -1) == false) break;

            ackmessage(recvbuffer);
            --delayed;
        }
    }

    // 处理剩余的确认报文
    while (delayed > 0)
    {
        if (tcpclient.read(recvbuffer, 10) == false) break;

        ackmessage(recvbuffer);
        --delayed;
    }

    return true;
}

bool sendfile(const string& filename, const int filesize)
{
    int onread = 0;
    int totalbytes = 0;
    char buffer[1024];

    cifile ifile;
    if (ifile.open(filename, ios::in | ios::binary) == false)
    {
        logfile.write("[sendfile: open file failed] ifile.open(%s)\n", filename.c_str());
        return false;
    }

    while (totalbytes < filesize)
    {
        memset(buffer, 0, sizeof(buffer));

        onread = (filesize - totalbytes) > 1024 ? 1024 : (filesize - totalbytes);

        ifile.read(buffer, onread);

        if (tcpclient.write(buffer, onread) == false) return false;

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

    if (starg.ptype == 1)
    {
        string removefile = sformat("%s/%s", starg.clientpath, filename.c_str());
        if (remove(removefile.c_str()) != 0)
        {
            logfile.write("[ackmessage: remove file failed] remove(%s)\n", removefile.c_str());
            return false;
        }
    }

    if (starg.ptype == 2)
    {
        string rscfile = sformat("%s/%s", starg.clientpath, filename.c_str());
        string dstfile = sformat("%s/%s", starg.clientpathbak, filename.c_str());
        if (rename(rscfile.c_str(), dstfile.c_str()) == false)
        {
            logfile.write("[ackmessage: bak file failed] rename(%s, %s)\n", rscfile.c_str(), dstfile.c_str());
            return false;
        }
    }

    return true;
}

void EXIT(int sig)
{
    logfile.write("[process exit] sig=%d", sig);

    exit(0);
}

void _help()
{
    cout << "\n\n"
    "Using:/MDC/bin/tools/tcpputfiles logfilename xmlbuffer\n\n"

    "Example:\n"
    "/MDC/bin/tools/procctl 20 /MDC/bin/tools/tcpputfiles /MDC/log/tools/tcpputfiles_test.log "
    "\"<ip>192.168.19.132</ip><port>5005</port>"
    "<ptype>1</ptype><srvpath>/test/tcp/server</srvpath>"
    "<clientpath>/test/tcp/client</clientpath><andchild>true</andchild>"
    "<matchname>*</matchname><clientpathbak>/test/tcp/client_bak</clientpathbak>"
    "<timetvl>10</timetvl><timeout>50</timeout><pname>tcpputfiles_test</pname>\"\n\n"

    "本程序是数据中心的公共功能模块，采用tcp协议向服务端上传文件\n"
    "logfilename   本程序运行的日志文件\n"
    "xmlbuffer     本程序运行的参数，如下：\n"
    "ip            服务端的IP地址\n"
    "port          服务端的端口\n"
    "ptype         文件上传成功后客户端文件的处理方式：1-删除文件；2-移动到备份目录\n"
    "srvpath       服务端文件存放的根目录\n"
    "clientpath    客户端文件存放的根目录\n"
    "andchild      是否上传clientpath目录下各级子目录的文件，true-是；false-否，缺省为false\n"
    "matchname     待上传文件名的匹配规则，如\"*.TXT,*.XML\"\n"
    "clientpathbak 文件成功上传后，客户端文件备份的根目录，当ptype==2时有效\n"
    "timetvl       扫描客户端目录文件的时间间隔，单位：秒，取值在1-30之间\n"
    "timeout       本程序的超时时间，单位：秒，视文件大小和网络带宽而定，建议设置50以上\n"
    "pname         进程名，尽可能采用易懂的、与其它进程不同的名称，方便故障排查\n\n";
}

bool _xmltoarg(const string& xmlbuffer)
{
    memset(&starg, 0, sizeof(struct st_arg));

    getxmlbuffer(xmlbuffer, "ip", starg.ip, 31);
    if (strlen(starg.ip) == 0) { logfile.write("ip is null\n"); return false; }

    getxmlbuffer(xmlbuffer, "port", starg.port);
    if (starg.port == 0) { logfile.write("port is null\n"); return false; }

    getxmlbuffer(xmlbuffer, "ptype", starg.ptype);
    if ((starg.ptype != 1) && (starg.ptype != 2)) 
    { logfile.write("ptype must in {1,2}\n"); return false; }

    getxmlbuffer(xmlbuffer, "srvpath", starg.srvpath, 255);
    if (strlen(starg.srvpath) == 0) { logfile.write("srvpath is null\n"); return false; }

    getxmlbuffer(xmlbuffer, "clientpath", starg.clientpath, 255);
    if (strlen(starg.clientpath) == 0) { logfile.write("clientpath is null\n"); return false; }

    getxmlbuffer(xmlbuffer, "andchild", starg.andchild);

    getxmlbuffer(xmlbuffer, "matchname", starg.matchname, 255);
    if (strlen(starg.matchname) == 0) { logfile.write("matchname is null\n"); return false; }

    if (starg.ptype == 2)
    {
        getxmlbuffer(xmlbuffer, "clientpathbak", starg.clientpathbak, 255);
        if (strlen(starg.clientpathbak) == 0) { logfile.write("clientpathbak is null\n"); return false; }
    }

    getxmlbuffer(xmlbuffer, "timetvl", starg.timetvl);
    if (starg.timetvl == 0) { logfile.write("timetvl is null\n"); return false; }
    if (starg.timetvl > 30) starg.timetvl = 30; // 程序扫描的间隔不必大于30s

    getxmlbuffer(xmlbuffer, "timeout", starg.timeout);
    if (starg.timeout == 0) { logfile.write("timeout is null\n"); return false; }
    if (starg.timeout <=  starg.timetvl) // timeout必须大于timetvl
    { logfile.write("timeout(%d) <= timetvl(%d)\n", starg.timeout, starg.timetvl); return false; }

    getxmlbuffer(xmlbuffer, "pname", starg.pname, 63);

    return true;
}