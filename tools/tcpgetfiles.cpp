/*
    tcpgetfiles.cpp
    本程序是数据中心的公共功能模块，采用tcp协议从服务端下载文件
*/

#include "_public.h"

using namespace idc;

// 程序运行的参数
struct st_arg
{
    int clienttype;     // 客户端类型，1-下载，2-上传，本程序为1
    char ip[32];
    int port;
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
ctcpclient tcpclient;   // tcp客户端

string sendbuffer;      // 发送报文
string recvbuffer;      // 接收报文

bool login(const char* argv); // 登录函数，向服务端发送本程序的信息（运行参数）

void _tcpgetfiles();    // 下载文件的主函数
bool recvfile(const string& filename, const string& mtime, const int filesize); // 接收一次文件传输的函数，使用绝对路径        

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

    _tcpgetfiles();

    return 0;
}

bool login(const char* argv)
{
    // 向服务端发送登录报文
    // 登录报文包含客户端的类型以及其它服务端所需的信息，这里直接将整个argv[2]传过去更方便
    sformat(sendbuffer, "%s<clienttype>1</clienttype>", argv);

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
    logfile << "success\n";

    if (recvbuffer == "failed") return false;

    return true;
}

void _tcpgetfiles()
{
    while (true)
    {
        logfile.write("[_tcpgetfiles] recv ... ");
        if (tcpclient.read(recvbuffer) == false)
        {
            logfile << "failed\n";
            return;
        }
        logfile << sformat("success, buffer: %s\n", recvbuffer.c_str());

        // 处理心跳报文
        if (recvbuffer == "<activetest>ok</activetest>")
        {
            sendbuffer = "ok";
            if (tcpclient.write(sendbuffer) == false)
            {
                logfile.write("[_tcpgetfiles: send buffer failed] tcpclient.write(%s)\n", sendbuffer.c_str());
                return;
            }
        }

        // 处理发送文件的请求报文
        if (recvbuffer.find("<filename>") != string::npos)
        {
            string filename;
            string mtime;
            int filesize;

            getxmlbuffer(recvbuffer, "filename", filename);
            getxmlbuffer(recvbuffer, "mtime", mtime);
            getxmlbuffer(recvbuffer, "filesize", filesize);

            string localfile = sformat("%s/%s", starg.clientpath, filename.c_str());
            sendbuffer = sformat("<filename>%s</filename>", filename.c_str());
            if (recvfile(localfile, mtime, filesize) == false)
                sendbuffer.append("<result>failed</result>");
            else
                sendbuffer.append("<result>success</result>");

            // 返回确认报文
            logfile.write("[_tcpgetfiles] send %s ... ", sendbuffer.c_str());
            if (tcpclient.write(sendbuffer) == false)
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
    if (ofile.open(filename, true, ios::out | ios::binary) == false)
    {
        logfile.write("[getfile: open file failed] ofile.open(%s)\n", filename.c_str());
        return false;
    }

    while (totalbytes < filesize)
    {
        memset(buffer, 0, sizeof(buffer));

        onread = (filesize - totalbytes) > 1024 ? 1024 : (filesize - totalbytes);

        if (tcpclient.read(buffer, onread) == false) return false;

        ofile.write(buffer, onread);

        totalbytes += onread;
    }
    ofile.closeandrename();

    setmtime(filename, mtime);

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
    "Using:/MDC/bin/tools/tcpgetfiles logfilename xmlbuffer\n\n"

    "Example:\n"
    "/MDC/bin/tools/procctl 20 /MDC/bin/tools/tcpgetfiles /MDC/log/tools/tcpgetfiles_test.log "
    "\"<ip>192.168.19.132</ip><port>5005</port>"
    "<clientpath>/test/tcp/client</clientpath>"
    "<ptype>1</ptype><srvpath>/test/tcp/server</srvpath>"
    "<andchild>true</andchild><matchname>*</matchname>"
    "<timetvl>10</timetvl><timeout>50</timeout><pname>tcpgetfiles_test</pname>\"\n\n"

    "本程序是数据中心的公共功能模块，采用tcp协议从服务端下载文件\n"
    "logfilename   本程序运行的日志文件\n"
    "xmlbuffer     本程序运行的参数，如下：\n"
    "ip            服务端的IP地址\n"
    "port          服务端的端口\n"
    "ptype         文件下载成功后服务端文件的处理方式：1-删除文件；2-移动到备份目录\n"
    "srvpath       服务端文件存放的根目录\n"
    "srvpathbak    文件成功下载后，服务端文件备份的根目录，当ptype==2时有效\n"
    "andchild      是否下载srvpath目录下各级子目录的文件，true-是；false-否，缺省为false\n"
    "matchname     待下载文件名的匹配规则，如\"*.TXT,*.XML\"\n"
    "clientpath    客户端文件存放的根目录\n"
    "timetvl       扫描服务目录文件的时间间隔，单位：秒，取值在1-30之间\n"
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

    if (starg.ptype == 2)
    {
        getxmlbuffer(xmlbuffer, "srvpathbak", starg.srvpathbak, 255);
        if (strlen(starg.srvpathbak) == 0) { logfile.write("srvpathbak is null\n"); return false; }
    }

    getxmlbuffer(xmlbuffer, "andchild", starg.andchild);

    getxmlbuffer(xmlbuffer, "matchname", starg.matchname, 255);
    if (strlen(starg.matchname) == 0) { logfile.write("matchname is null\n"); return false; }

    getxmlbuffer(xmlbuffer, "clientpath", starg.clientpath, 255);
    if (strlen(starg.clientpath) == 0) { logfile.write("clientpath is null\n"); return false; }

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