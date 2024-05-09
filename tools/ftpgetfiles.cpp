/*
    ftpgetfiles.cpp
    本程序是通用的功能模块，用于把远程ftp服务端的文件下载到本地目录
    下载方式分为增量下载和全量下载
    如果是增量下载，需要记录已下载的文件，用一个xml文件记录文件名和修改时间
    如果选择检查服务端文件时间，则更新过的已下载文件会重新下载
    如果是全量下载，需要选择服务端文件的处理方式：删除或备份
*/

#include "_public.h"
#include "_ftp.h"

using namespace idc;

// 程序运行的参数的结构体
struct st_arg           
{
    char host[32];
    int mode;
    char username[32];
    char password[32];
    char remotepath[256];
    char localpath[256];
    char matchname[256];
    int ptype;
    char remotepathbak[256];
    char okfilename[256];
    bool checkmtime;
    int timeout;
    char pname[64];
}starg;

clogfile logfile;     // 日志
cpactive pactive;     // 进程心跳
cftpclient ftpclient; // ftp客户端

// 文件信息的结构体，记录文件名和修改时间
struct st_fileinfo
{
    string filename;
    string mtime;

    st_fileinfo() = default;
    st_fileinfo(const string& in_filename, const string& in_mtime)
        : filename(in_filename), mtime(in_mtime) {}
    
    void clear() { filename.clear(); mtime.clear(); }
};

// vfromnlist = vtook + vdownload
// 更新okfilename：vtook + vdownload中成功下载的文件
// vdownload可能与mfromok有重叠，因为文件更新后需要重新下载
map<string, string> mfromok;    // 已下载的文件列表，从okfilename中读取，键是文件名，值是修改时间
list<struct st_fileinfo> vfromnlist;        // 服务端的文件列表，通过nlist获取
list<struct st_fileinfo> vtook;             // 本次不需要下载的文件
list<struct st_fileinfo> vdownload;         // 本次需要下载的文件

bool loadokfile();              // 加载starg.okfilename文件中的内容到容器mfromok中
bool loadnlistfile();           // 把ftp.nlist()方法获取到的list文件加载到容器vfromnlist中
bool compmap();                 // 比较vfromnlist和mfromok，得到vtook和vdownload
bool writetookfile();           // 把容器vtook中的数据写入starg.okfilename文件，覆盖之前的旧starg.okfilename文件
bool appendtookfile(struct st_fileinfo &stfileinfo); // 把下载成功的文件记录追加到starg.okfilename文件中

void EXIT(int sig); // 退出函数
void _help();       // 帮助文档
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

    // 登录ftp服务器
    if (ftpclient.login(starg.host,starg.username,starg.password,starg.mode) == false)
    {
        logfile.write("[ftp login(%s,%s,%s) failed] %s\n",
            starg.host,starg.username,starg.password,ftpclient.response()); 
        return -1;
    }

    // 进入ftp服务端存放文件的目录
    if (ftpclient.chdir(starg.remotepath) == false)
    {
        logfile.write("[ftp cd remotepath(%s) failed] %s\n", starg.remotepath, ftpclient.response());
        return -1;
    }

    // 调用ftpclient.nlist()方法列出服务器目录中的文件名，保存在本地文件中
    if (ftpclient.nlist(".", sformat("/tmp/nlist/ftpgetfiles_%d.nlist",getpid())) == false)
    {
        logfile.write("[ftp nlist remotefiles failed] %s\n", ftpclient.response());
        return -1;
    }

    pactive.uptatime();     // 更新进程心跳

    // 把ftpclient.nlist()方法获取到的list文件加载到容器vfromnlist中
    if (loadnlistfile() == false) return -1;

    if (starg.ptype == 1)
    {
        loadokfile();       // 加载starg.okfilename文件中的内容到容器mfromok中
        compmap();          // 比较vfromnlist和mfromok，得到vtook和vdownload
        writetookfile();    // 把容器vtook中的数据写入starg.okfilename文件，覆盖之前的旧starg.okfilename文件
    }
    else
    {
        // 交换vfromnlist和vdownload，统一使用vdownload下载文件
        vfromnlist.swap(vdownload);
    }

    pactive.uptatime();     // 更新进程心跳

    string remotefilename;
    string localfilename;
    for (auto e : vdownload)
    {
        sformat(remotefilename, "%s/%s", starg.remotepath, e.filename.c_str());
        sformat(localfilename, "%s/%s", starg.localpath, e.filename.c_str());

        logfile.write("[download %s ... ", remotefilename.c_str());
        if (ftpclient.get(remotefilename, localfilename) == false)
        {
            logfile << "failed] " << ftpclient.response() << "\n";
            return -1;
        }
        logfile << "success]\n";

        if (starg.ptype == 1) appendtookfile(e); // 把下载成功的文件记录追加到starg.okfilename文件中

        if (starg.ptype == 2) // 删除服务端的文件
        {
            if (ftpclient.ftpdelete(remotefilename) == false)
            {
                logfile.write("[delete remote file failed] ftpclient.ftpdelete(%s) %s\n", remotefilename.c_str(), ftpclient.response());
                return false;
            }
        }

        if (starg.ptype == 3) // 备份服务端的文件
        {
            string remotefilebak = sformat("%s/%s", starg.remotepathbak, e.filename.c_str());
            if (ftpclient.ftprename(remotefilename, remotefilebak) == false)
            {
                logfile.write("[bak remote file failed] ftpclient.ftprename(%s, %s) %s\n",
                    remotefilename.c_str(), remotefilebak.c_str(), ftpclient.response());
                return -1;
            }
        }
    }

    return 0;
}   

bool loadokfile()
{
    mfromok.clear();

    cifile ifile;
    // 第一次下载时okfilename是空的，函数返回true
    if (ifile.open(starg.okfilename) == false) return true;

    string xmlbuffer;
    struct st_fileinfo stfileinfo;
    while (ifile.readline(xmlbuffer))
    {
        stfileinfo.clear();

        getxmlbuffer(xmlbuffer, "filename", stfileinfo.filename);
        getxmlbuffer(xmlbuffer, "mtime", stfileinfo.mtime);

        mfromok[stfileinfo.filename] = stfileinfo.mtime;
    }

    return true;
}

bool loadnlistfile()
{
    vfromnlist.clear();

    // 打开nlist文件
    cifile ifile;
    string nlistfile = sformat("/tmp/nlist/ftpgetfiles_%d.nlist",getpid());
    if (ifile.open(nlistfile) == false)
    {
        logfile.write("[loadnlistfile: open file failed] ifile.open(%s)\n", nlistfile.c_str());
        return false;
    }

    // 读取文件，获取符合matchname的文件，记录修改时间
    string filename;
    while (ifile.readline(filename))
    {
        if (matchstr(filename, starg.matchname) == false) continue;

        if ((starg.ptype == 1) && starg.checkmtime)
        {
            if (ftpclient.mtime(filename) == false)
            {
                logfile.write("[loadnlistfile: ftp get file mtime failed] ftpclient.mtime(%s)\n", filename.c_str());
                return false;
            }
        }

        vfromnlist.emplace_back(filename, ftpclient.m_mtime);
    }

    ifile.closeandremove();

    return true;
}

bool compmap()
{
    vtook.clear();
    vdownload.clear();

    for (auto& e : vfromnlist)
    {
        auto it = mfromok.find(e.filename); // 查找是否在已下载的文件中
        if (it != mfromok.end())
        {
            // 如果在已下载的文件中，再检查修改时间
            if (starg.checkmtime)
            {
                if (e.mtime == mfromok[e.filename]) // 如果文件未修改
                    vtook.emplace_back(e.filename, e.mtime);
                else // 如果文件已修改
                    vdownload.emplace_back(e.filename, e.mtime);
            }
            else // 如果不检查文件修改时间
                vtook.emplace_back(e.filename, e.mtime);
        }
        else
        {
            // 如果不在，加入下载列表中
            vdownload.emplace_back(e.filename, e.mtime);
        }
    }

    return true;
}

bool writetookfile()
{
    cofile ofile;
    if (ofile.open(starg.okfilename) == false)
    {
        logfile.write("[writetookfile: open file failed] ofile.open(%s)\n", starg.okfilename);
        return false;
    }

    for (auto e : vtook)
        ofile.writeline("<filename>%s</filename><mtime>%s</mtime>\n", e.filename.c_str(), e.mtime.c_str());

    ofile.closeandrename();

    return true;
}

bool appendtookfile(struct st_fileinfo &stfileinfo)
{
    cofile ofile;
    // 以追加写的方式打开文件，第二个参数一定要填false
    if (ofile.open(starg.okfilename, false, ios::app) == false)
    {
        logfile.write("[appendtookfile: open file failed] ofile.open(%s)\n", starg.okfilename);
        return false;
    }

    ofile.writeline("<filename>%s</filename><mtime>%s</mtime>\n", stfileinfo.filename.c_str(), stfileinfo.mtime.c_str());

    return true;
}

void EXIT(int sig)
{
    logfile.write("[process exit] sig=%d\n", sig);

    exit(0);
}

void _help()
{
    cout << "\n\nUsing:ftpgetfiles logfilename xmlbuffer\n\n"

            "Example:\n"
            "/MDC/bin/tools/procctl 30 /MDC/bin/tools/ftpgetfiles /MDC/log/tools/ftpgetfiles.log "
            "\"<host>192.168.19.132:21</host><mode>1</mode>"
            "<username>elokuu</username><password>reflection</password>"
            "<remotepath>/test/ftp/server</remotepath><localpath>/test/ftp/client</localpath>"
            "<matchname>*.TXT</matchname><ptype>1</ptype>"
            "<remotepathbak>/test/ftp/server_bak</remotepathbak>"
            "<okfilename>/test/ftp/ftpgetfiles_test.xml</okfilename>"
            "<checkmtime>true</checkmtime>"
            "<timeout>30</timeout><pname>ftpgetfiles_test</pname>\"\n\n"

            "本程序是通用的功能模块，用于把远程ftp服务端的文件下载到本地目录\n"
            "logfilename是本程序运行的日志文件\n"
            "xmlbuffer为文件下载的参数，如下：\n"
            "<host>192.168.19.132:21</host> 远程服务端的IP和端口\n"
            "<mode>1</mode> 传输模式，1-被动模式，2-主动模式，缺省采用被动模式\n"
            "<username>elokuu</username> 远程服务端ftp的用户名\n"
            "<password>reflection</password> 远程服务端ftp的密码\n"
            "<remotepath>/test/ftp/server</remotepath> 远程服务端存放文件的目录\n"
            "<localpath>/tmp/ftp/client</localpath> 本地文件存放的目录\n"
            "<matchname>*.TXT</matchname> 待下载文件匹配的规则"
            "<ptype>1</ptype> 文件下载成功后，远程服务端文件的处理方式："
            "1-什么也不做（增量下载）；2-删除；3-备份，如果为3，还要指定备份的目录\n"
            "<remotepathbak>/test/ftp/server_bak</remotepathbak> 文件下载成功后，服务端文件的备份目录"
            "此参数只有当ptype=3时才有效\n"
            "<okfilename>/test/ftp/ftpgetfiles_test.xml</okfilename> 已下载成功文件名清单，"
            "此参数只有当ptype=1时才有效\n"
            "<checkmtime>true</checkmtime> 是否需要检查服务端文件的时间，true-需要，false-不需要，"
            "此参数只有当ptype=1时才有效，缺省为false\n"
            "<timeout>30</timeout> 下载文件超时时间，单位：秒，视文件大小和网络带宽而定\n"
            "<pname>ftpgetfiles_test</pname> 进程名，尽可能采用易懂的、与其它进程不同的名称，方便故障排查\n\n";
}

bool _xmltoarg(const string& xmlbuffer)
{
    memset(&starg, 0, sizeof(struct st_arg));

    getxmlbuffer(xmlbuffer, "host", starg.host, 31);
    if (strlen(starg.host) == 0) { logfile.write("host is null\n"); return false; }

    getxmlbuffer(xmlbuffer, "mode", starg.mode);
    if (starg.mode != 1) starg.mode = 1;

    getxmlbuffer(xmlbuffer, "username", starg.username, 31);
    if (strlen(starg.username) == 0) { logfile.write("username is null\n"); return false; }

    getxmlbuffer(xmlbuffer, "password", starg.password, 31);
    if (strlen(starg.password) == 0) { logfile.write("password is null\n"); return false; }

    getxmlbuffer(xmlbuffer, "remotepath", starg.remotepath, 255);
    if (strlen(starg.remotepath) == 0) { logfile.write("remotepath is null\n"); return false; }

    getxmlbuffer(xmlbuffer, "localpath", starg.localpath, 255);
    if (strlen(starg.localpath) == 0) { logfile.write("localpath is null\n"); return false; }

    getxmlbuffer(xmlbuffer, "matchname", starg.matchname, 255);
    if (strlen(starg.matchname) == 0) { logfile.write("matchname is null\n"); return false; }

    getxmlbuffer(xmlbuffer, "ptype", starg.ptype);
    if ((starg.ptype != 1) && (starg.ptype != 2) && (starg.ptype != 3)) 
    { logfile.write("ptype must in {1,2,3}\n"); return false; }

    if (starg.ptype == 3)
    {
        getxmlbuffer(xmlbuffer, "remotepathbak", starg.remotepathbak, 255);
        if (strlen(starg.remotepathbak) == 0) { logfile.write("remotepathbak is null\n"); return false; }
    }

    if (starg.ptype == 1)
    {
        getxmlbuffer(xmlbuffer, "okfilename", starg.okfilename, 255);
        if (strlen(starg.okfilename) == 0) { logfile.write("okfilename is null\n"); return false; }

        getxmlbuffer(xmlbuffer, "checkmtime", starg.checkmtime);
        if (starg.checkmtime != true) starg.checkmtime = false;
    }

    getxmlbuffer(xmlbuffer, "timeout", starg.timeout);
    if (starg.timeout == 0) { logfile.write("timeout is null\n"); return false; }

    getxmlbuffer(xmlbuffer, "pname", starg.pname, 63); // pname可以为空

    return true;
}