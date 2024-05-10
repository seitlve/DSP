/*
    xmltodb.cpp
    本程序是数据中心的公共功能模块，用于把xml文件入库到Oracle的表中
    需要传入xml文件的目录，以及一个inifile
    inifile中保存数据入库的参数
    一行xml数据对应一个数据入库参数，分别是：
        1.filename：入库文件的匹配规则
        2.tname：表名
        3.uptbz：更新标志：1-更新；2-不更新
        4.execsql：数据文件入库之前执行的sql语句
    正常情况下，一种xml文件（一种匹配规则）应当对应唯一一个数据入库参数
*/

#include "_tools.h"

// 程序运行的参数
struct st_arg       
{
    char connstr[128];
    char charset[64];
    char inifilename[256];
    char xmlpath[256];
    char xmlpathbak[256];
    char xmlpatherr[256];
    int timetvl;
    int timeout;
    char pname[64];
}starg;

clogfile logfile;       // 日志
cpactive pactive;       // 进程心跳
connection conn;        // 数据库连接

// 数据入库参数的结构体
struct st_xmltotable
{
    char filename[128]; // xml文件的匹配规则，用逗号分隔
    char tname[32];     // 待入库的表名
    int uptbz;          // 更新标志：1-更新；2-不更新
    char execsql[256];  // 处理xml文件之前，执行的SQL语句
} stxmltotable;

vector<struct st_xmltotable> vxmltotable;   // 存放数据入库的参数

bool loadxmltotable();  // 从inifile中将入库参数加载到vxmltotable中
bool _xmltodb();        // 数据入库的主函数
int _xmltodb(const string& fullfilename,const string& filename); // 处理xml文件的子函数，返回值：0-成功
bool findxmltotable(const string& xmlfile); // 根据文件名，从vxmltotable容器中查找的入库参数，存放在stxmltotable结构体中

ctimer timer;                       // 处理每个xml文件消耗的时间
int totalcount, inscount, uptcount; // xml文件的总记录数、插入记录数和更新记录数

ctcols tcols;                       // 用于获取表的字段和主键

string insertsql;                   // 插入表的SQL语句
string updatesql;                   // 更新表的SQL语句
vector<string> vcolvalue;           // 存放从xml每一行中解析出来的字段的值，将用于插入和更新表的SQL语句绑定变量
sqlstatement stmtins, stmtupt;      // 插入和更新表的sqlstatement语句
sqlstatement stmtpre;               // 文件入库前执行的sql 

void crtsql();          // 拼接插入和更新表数据的SQL
void preparesql();      // 准备插入和更新的sql语句，绑定输入变量
bool execsql();         // 在处理xml文件之前，如果stxmltotable.execsql不为空，就执行它
void splitbuffer(const string& xmlbuffer); // 解析xml，存放在vcolvalue中

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
    if (_xmltoarg(argv[2]) == false) EXIT(-1);

    // 配置心跳信息
    pactive.addpinfo(starg.timeout, starg.pname);

    _xmltodb();

    return 0;
}

bool loadxmltotable()
{
    vxmltotable.clear();

    cifile ifile;
    if (ifile.open(starg.inifilename) == false)
    {
        logfile.write("[loadxmltotable: open inifile failed] ifile.open(%s)\n", starg.inifilename);
        return false;
    }

    string buffer;
    // readline()函数可以指定每行的结束标志，这里为</endl>
    while (ifile.readline(buffer, "<endl/>"))
    {
        memset(&stxmltotable, 0, sizeof(struct st_xmltotable));

        getxmlbuffer(buffer, "filename", stxmltotable.filename);
        getxmlbuffer(buffer, "tname", stxmltotable.tname);
        getxmlbuffer(buffer, "uptbz", stxmltotable.uptbz);
        getxmlbuffer(buffer, "execsql", stxmltotable.execsql);

        vxmltotable.push_back(stxmltotable);
    }
    logfile.write("[load xmltotable success]\n");

    return true;
}

bool _xmltodb()
{
    cdir dir;
    int inicount = 50;

    while (true)
    {
        // 本程序常驻内存，需要定时加载参数，因为inifile随时可能被修改
        if (++inicount > 30)
        {
            if (loadxmltotable() == false) return false;

            inicount = 0;
        }

        // 打开starg.xmlpath目录，为了保证先生成的xml文件先入库，打开目录的时候，应该按文件名排序。
        if (dir.opendir(starg.xmlpath, "*.XML", 10000, false, true) == false)
        {
            logfile.write("[_xmltodb: open directory failed] dir.opendir(%s)\n",starg.xmlpath); 
            return false;
        }
        
        if (conn.isopen() == false)
        {
            if (conn.connecttodb(starg.connstr, starg.charset) != 0)
            {
                logfile.write("[connect to database failed] conn.connecttodb(%s, %s)\n", starg.connstr, starg.charset);
                EXIT(-1);
            }
            logfile.write("[connect to database(%s) success]\n", starg.connstr);
        } 

        while (dir.readdir())
        {   
            logfile.write("[_xmltodb] process file(%s) ... ", dir.m_ffilename.c_str());

            int ret=_xmltodb(dir.m_ffilename,dir.m_filename);

            pactive.uptatime();   // 更新进程的心跳

            if (ret == 0) // 文件入库成功，将其移至备份目录
            {
                string bakfile = sformat("%s/%s", starg.xmlpathbak, dir.m_filename.c_str());
                // 备份文件一般不会失败，如果失败了，程序将退出
                if (rename(dir.m_ffilename.c_str(), bakfile.c_str()) != 0) 
                {
                    logfile << sformat("failed, bak file(%s, %s) failed\n", 
                        dir.m_ffilename.c_str(), bakfile.c_str());
                    return false;
                }
                logfile << sformat("success(total: %d, insert: %d, update: %d, failed: %d, time: %f)\n", 
                    totalcount, inscount, uptcount, totalcount - inscount - uptcount, timer.elapsed());
            }

            // 1-入库参数不正确；3-待入库的表不存在；4-执行入库前的SQL语句失败
            // 把xml文件移动到错误目录
            if ((ret == 1) || (ret == 3) || (ret == 4))
            {
                if (ret == 1) logfile << "failed, incorrect xmltotable\n";
                if (ret == 3) logfile << "failed, table not exist\n";
                if (ret == 4) 
                {
                    logfile << sformat("failed, execute previous sql failed\nsql: %s\nerror: %s\n", 
                        stmtpre.sql(), stmtpre.message());
                }

                string errfile = sformat("%s/%s", starg.xmlpatherr, dir.m_filename.c_str());
                 if (rename(dir.m_ffilename.c_str(), errfile.c_str()) != 0)
                {
                    logfile.write("[_xmltodb: move file to error directory failed] rename(%s, %s)\n", 
                        dir.m_ffilename.c_str(), errfile.c_str());
                    return false;
                }
            }

            // 2-数据库错误，程序将退出
            if (ret == 2)
            {
                logfile << "failed, database error\n";  
                return false;
            }

            // 5-打开xml文件失败，程序将退出
            if (ret == 5)
            {
                logfile << sformat("failed, open file(%s) failed\n", dir.m_filename); 
                return false;
            }
        }

        // 刚刚处理了文件，就继续处理，否则程序休眠
        if (dir.size() == 0) sleep(starg.timetvl);

        pactive.uptatime();   // 更新进程的心跳
    }

    return true;
}

int _xmltodb(const string& fullfilename,const string& filename)
{
    timer.start();
    totalcount = inscount = uptcount = 0;

    // 找到文件对应的入库参数，如果没有返回1
    if (findxmltotable(filename) == false) return 1;

    // 获取字段和主键，失败返回2，失败原因为数据库系统有问题，或网络断开，或连接超时
    if (tcols.allcols(conn, stxmltotable.tname) == false) return 2;
    if (tcols.pkcols(conn, stxmltotable.tname) == false)  return 2;

    // 如果tcols.m_vallcols.size()为0，说明表根本不存在（配错了参数或忘了建表），返回3
    if (tcols.m_vallcols.size() == 0) return 3;

    // 拼接sql语句
    crtsql();

    // 准备sql对象
    preparesql();

    // 在处理xml文件之前，如果stxmltotable.execsql不为空，就执行它
    // 如果执行失败，返回4
    if (execsql() == false) return 4;

    // 打开xml文件，如果失败，返回5
    cifile ifile;
    if (ifile.open(fullfilename) == false) 
    {
        conn.rollback(); // 打开文件失败，需要回滚execsql()的事务
        return 5;
    }

    string xmlbuffer;
    while (ifile.readline(xmlbuffer, "<endl/>"))
    {
        ++totalcount;           // xml文件的总记录数加1

        splitbuffer(xmlbuffer); // 解析xml的值到vcolvalue中

        // 执行插入语句
        if (stmtins.execute() != 0)
        {
            if (stmtins.rc() == 1) // 违反唯一性约束，表示记录已存在，执行更新语句
            {
                if (stxmltotable.uptbz == 1)
                {
                    if (stmtupt.execute() != 0)
                    {
                        // 更新语句失败，主要是数据本身有问题，例如时间的格式不正确、数值不合法、数值太大
                        // 记录日志，但不返回失败
                        logfile.write("[_xmltodb: execute update sql failed]\nxml: %s\nsql: %S\nerror: %s\n", 
                            xmlbuffer.c_str(), stmtupt.sql(), stmtupt.message());
                    }
                    else ++uptcount; // 更新的记录数加1
                }
            }
            else
            {
                // 插入语句失败，记录日志
                // 如果是数据本身的问题，则不返回失败
                logfile.write("[_xmltodb: execute insert sql failed]\nxml: %s\nsql: %S\nerror: %s\n", 
                            xmlbuffer.c_str(), stmtins.sql(), stmtins.message());

                // 如果是数据库系统出了问题，常见的问题如下，还可能有更多的错误，如果出现了，再加进来
                // ORA-03113: 通信通道的文件结尾；ORA-03114: 未连接到ORACLE；ORA-03135: 连接失去联系；ORA-16014：归档失败
                if ((stmtins.rc() == 3113) ||
                    (stmtins.rc() == 3114) ||
                    (stmtins.rc() == 3135) ||
                    (stmtins.rc() == 16014)) 
                    return 2;
            }
        }
        else ++inscount; // 插入的记录数加1
    }

    conn.commit();

    return 0;
}

bool findxmltotable(const string& xmlfile)
{
    for (auto& e : vxmltotable)
    {
        if (matchstr(xmlfile, e.filename))
        {
            stxmltotable = e;
            return true;
        }
    }

    return false;
}

void crtsql()
{   
    // 拼接插入表的SQL语句。 
    // insert into T_ZHOBTMIND1(obtid,ddatetime,t,p,u,wd,wf,r,vis,keyid) \
             values(:1,to_date(:2,'yyyymmddhh24miss'),:3,:4,:5,:6,:7,:8,:9,SEQ_ZHOBTMIND1.nextval)
    string cols;        // 字段列表
    string binds;       // 绑定部分的字符串
    int colseq = 1;     // 绑定的序号

    for (auto& e : tcols.m_vallcols)
    {
        // upttime字段的缺省值是sysdate，不需要处理
        if (strcmp(e.colname,"upttime") == 0) continue;

        cols += sformat("%s,", e.colname);

        // 需要区分keyid字段、date类型字段和其它字段
        if (strcmp(e.colname,"keyid") == 0)
        {
            // keyid为递增字段，名称固定为SEQ_表名（除去开头的两个字符"T_"），值固定为nextval
            binds += sformat("SEQ_%s.nextval,", stxmltotable.tname + 2);
        }
        else if (strcmp(e.datatype,"date") == 0)
        {
            // date字段需要将值转成date类型
            binds += sformat("to_date(:%d,'yyyymmddhh24miss'),", colseq++);
        }
        else
        {
            // 其它字段
            binds += sformat(":%d,", colseq++);
        }
    }
    deleterchr(cols, ','); // 删除最后一个逗号
    deleterchr(binds, ','); // 删除最后一个逗号

    insertsql = sformat("insert into %s(%s) values(%s)", 
        stxmltotable.tname, cols.c_str(), binds.c_str());

    // 如果入库参数中指定了表数据不需要更新，就不拼接update语句了，函数返回
    if (stxmltotable.uptbz != 1) return;

    // 拼接更新表的SQL语句
    // sql语句固定根据主键查找记录，即where条件固定为主键
    // update T_ZHOBTMIND1 set t=:1,p=:2,u=:3,wd=:4,wf=:5,r=:6,vis=:7 \
             where 1=1 and obtid=:8 and ddatetime=to_date(:9,'yyyymmddhh24miss')
    // 1=1的目的是为了方便字符串拼接，即where部分的字符串固定以where 1=1开头
    string strset = " set ";            // set部分的字符串
    string strwhere = " where 1=1";     // where部分的字符串
    colseq = 1;                         // 绑定的序号

    for (auto& e : tcols.m_vallcols)
    {
        // 先处理set部分
        if (e.pkseq != 0) continue;

        // 不需要处理keyid
        if (strcmp(e.colname,"keyid") == 0) continue;

        // 需要区分upttime字段、date类型字段和其它字段
        if (strcmp(e.colname,"upttime") == 0)
        {
            // upttime的值固定为sysdate
            strset += sformat("upttime=sysdate,");
        }
        else if (strcmp(e.datatype,"date") == 0)
        {
            // date字段需要将值转成date类型
            strset += sformat("%s=to_date(:%d,'yyyymmddhh24miss),", e.colname, colseq++);
        }
        else
        {
            // 其它字段
            strset += sformat("%s=:%d,", e.colname, colseq++);
        }
    }
    deleterchr(strset, ','); // 删除最后一个逗号

    for (auto& e : tcols.m_vpkcols)
    {
        // 区分date类型字段和非date类型字段
        if (strcmp(e.datatype,"date") == 0)
        {
            strwhere += sformat(" and %s=to_date(:%d,'yyyymmddhh24miss')", e.colname, colseq++);
        }
        else
        {
            strwhere += sformat(" and %s=:%d", e.colname, colseq++);
        }
    }

    updatesql = sformat("update %s%s%s", stxmltotable.tname, strset.c_str(), strwhere.c_str());

    return;
}

void preparesql()
{
    // 为输入变量的数组vcolvalue分配内存
    vcolvalue.resize(tcols.m_vallcols.size());

    // 准备插入的sql
    stmtins.connect(&conn);
    stmtins.prepare(insertsql);

    int colseq = 1;
    for (int i = 0; i < tcols.m_vallcols.size(); ++i)
    {   
        // upttime不需要处理
        if (strcmp(tcols.m_vallcols[i].colname,"upttime") == 0) continue;

        // keyid字段不需要绑定
        if (strcmp(tcols.m_vallcols[i].colname,"keyid") == 0) continue;

        // 其它字段，值存放在容器vcolvalue中
        // vcolvalue每个值的下标与m_vallcols一一对应
        stmtins.bindin(colseq++, vcolvalue[i], tcols.m_vallcols[i].collen);
    }

     // 如果入库参数中指定了表数据不需要更新，就不拼接update语句了，函数返回
    if (stxmltotable.uptbz != 1) return;

    // 准备更新的sql
    stmtupt.connect(&conn);
    stmtupt.prepare(updatesql);

    // 先绑定set部分
    colseq = 1;
    for (int i = 0; i < tcols.m_vallcols.size(); ++i)
    {   
        if (tcols.m_vallcols[i].pkseq != 0) continue;

        // keyid字段不需要处理
        if (strcmp(tcols.m_vallcols[i].colname,"keyid") == 0) continue;

        // upttime不需要绑定
        if (strcmp(tcols.m_vallcols[i].colname,"upttime") == 0) continue;

        // 其它字段，值存放在容器vcolvalue中
        // vcolvalue每个值的下标与m_vallcols一一对应
        stmtupt.bindin(colseq++, vcolvalue[i], tcols.m_vallcols[i].collen);
    }

    // 再绑定where部分
    for (int i = 0; i < tcols.m_vallcols.size(); ++i)
    {
        if (tcols.m_vallcols[i].pkseq == 0) continue;

        stmtupt.bindin(colseq++, vcolvalue[i], tcols.m_vallcols[i].collen);
    }

    return;
}

bool execsql()
{   
    // 没有sql要执行，返回true
    if (strlen(stxmltotable.execsql) == 0) return true;

    stmtpre.connect(&conn);
    stmtpre.prepare(stxmltotable.execsql);
    if (stmtpre.execute() != 0) return false;
    // 这里不提交，因为后续可能会回滚

    return true;
}

void splitbuffer(const string& xmlbuffer)
{
    string temp; // 存放字段值的临时变量

    for (int i = 0; i < tcols.m_vallcols.size(); ++i)
    {
        getxmlbuffer(xmlbuffer, tcols.m_vallcols[i].colname, 
            temp, tcols.m_vallcols[i].collen);

        // 如果是日期时间字段date，提取数字就可以了
        // 也就是说，xml文件中的日期时间只要包含了yyyymmddhh24miss就行，可以是任意分隔符
        if (strcmp(tcols.m_vallcols[i].datatype, "date") == 0)
        {
            picknumber(temp, temp, false, false);
        }
        else if (strcmp(tcols.m_vallcols[i].datatype, "number") == 0)
        {
            // 如果是数值字段number，提取数字、+-符号和圆点
            picknumber(temp, temp, true, true);
        }

        // 如果是字符字段char，不需要任何处理  
        // 这里不能使用这行代码：vcolvalue[i]=temp;
        // 因为sql对象绑定的是C风格的字符串，即char*
        // 直接将temp赋值给vcolvalue[i]，会使用移动赋值
        // 这将导致sql对象绑定的字符串指针指向temp
        vcolvalue[i] = temp.c_str();    
    }

    return;
}

void EXIT(int sig)
{
    logfile.write("[process exit] sig=%d\n", sig);

    exit(0);
}

void _help()
{
    printf("Using:/MDC/bin/tools/xmltodb logfilename xmlbuffer\n\n");

    printf("Sample:/MDC/bin/tools/procctl 10 /MDC/bin/tools/xmltodb /MDC/log/tools/xmltodb_vip.log "\
              "\"<connstr>idc/idcpwd@snorcl11g_132</connstr><charset>Simplified Chinese_China.AL32UTF8</charset>"\
              "<inifilename>/workspace/MDC/idc/ini/xmltodb.xml</inifilename>"\
              "<xmlpath>/MDC/data/xmltodb/vip</xmlpath><xmlpathbak>/MDC/data/xmltodb/vipbak</xmlpathbak>"\
              "<xmlpatherr>/MDC/data/xmltodb/viperr</xmlpatherr>"\
              "<timetvl>5</timetvl><timeout>63</timeout><pname>xmltodb_vip</pname>\"\n\n");

    printf("本程序是数据中心的公共功能模块，用于把xml文件入库到Oracle的表中\n");
    printf("logfilename   本程序运行的日志文件\n");
    printf("xmlbuffer     本程序运行的参数，用xml表示，具体如下：\n\n");

    printf("connstr     数据库的连接参数，格式：username/passwd@tnsname\n");
    printf("charset     数据库的字符集，这个参数要与数据源数据库保持一致，否则会出现中文乱码的情况\n");
    printf("inifilename 数据入库的参数配置文件\n");
    printf("xmlpath     待入库xml文件存放的目录\n");
    printf("xmlpathbak  xml文件入库后的备份目录\n");
    printf("xmlpatherr  入库失败的xml文件存放的目录\n");
    printf("timetvl     扫描xmlpath目录的时间间隔（执行入库任务的时间间隔），单位：秒，视业务需求而定，2-30之间\n");
    printf("timeout     本程序的超时时间，单位：秒，视xml文件大小而定，建议设置30以上\n");
    printf("pname       进程名，尽可能采用易懂的、与其它进程不同的名称，方便故障排查\n\n");

    cout << "\n\n"
    "Using:/MDC/bin/tools/xmltodb logfilename xmlbuffer\n\n"
    "Example:\n"
    "/MDC/bin/tools/procctl 10 /MDC/bin/tools/xmltodb /MDC/log/tools/xmltodb_vip.log "
    "\"<connstr>idc/idcpwd@snorcl11g_132</connstr><charset>Simplified Chinese_China.AL32UTF8</charset>"
    "<inifilename>/workspace/MDC/idc/ini/xmltodb.xml</inifilename>"
    "<xmlpath>/MDC/data/xmltodb/vip</xmlpath><xmlpathbak>/MDC/data/xmltodb/vipbak</xmlpathbak>"
    "<xmlpatherr>/MDC/data/xmltodb/viperr</xmlpatherr>"
    "<timetvl>5</timetvl><timeout>63</timeout><pname>xmltodb_vip</pname>\"\n\n"

    "本程序是数据中心的公共功能模块，用于把xml文件入库到Oracle的表中\n"
    "logfilename   本程序运行的日志文件\n"
    "xmlbuffer     本程序运行的参数，用xml表示，具体如下：\n\n"

    "connstr     数据库的连接参数，格式：username/passwd@tnsname\n"
    "charset     数据库的字符集，这个参数要与数据源数据库保持一致，否则会出现中文乱码的情况\n"
    "inifilename 数据入库的参数配置文件\n"
    "xmlpath     待入库xml文件存放的目录\n"
    "xmlpathbak  xml文件入库后的备份目录\n"
    "xmlpatherr  入库失败的xml文件存放的目录\n"
    "timetvl     扫描xmlpath目录的时间间隔（执行入库任务的时间间隔），单位：秒，视业务需求而定，2-30之间\n"
    "timeout     本程序的超时时间，单位：秒，视xml文件大小而定，建议设置30以上\n"
    "pname       进程名，尽可能采用易懂的、与其它进程不同的名称，方便故障排查\n\n";
}

bool _xmltoarg(const string& xmlbuffer)
{
    memset(&starg,0,sizeof(struct st_arg));

    getxmlbuffer(xmlbuffer,"connstr",starg.connstr,127);
    if (strlen(starg.connstr)==0) { logfile.write("connstr is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"charset",starg.charset,63);
    if (strlen(starg.charset)==0) { logfile.write("charset is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"inifilename",starg.inifilename,255);
    if (strlen(starg.inifilename)==0) { logfile.write("inifilename is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"xmlpath",starg.xmlpath,255);
    if (strlen(starg.xmlpath)==0) { logfile.write("xmlpath is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"xmlpathbak",starg.xmlpathbak,255);
    if (strlen(starg.xmlpathbak)==0) { logfile.write("xmlpathbak is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"xmlpatherr",starg.xmlpatherr,255);
    if (strlen(starg.xmlpatherr)==0) { logfile.write("xmlpatherr is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"timetvl",starg.timetvl);
    if (starg.timetvl< 2) starg.timetvl=2;   
    if (starg.timetvl>30) starg.timetvl=30;

    getxmlbuffer(xmlbuffer,"timeout",starg.timeout);
    if (starg.timeout==0) { logfile.write("timeout is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"pname",starg.pname,63);
    if (strlen(starg.pname)==0) { logfile.write("pname is null.\n"); return false; }

    return true;
}