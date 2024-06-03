/*
    migratetable.cpp
    本程序是共享平台的公共功能模块，用于迁移表中的数据
    数据迁移用于将重要的表中不重要的数据迁移到其它表中，以提高效率
    例如：气象数据中，实时数据最重要，过时的数据就可以迁移至历史表中
    数据迁移的步骤：
        1.从源表中查询迁移的记录，即满足where条件的记录（通过唯一键来定位记录，使用rowid效率最高）
        2.向目的表插入记录，再将源表中的记录删除
        3.第二步分多次执行，每次最多maxcount条记录，这样做会降低效率，但可以防止产生大事务
*/

#include "_tools.h"

// 程序运行的参数
struct st_arg       
{
    char connstr[128];
    char tname[32];
    char totname[32];
    char keycol[32];
    char where[1024];
    int maxcount;
    char starttime[32];
    int timeout;
    char pname[64];
}starg;

clogfile logfile;       // 日志
cpactive pactive;       // 进程心跳
connection conn;        // 数据库连接
ctcols tcols;           // 获取表的字段的工具类

bool _migratetable();   // 业务处理主函数

bool instarttime();     // 判断当前时间是否在程序运行的时间区间内
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

    // 判断是否在运行时间内
    if (instarttime() == false) return 0;

    // 配置心跳信息
    pactive.addpinfo(starg.timeout, starg.pname);

    // 连接数据库
    // 本程序的sql操作不涉及字符集，charset参数可以随便填
    if (conn.connecttodb(starg.connstr, "Simplified Chinese_China.AL32UTF8") != 0)
    {
        logfile.write("[connect to database failed] conn.connecttodb(%s)\n", starg.connstr);
        EXIT(-1);
    }
    logfile.write("[connect to database(%s) success]\n", starg.connstr);

    _migratetable();

    return 0;
}

bool _migratetable()
{
    ctimer timer;       // 用于计时
    char keyvalue[21];  // 保存唯一键的值
    char keyvalues[starg.maxcount][21]; // 保存唯一键的值的数组，对应maxcount条记录

    // 准备查询源表的sql，只查询keycol
    // 例：select rowid from T_ZHOBTMIND1 where ddatetime<sysdate-1
    sqlstatement stmtsel(&conn);
    stmtsel.prepare("select %s from %s %s", starg.keycol, starg.tname, starg.where);
    stmtsel.bindout(1, keyvalue);

    // 准备插入目的表的sql
    // 每次最多maxcount条记录
    // 例：insert into T_ZHOBTMIND1_HIS(全部字段) select 全部字段 from T_ZHOBTMIND1 where rowid in (:1,:2,...,:maxcount)
    tcols.allcols(conn, starg.tname);

    string binds; // 绑定部分的字符串
    for (int i = 1; i <= starg.maxcount; ++i)
        binds += sformat(":%lu,", i);
    deleterchr(binds, ',');

    sqlstatement stmtins(&conn);
    stmtins.prepare("insert into %s(%s) select %s from %s where %s in (%s)", 
        starg.totname, tcols.m_allcols.c_str(), tcols.m_allcols.c_str(), starg.tname, starg.keycol, binds.c_str());

    for (int i = 1; i <= starg.maxcount; ++i)
        stmtins.bindin(i, keyvalues[i - 1]);
    
    // 准备删除源表记录的sql
    // 每次最多maxcount条记录
    // 例：delete from T_ZHOBTMIND1 where rowid in (:1,:2,...,:maxcount)
    binds.clear(); // 绑定部分的字符串
    for (int i = 1; i <= starg.maxcount; ++i)
        binds += sformat(":%lu,", i);
    deleterchr(binds, ',');

    sqlstatement stmtdel(&conn);
    stmtdel.prepare("delete from %s where %s in (%s)", 
        starg.tname, starg.keycol, binds.c_str());

    for (int i = 1; i <= starg.maxcount; ++i)
        stmtdel.bindin(i, keyvalues[i - 1]);

    // 查询记录
    if (stmtsel.execute() != 0)
    {
        logfile.write("[_migratetable: execute select sql failed] sql: %s\nerror: %s\n", 
            stmtsel.sql(), stmtsel.message());
        return false;
    }

    int rowcount=0; // 已获取的记录数
    memset(keyvalues, 0, sizeof(keyvalues));

    while (true)
    {
        memset(keyvalue, 0, sizeof(keyvalue));
        if (stmtsel.next() != 0) break;
        strcpy(keyvalues[rowcount++], keyvalue);  // 将查询到的唯一键值存放到数组中

        // 如果计数达到maxcount就执行一次迁移操作
        if (rowcount == starg.maxcount)
        {

            if (stmtins.execute() != 0)
            {
                logfile.write("[_migratetable: execute insert sql failed] sql: %s\nerror: %s\n", 
                    stmtsel.sql(), stmtsel.message());
                return false;
            }
            if (stmtdel.execute() != 0)
            {
                logfile.write("[_migratetable: execute delete sql failed] sql: %s\nerror: %s\n", 
                    stmtsel.sql(), stmtsel.message());
                return false;
            }
            conn.commit(); // 提交事务

            memset(keyvalues, 0, sizeof(keyvalues)); // 重置值数组
            rowcount = 0;
            pactive.uptatime();
        }
    }

    if (rowcount > 0) // 如果还有剩余的记录
    {
        if (stmtins.execute() != 0)
        {
            logfile.write("[_migratetable: execute insert sql failed] sql: %s\nerror: %s\n", 
                stmtsel.sql(), stmtsel.message());
            return false;
        }
        if (stmtdel.execute() != 0)
        {
            logfile.write("[_migratetable: execute delete sql failed] sql: %s\nerror: %s\n", 
                stmtsel.sql(), stmtsel.message());
            return false;
        }
        conn.commit(); // 提交事务

        pactive.uptatime();
    }

    logfile.write("[_migratetable] migrate from %s to %s %d rows in %.02fsec\n", 
        starg.tname, starg.totname, stmtsel.rpc(), timer.elapsed());

    return true;
}

bool instarttime()
{
    if (strlen(starg.starttime) != 0) 
    {
        if (strstr(starg.starttime, ltime1("24hh", 0).c_str()) == 0) return false;
    }

    return true;
}

void EXIT(int sig)
{
    logfile.write("[process exit] sig=%d\n", sig);

    exit(0);
}

void _help()
{
    cout << "\n\n"
    "Using:migratetable logfilename xmlbuffer\n\n"
    "Example:\n"
    "/MDC/bin/tools/procctl 3600 /MDC/bin/tools/migratetable /MDC/log/tools/migratetable_ZHOBTMIND1.log "
    "\"<connstr>idc/idcpwd@snorcl11g_132</connstr><tname>T_ZHOBTMIND1</tname>"\
    "<totname>T_ZHOBTMIND1_HIS</totname><keycol>rowid</keycol><where>where ddatetime<sysdate-0.03</where>"\
    "<maxcount>10</maxcount><starttime>22,23,00,01,02,03,04,05,06,13</starttime>"\
    "<timeout>120</timeout><pname>migratetable_ZHOBTMIND1</pname>\"\n\n"

    "本程序是共享平台的公共功能模块，用于迁移表中的数据\n"
    "logfilename   本程序运行的日志文件\n"
    "xmlbuffer     本程序运行的参数，用xml表示，具体如下：\n\n"

    "connstr     数据库的连接参数，格式：username/passwd@tnsname\n"
    "charset     数据库的字符集，这个参数要与数据源数据库保持一致，否则会出现中文乱码的情况\n"
    "tname       待迁移数据表的表名，例如T_ZHOBTMIND1\n"
    "totname     目的表名，例如T_ZHOBTMIND1_HIS\n"
    "keycol      待迁移数据表的唯一键字段名，可以用记录编号，如keyid，建议用rowid，效率最高\n"
    "where       待迁移的数据需要满足的条件，即SQL语句中的where部分\n"
    "maxcount    执行一次SQL语句删除的记录数，建议在100-500之间\n"
    "starttime   程序运行的时间区间，例如02,13表示：如果程序运行时，踏中02时和13时则运行，其它时间不运行"\
                "如果starttime为空，本参数将失效，只要本程序启动就会执行数据迁移，"\
                "为了减少对数据库的压力，数据迁移一般在业务最闲的时候时进行\n"
    "timeout     本程序的超时时间，单位：秒，视xml文件大小而定，建议设置30以上\n"
    "pname       进程名，尽可能采用易懂的、与其它进程不同的名称，方便故障排查\n\n";
}

bool _xmltoarg(const string& xmlbuffer)
{
    memset(&starg,0,sizeof(struct st_arg));

    getxmlbuffer(xmlbuffer,"connstr",starg.connstr,127);
    if (strlen(starg.connstr)==0) { logfile.write("connstr is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"tname",starg.tname,31);
    if (strlen(starg.tname)==0) { logfile.write("tname is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"totname",starg.totname,31);
    if (strlen(starg.totname)==0) { logfile.write("totname is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"keycol",starg.keycol,31);
    if (strlen(starg.keycol)==0) { logfile.write("keycol is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"where",starg.where,1023);
    if (strlen(starg.where)==0) { logfile.write("where is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"maxcount",starg.maxcount);

    getxmlbuffer(xmlbuffer,"starttime",starg.starttime, 31);

    getxmlbuffer(xmlbuffer,"timeout",starg.timeout);
    if (starg.timeout==0) { logfile.write("timeout is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"pname",starg.pname,63);
    if (strlen(starg.pname)==0) { logfile.write("pname is null.\n"); return false; }

    return true;
}