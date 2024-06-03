/*
    syncref.cpp
    本程序是共享平台的公共功能模块，采用刷新的方法同步Oracle数据库之间的表
    使用dblink实现同步，将远程表的数据同步到本地表
    常见的使用场景为：远程表是管理数据的主表，本地表是提供数据访问的业务表
    同步的语义：
        两表同一记录对应的数据应相同
        远程表新增记录要插入本地表中
    同步的过程：
        本地表根据传入的条件删除记录，条件为空则删除所有记录
        远程表根据传入的条件查询记录，条件为空则查询所有记录
        将远程表查询到的记录插入本地表
    刷新同步分为分批同步和不分批同步
    分批同步需要唯一键（rowid效率最高）和每次操作的最大记录数
*/

#include "_tools.h"

// 程序运行的参数
struct st_arg       
{
    char localconnstr[128];
    char charset[64];
    char linktname[32];
    char localtname[32];
    char remotecols[1024];
    char localcols[1024];
    char rwhere[1024];
    char lwhere[1024];
    int synctype;
    char remoteconnstr[128];
    char remotetname[32];
    char remotekeycol[32];
    char localkeycol[32];
    int keylen;
    int maxcount;
    int timeout;
    char pname[64];
}starg;

clogfile logfile;       // 日志
cpactive pactive;       // 进程心跳
connection connloc;     // 本地数据库连接
connection connrem;     // 远程数据库连接
ctcols tcols;           // 获取表的字段的工具类

bool _syncref();        // 业务处理的主函数

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

    // 连接本地数据库
    if (connloc.connecttodb(starg.localconnstr, starg.charset) != 0)
    {
        logfile.write("[connect to database failed] connloc.connecttodb(%s, %s)\n", 
            starg.localconnstr, starg.charset);
        EXIT(-1);
    }

    if ((strlen(starg.remotecols) == 0)  || (strlen(starg.localcols) == 0))
    {
        // 如果remotecols或localcols为空，就用localtname的字段填充
        if (tcols.allcols(connloc, starg.localtname) == false)
        {
            logfile.write("[_xmltoarg: get all cols from local table failed] tcols.allcols(%s, %s)\n", 
                starg.localconnstr, starg.localtname);
            return false;
        }

        if (strlen(starg.remotecols) == 0) strcpy(starg.remotecols, tcols.m_allcols.c_str());
        if (strlen(starg.localcols) == 0)  strcpy(starg.localcols, tcols.m_allcols.c_str());
    }

    _syncref();

    return 0;
}

bool _syncref()
{
    ctimer timer;

    sqlstatement stmtdel(&connloc);  // 本地表删除的sql
    sqlstatement stmtins(&connloc);  // 本地表插入的sql

    // ---不分批刷新---
    // delete from T_ZHOBTCODE2 where stid like '57%';
    // insert into T_ZHOBTCODE2(stid,cityname,provname,lat,lon,height,upttime,recid)
    //        select obtid,cityname,provname,lat,lon,height,upttime,keyid from T_ZHOBTCODE1@db128 where obtid like '57%';
    if (starg.synctype == 1)
    {
        // 删除本地表的记录
        stmtdel.prepare("delete from %s %s", starg.localtname, starg.lwhere);
        if (stmtdel.execute() != 0)
        {
            logfile.write("[_syncref: delete from local table failed] sql: %s\nerror: %s\n", 
                stmtdel.sql(), stmtdel.message());
            return false;
        }

        // 将远程表的记录插入本地表
        stmtins.prepare("insert into %s(%s) select %s from %s %s", 
            starg.localtname, starg.localcols, starg.remotecols, starg.linktname, starg.rwhere);
        if (stmtins.execute() != 0)
        {
            logfile.write("[_syncref: insert into local table failed] sql: %s\nerror: %s\n", 
                stmtins.sql(), stmtins.message());
            return false;
        }

        logfile.write("[_syncref: sync %s to %s %drows in %.2fsec]\n", 
            starg.linktname, starg.localtname, stmtins.rpc(), timer.elapsed());

        return true;
    }

    // ---分批刷新--- 
    // 连接远程表
    if (connrem.connecttodb(starg.remoteconnstr, starg.charset) != 0)
    {
        logfile.write("[_syncref: connect to remote database failed] connrem.connecttodb(%s, %s)\n", 
            starg.remoteconnstr, starg.charset);
        return false;
    }

    // 查询远程表的sql
    // select obtid from T_ZHOBTCODE1 where obtid like '57%'
    sqlstatement stmtsel(&connrem);
    stmtsel.prepare("select %s from %s %s", starg.remotekeycol, starg.remotetname, starg.rwhere);
    char remkeyvalue[starg.keylen + 1];
    stmtsel.bindout(1, remkeyvalue, starg.keylen);

    // 拼接绑定部分的字符串
    string binds;
    for (int i = 1; i <= starg.maxcount; ++i)
        binds += sformat(":%d,", i);
    deleterchr(binds, ',');

    // 保存唯一键值的字符串数组
    char keyvalues[starg.maxcount][starg.keylen + 1];

    // 删除本地表记录的sql
    // delete from T_ZHOBTCODE2 where stid in (:1,:2,...,:maxcount)
    stmtdel.prepare("delete from %s where %s in (%s)", starg.localtname, starg.localkeycol, binds.c_str());
    for (int i = 1; i <= starg.maxcount; ++i)
        stmtdel.bindin(i, keyvalues[i - 1]);

    // 插入本地表的sql
    // insert into T_ZHOBTCODE2(stid,cityname,provname,lat,lon,height,upttime,recid)
    //     select obtid,cityname,provname,lat,lon,height,upttime,keyid 
    //         from T_ZHOBTCODE1@db128 where obtid in (:1,:2,...,:maxcount)
    stmtins.prepare("insert into %s(%s) select %s from %s where %s in (%s)", 
        starg.localtname, starg.localcols, starg.remotecols, starg.linktname, starg.remotekeycol, binds.c_str());
    for (int i = 1; i <= starg.maxcount; ++i)
        stmtins.bindin(i, keyvalues[i - 1]);

    int rowcount=0;    // 记录从结果集中已获取记录的计数器
    memset(keyvalues, 0, sizeof(keyvalues));

    if (stmtsel.execute() != 0)
    {
        logfile.write("[_syncref: select from remote table failed] sql: %s\nerror: %s\n", 
            stmtsel.sql(), stmtsel.message());
        return false;
    }

    while (stmtsel.next() == 0)
    {
        strcpy(keyvalues[rowcount++], remkeyvalue);

        if (rowcount == starg.maxcount)
        {
            if (stmtdel.execute() != 0)
            {
                logfile.write("[_syncref: delete from local table failed] sql: %s\nerror: %s\n", 
                    stmtdel.sql(), stmtdel.message());
                return false;
            }

            if (stmtins.execute() != 0)
            {
                logfile.write("[_syncref: insert into local table failed] sql: %s\nerror: %s\n", 
                    stmtins.sql(), stmtins.message());
                return false;
            }

            connloc.commit();

            rowcount = 0;
            memset(keyvalues, 0, sizeof(keyvalues));

            pactive.uptatime();
        }
    }

    if (rowcount > 0)
    {
        if (stmtdel.execute() != 0)
            {
                logfile.write("[_syncref: delete from local table failed] sql: %s\nerror: %s\n", 
                    stmtdel.sql(), stmtdel.message());
                return false;
            }

            if (stmtins.execute() != 0)
            {
                logfile.write("[_syncref: insert into local table failed] sql: %s\nerror: %s\n", 
                    stmtins.sql(), stmtins.message());
                return false;
            }

            connloc.commit();

            pactive.uptatime();
    }

    logfile.write("[_syncref: sync %s to %s %drows in %.2fsec]\n", 
        starg.linktname, starg.localtname, stmtins.rpc(), timer.elapsed());

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
    "Using:syncref logfilename xmlbuffer\n\n"

    "Example:\n"
    "不分批同步，把T_ZHOBTCODE1@db132同步到T_ZHOBTCODE2\n"
    "/MDC/bin/tools/procctl 10 /MDC/bin/tools/syncref /MDC/log/tools/syncref_ZHOBTCODE2.log "
    "\"<localconnstr>idc/idcpwd@snorcl11g_132</localconnstr><charset>Simplified Chinese_China.AL32UTF8</charset>"
    "<linktname>T_ZHOBTCODE1@db132</linktname><localtname>T_ZHOBTCODE2</localtname>"
    "<remotecols>obtid,cityname,provname,lat,lon,height,upttime,keyid</remotecols>"
    "<rwhere>where obtid like '57%%'</rwhere><lwhere>where stid like '57%%'</lwhere>"
    "<synctype>1</synctype><timeout>50</timeout><pname>syncref_ZHOBTCODE2</pname>\"\n\n"

    "分批同步，把T_ZHOBTCODE1@db132同步到T_ZHOBTCODE3\n"
    "因为测试的需要，xmltodb程序每次会删除T_ZHOBTCODE1@db132中的数据，全部的记录重新入库，keyid会变\n"
    "所以，以下脚本不能用keyid，要用obtid\n"
    "/MDC/bin/tools/procctl 10 /MDC/bin/tools/syncref /MDC/log/tools/syncref_ZHOBTCODE2.log "
    "\"<localconnstr>idc/idcpwd@snorcl11g_132</localconnstr><charset>Simplified Chinese_China.AL32UTF8</charset>"
    "<linktname>T_ZHOBTCODE1@db132</linktname><localtname>T_ZHOBTCODE3</localtname>"
    "<remotecols>obtid,cityname,provname,lat,lon,height,upttime,keyid</remotecols>"
    "<localcols>stid,cityname,provname,lat,lon,height,upttime,recid</localcols>"
    "<rwhere>where obtid like '57%%'</rwhere>"
    "<synctype>2</synctype><remoteconnstr>idc/idcpwd@snorcl11g_132</remoteconnstr>"
    "<remotetname>T_ZHOBTCODE1</remotetname><remotekeycol>obtid</remotekeycol>"
    "<localkeycol>stid</localkeycol><keylen>5</keylen>"
    "<maxcount>10</maxcount><timeout>50</timeout><pname>syncref_ZHOBTCODE3</pname>\"\n\n"

    "分批同步，把T_ZHOBTMIND1@db132同步到T_ZHOBTMIND2\n"
    "/MDC/bin/tools/procctl 10 /MDC/bin/tools/syncref /MDC/log/tools/syncref_ZHOBTMIND2.log "
    "\"<localconnstr>idc/idcpwd@snorcl11g_132</localconnstr><charset>Simplified Chinese_China.AL32UTF8</charset>"
    "<linktname>T_ZHOBTMIND1@db132</linktname><localtname>T_ZHOBTMIND2</localtname>"
    "<remotecols>obtid,ddatetime,t,p,u,wd,wf,r,vis,upttime,keyid</remotecols>"
    "<localcols>stid,ddatetime,t,p,u,wd,wf,r,vis,upttime,recid</localcols>"
    "<rwhere>where ddatetime>sysdate-10/1440</rwhere>"
    "<synctype>2</synctype><remoteconnstr>idc/idcpwd@snorcl11g_132</remoteconnstr>"
    "<remotetname>T_ZHOBTMIND1</remotetname><remotekeycol>keyid</remotekeycol>"
    "<localkeycol>recid</localkeycol><keylen>15</keylen>"
    "<maxcount>10</maxcount><timeout>50</timeout><pname>syncref_ZHOBTMIND2</pname>\"\n\n"

    "本程序是共享平台的公共功能模块，采用刷新的方法同步Oracle数据库之间的表\n"
    "logfilename   本程序运行的日志文件\n"
    "xmlbuffer     本程序运行的参数，用xml表示，具体如下：\n\n"

    "localconnstr        数据库的连接参数，格式：username/passwd@tnsname\n"
    "charset        数据库的字符集，这个参数要与数据源数据库保持一致，否则会出现中文乱码的情况\n"
    "linktname      dblink指向的远程表名，如T_ZHOBTCODE1@db132\n"
    "localtname     本地表名，如T_ZHOBTCODE2\n"
    "remotecols     远程表的字段列表，用于填充在select和from之间，所以，remotecols可以是真实的字段，"\
                    "也可以是函数的返回值或者运算结果如果本参数为空，就用localtname表的字段列表填充\n"
    "localcols      本地表的字段列表，与remotecols不同，它必须是真实存在的字段如果本参数为空，"\
                    "就用localtname表的字段列表填充\n"
    "rwhere         同步数据的条件，填充在远程表的查询语句之后，为空则表示同步全部的记录\n"
    "lwhere         同步数据的条件，填充在本地表的删除语句之后，为空则表示同步全部的记录\n"
    "synctype       同步方式：1-不分批刷新；2-分批刷新\n"
    "remoteconnstr  远程数据库的连接参数，格式与localconnstr相同，当synctype==2时有效\n"
    "remotetname    没有dblink的远程表名，当synctype==2时有效\n"
    "remotekeycol   远程表的键值字段名，必须是唯一的，当synctype==2时有效\n"
    "localkeycol    本地表的键值字段名，必须是唯一的，当synctype==2时有效\n"
    "keylen         键值字段的长度，当synctype==2时有效\n"
    "maxcount       执行一次同步操作的记录数，当synctype==2时有效\n"
    "timeout        本程序的超时时间，单位：秒，视数据量的大小而定，建议设置30以上\n"
    "pname          本程序运行时的进程名，尽可能采用易懂的、与其它进程不同的名称，方便故障排查\n\n"

    "注意：\n"
    "1）remotekeycol和localkeycol字段的选取很重要，如果是自增字段，那么在远程表中数据生成后自增字段的值不可改变，否则同步会失败；\n"
    "2）当远程表中存在delete操作时，无法分批刷新，因为远程表的记录被delete后就找不到了，无法从本地表中执行delete操作\n\n";
}

bool _xmltoarg(const string& xmlbuffer)
{
    memset(&starg,0,sizeof(struct st_arg));

    getxmlbuffer(xmlbuffer,"localconnstr",starg.localconnstr,127);
    if (strlen(starg.localconnstr)==0) { logfile.write("localconnstr is null\n"); return false; }

    getxmlbuffer(xmlbuffer,"charset",starg.charset,63);
    if (strlen(starg.charset)==0) { logfile.write("charset is null\n"); return false; }

    getxmlbuffer(xmlbuffer,"linktname",starg.linktname,31);
    if (strlen(starg.linktname)==0) { logfile.write("linktname is null\n"); return false; }

    getxmlbuffer(xmlbuffer,"localtname",starg.localtname,31);
    if (strlen(starg.localtname)==0) { logfile.write("localtname is null\n"); return false; }

    getxmlbuffer(xmlbuffer,"remotecols",starg.remotecols,1023); 

    getxmlbuffer(xmlbuffer,"localcols",starg.localcols,1023);

    getxmlbuffer(xmlbuffer,"rwhere",starg.rwhere,1023);

    getxmlbuffer(xmlbuffer,"lwhere",starg.lwhere,1023);

    getxmlbuffer(xmlbuffer,"synctype",starg.synctype);
    if ((starg.synctype != 1) && (starg.synctype != 2)) { logfile.write("synctype not in {1, 2}\n"); return false; }

    if (starg.synctype == 2)
    {
        getxmlbuffer(xmlbuffer,"remoteconnstr",starg.remoteconnstr,127);
        if (strlen(starg.remoteconnstr)==0) { logfile.write("remoteconnstr is null\n"); return false; }

        getxmlbuffer(xmlbuffer,"remotetname",starg.remotetname,31);
        if (strlen(starg.remotetname)==0) { logfile.write("remotetname is null\n"); return false; }

        getxmlbuffer(xmlbuffer,"remotekeycol",starg.remotekeycol,31);
        if (strlen(starg.remotekeycol)==0) { logfile.write("remotekeycol is null\n"); return false; }

        getxmlbuffer(xmlbuffer,"localkeycol",starg.localkeycol,31);
        if (strlen(starg.localkeycol)==0) { logfile.write("localkeycol is null\n"); return false; }

        getxmlbuffer(xmlbuffer,"maxcount",starg.maxcount);

        getxmlbuffer(xmlbuffer,"keylen",starg.keylen);
        if (starg.keylen==0) { logfile.write("keylen is null.\n"); return false; }
    }

    getxmlbuffer(xmlbuffer,"timeout",starg.timeout);
    if (starg.timeout==0) { logfile.write("timeout is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"pname",starg.pname,63);
    if (strlen(starg.pname)==0) { logfile.write("pname is null.\n"); return false; }

    return true;
}