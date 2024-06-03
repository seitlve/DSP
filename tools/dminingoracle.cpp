/*
    dminingoracle.cpp
    本程序是数据中心的公共功能模块，用于从Oracle数据库源表抽取数据，生成xml文件
    数据抽取分为增量抽取和全量抽取
    如果向程序传递了递增字段，则为增量抽取
    对于增量抽取，需要额外保存每次抽取的数据中递增字段的最大值
    可以使用文件保存或存入数据库表中，支持自动创建文件或数据库表

    程序需要传入完整的查询语句，即
    select 字段名列表 from 表名 where 查询条件
    如果是增量抽取，查询条件必须以此开头：where 递增字段>上次抽取的最大值（即递增字段应该第一个绑定）
    字段名列表以及对应的长度也需要传入
    同时，递增字段必须在查询的字段列表中，因为需要查询递增字段的值来更新最大值
    递增字段不是严格以1为间隔的，所以需要查询
*/
#include "_public.h"
#include "_ooci.h"

using namespace idc;

// 程序运行的参数
struct st_arg       
{
    char connstr[128];
    char charset[64];
    char selectsql[1024];
    char fieldstr[512];
    char fieldlen[512];
    char outpath[256];
    char bfilename[32];
    char efilename[32];
    int maxcount;
    char starttime[64];
    char incfield[32];
    char incfilename[256];
    char connstr1[128];
    int timeout;
    char pname[64];
}starg;

clogfile logfile;       // 日志
cpactive pactive;       // 进程心跳
connection conn;        // 数据库连接

ccmdstr fieldname;      // 用于存放字段名
ccmdstr fieldlen;       // 用于存放字段长度
int maxincvalue;        // 递增字段最大值
int incfieldpos = -1;   // 递增字段在fieldstr中的位置

bool readincfield();    // 读取递增字段最大值。
bool _dminingoracle();  // 数据抽取的主函数
bool writeincfield();   // 将最大值写入数据库表或文件中

bool instarttime();     // 用于判断程序是否处于运行时间
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

    // 判断是否在执行时间内
    if (instarttime() == false) return 0;

    // 配置心跳信息
    pactive.addpinfo(starg.timeout, starg.pname);

    // 读取递增字段最大值
    if (readincfield() == false)
    {
        logfile.write("[read maxincvalue failed]\n");
        EXIT(-1);
    }

    // 连接数据库
    if (conn.connecttodb(starg.connstr, starg.charset) != 0)
    {
        logfile.write("[connect to database failed] conn.connecttodb(%s, %s)\n", starg.connstr, starg.charset);
        EXIT(-1);
    }
    logfile.write("[connect to database(%s) success]\n", starg.connstr);

    _dminingoracle();

    return 0;
}

bool readincfield()
{
    maxincvalue = 0; // 初始化递增字段最大值

    if (strlen(starg.incfield) == 0) return true;

    // 判断递增字段是否在fieldstr中
    for (int i = 0; i < fieldname.size(); ++i)
    {
        if (fieldname[i] == starg.incfield)
        {
            incfieldpos = i;
            break;
        }
    }

    if (incfieldpos == -1)
    {
        logfile.write("递增字段名%s不在列表%s中\n", starg.incfield, starg.fieldstr);
        return false;
    }

    // 优先查询数据库
    if (strlen(starg.connstr1) > 0)
    {
        // 连接存放最大值的数据库
        connection conn1;
        if (conn1.connecttodb(starg.connstr1, starg.charset) != 0)
        {
            logfile.write("[readincfield: connect to database failed] conn1.connecttodb(%s, %s)\n", 
                starg.connstr1, starg.charset);
            return false;
        }

        // 准备查询语句
        // 表名固定为T_MAXINCVALUE，字段固定为pname和maxincvalue
        // sql语句固定为：select maxincvalue from T_MAXINCVALUE where pname=:1
        sqlstatement stmtsel(&conn1);
        stmtsel.prepare("select maxincvalue from T_MAXINCVALUE where pname=:1");
        stmtsel.bindin(1, starg.pname);
        stmtsel.bindout(1, maxincvalue);
        // 如果执行失败，maxincvalue为0
        stmtsel.execute();
        stmtsel.next();
    }
    else if (strlen(starg.incfilename) > 0)
    {
        cifile ifile;
        // 如果打开文件失败，可能是没有文件或文件丢失，maxincvalue为0
        if (ifile.open(starg.incfilename) == false) return true;

        string temp;
        ifile.readline(temp);
        maxincvalue = stoi(temp);
    }
    else return false; // 如果两个参数都没有

    logfile.write("[maxincvalue of data mined last time] %ld\n", maxincvalue);

    return true;
}

bool _dminingoracle()
{
    // 准备查询语句
    sqlstatement stmtsel(&conn);
    stmtsel.prepare(starg.selectsql);

    // 绑定参数
    string fieldvalue[fieldname.size()]; // 用于存放一条查询记录
    for (int i = 0; i < fieldname.size(); ++i)
        stmtsel.bindout(i + 1, fieldvalue[i], stoi(fieldlen[i]));

    // 如果是递增查询，还需要绑定where条件中递增字段对应的值
    if  (strlen(starg.incfield) > 0) stmtsel.bindin(1, maxincvalue);

    // 执行sql语句
    if (stmtsel.execute() != 0)
    {
        logfile.write("[_dminingoracle: execute select sql failed] sql: %s\nerror: %s\n", 
            stmtsel.sql(), stmtsel.message());
        return false;
    }

    pactive.uptatime();

    // sql查询只执行一次，得到所有记录；将记录写入文件分多次，每个文件最多记录maxcount条
    cofile ofile;
    string xmlfile; // 输出的xml文件名，例如：ZHOBTCODE_20240519162835_togxpt_1.xml
    int iseq = 1;   // 输出xml文件的序号

    while (stmtsel.next() == 0)
    {
        if (ofile.isopen() == false) // 如果文件未打开
        {
            sformat(xmlfile, "%s/%s_%s_%s_%d.xml", 
                starg.outpath, starg.bfilename, ltime1("yyyymmddhh24miss", 0).c_str(), starg.efilename, iseq++);
            if (ofile.open(xmlfile) == false)
            {
                logfile.write("[_dminingoracle: open file failed] ofile.open(%s)\n", xmlfile.c_str());
                return false;
            }

            ofile.writeline("<data>\n"); // 写入数据集开始的标志
        }

        // 将结果集写入文件中
        for (int i = 0; i <fieldname.size(); ++i)
            ofile.writeline("<%s>%s</%s>", 
                fieldname[i].c_str(), fieldvalue[i].c_str(), fieldname[i].c_str());
        ofile.writeline("<endl/>\n"); // 写入每行结束标志

        // 如果记录数达到starg.maxcount行就关闭当前文件
        if ((starg.maxcount > 0) && (stmtsel.rpc() % starg.maxcount == 0))
        {
            ofile.writeline("</data>\n"); // 写入文件的结束标志
            if (ofile.closeandrename() == false)
            {
                logfile.write("[_dminingoracle: close and rename file failed] ofile.closeandrename()\n");
                return false;
            }
            logfile.write("[generate file %s(%d)]\n", xmlfile.c_str(), starg.maxcount);

            pactive.uptatime();
        }

        // 更新递增字段最大值
        if ((strlen(starg.incfield) > 0) && (maxincvalue < stoi(fieldvalue[incfieldpos])))
            maxincvalue = stoi(fieldvalue[incfieldpos]);
    }

    // 如果maxcount==0或者向xml文件中写入的记录数不足maxcount，关闭文件
    if ((starg.maxcount == 0) || (stmtsel.rpc() % starg.maxcount > 0))
    {
        ofile.writeline("</data>\n"); // 写入文件的结束标志
        if (ofile.closeandrename() == false)
        {
            logfile.write("[_dminingoracle: close and rename file failed] ofile.closeandrename()\n");
            return false;
        }

        if (starg.maxcount == 0)
            logfile.write("[generate file %s(%d)]\n", xmlfile.c_str(), stmtsel.rpc());
        else
            logfile.write("[generate file %s(%d)]\n", xmlfile.c_str(), stmtsel.rpc() % starg.maxcount);

        pactive.uptatime();    
    }

    // 更新最大值
    if (stmtsel.rpc() > 0) writeincfield();
    
    return true;
}

bool writeincfield()
{
    if (strlen(starg.incfield) == 0) return true;

    if (strlen(starg.connstr1) > 0)
    {
        connection conn1;
        if (conn1.connecttodb(starg.connstr1, starg.charset) != 0)
        {
            logfile.write("[readincfield: connect to database failed] conn1.connecttodb(%s, %s)\n", 
                starg.connstr1, starg.charset);
            return false;
        }

        sqlstatement stmtupt(&conn1);
        stmtupt.prepare("update T_MAXINCVALUE set maxincvalue=:1 where pname=:2");
        stmtupt.bindin(1, maxincvalue);
        stmtupt.bindin(2, starg.pname);
        if (stmtupt.execute() != 0) // 执行失败
        {
            if (stmtupt.rc() == 942) // 如果表不存在，stmt.execute()将返回ORA-00942的错误
            {
                // 如果表不存在，就创建表，然后插入记录
                conn1.execute("create table T_MAXINCVALUE(pname varchar2(64),maxincvalue) number(15),primary key(pname)");
                conn1.execute("insert into T_MAXINCVALUE(pname,maxincvalue) values('%s',%ld)", starg.pname, maxincvalue);
                conn1.commit();
                return true;
            }
            else
            {
                logfile.write("[writeincfield: execute update sql failed] sql: %s\nerror: %s\n", 
                    stmtupt.sql(), stmtupt.message());
            }
        }
        else // 执行成功
        {
            if (stmtupt.rpc() == 0) // 如果更新的记录不存在，就插入记录
                conn1.execute("insert into T_MAXINCVALUE(pname,maxincvalue) values('%s',%ld)", starg.pname, maxincvalue);

            conn1.commit();
            return true;
        }
    }
    else if (strlen(starg.incfilename) > 0)
    {
        cofile ofile;
        if (ofile.open(starg.incfilename, false) == false) // cofile::open()函数支持自动创建文件
        {
            logfile.write("[writeincfield: open file failed] ofile.open(%s)\n", starg.incfilename);
            return false;
        }

        ofile.writeline("%ld", maxincvalue); // 默认覆盖写
    }
    else return false;

    return true;  
}

bool instarttime()
{
    if (strlen(starg.starttime) > 0)
    {
        string strhh24 = ltime1("hh24", 0); // 获取当前时间，只需要小时
        // strstr返回字符串中首次出现子串的地址，返回0表示没找到
        if (strstr(starg.starttime, strhh24.c_str()) == 0) return false;
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
    "Using:dminingoracle logfilename xmlbuffer\n\n"
    "Sample:\n"
    "/MDC/bin/tools/procctl 3600 /MDC/bin/tools/dminingoracle /MDC/log/tools/dminingoracle_ZHOBTCODE.log "
    "\"<connstr>idc/idcpwd@snorcl11g_132</connstr><charset>Simplified Chinese_China.AL32UTF8</charset>"
    "<selectsql>select obtid,cityname,provname,lat,lon,height from T_ZHOBTCODE where obtid like '5%%'</selectsql>"
    "<fieldstr>obtid,cityname,provname,lat,lon,height</fieldstr><fieldlen>5,30,30,10,10,10</fieldlen>"
    "<bfilename>ZHOBTCODE</bfilename><efilename>togxpt</efilename><outpath>/MDC/data/dmin/idc</outpath>"
    "<timeout>30</timeout><pname>dminingoracle_ZHOBTCODE</pname>\"\n\n"

    "/MDC/bin/tools/procctl   30 /MDC/bin/tools/dminingoracle /MDC/log/tools/dminingoracle_ZHOBTMIND.log "\
    "\"<connstr>idc/idcpwd@snorcl11g_132</connstr><charset>Simplified Chinese_China.AL32UTF8</charset>"\
    "<selectsql>select obtid,to_char(ddatetime,'yyyymmddhh24miss'),t,p,u,wd,wf,r,vis,keyid from T_ZHOBTMIND where keyid>:1 and obtid like '5%%'</selectsql>"\
    "<fieldstr>obtid,ddatetime,t,p,u,wd,wf,r,vis,keyid</fieldstr><fieldlen>5,19,8,8,8,8,8,8,8,15</fieldlen>"\
    "<bfilename>ZHOBTMIND</bfilename><efilename>togxpt</efilename><outpath>/MDC/data/dmin/idc</outpath>"\
    "<starttime></starttime><incfield>keyid</incfield>"\
    "<incfilename>/MDC/data/dmin/incfile/dminingoracle_ZHOBTMIND_togxpt.keyid</incfilename>"\
    "<timeout>30</timeout><pname>dminingoracle_ZHOBTMIND_togxpt</pname>"\
    "<maxcount>1000</maxcount><connstr1>scott/scottpwd@snorcl11g_132</connstr1>\"\n\n"

    "本程序是数据中心的公共功能模块，用于从Oracle数据库源表抽取数据，生成xml文件\n"
    "logfilename 本程序运行的日志文件\n"
    "xmlbuffer   本程序运行的参数，用xml表示，具体如下：\n\n"

    "connstr     数据源数据库的连接参数，格式：username/passwd@tnsname\n"
    "charset     数据库的字符集，这个参数要与数据源数据库保持一致，否则会出现中文乱码的情况\n"
    "selectsql   从数据源数据库抽取数据的SQL语句，如果是增量抽取，一定要用递增字段作为查询条件，如where keyid>:1\n"
    "fieldstr    抽取数据的SQL语句输出结果集的字段名列表，中间用逗号分隔，将作为xml文件的字段名\n"
    "fieldlen    抽取数据的SQL语句输出结果集字段的长度列表，中间用逗号分隔。fieldstr与fieldlen的字段必须一一对应\n"
    "outpath     输出xml文件存放的目录\n"
    "bfilename   输出xml文件的前缀\n"
    "efilename   输出xml文件的后缀。\n"
    "maxcount    输出xml文件的最大记录数，缺省是0，表示无限制，如果本参数取值为0，注意适当加大timeout的取值，防止程序超时\n"
    "starttime   程序运行的时间区间，例如02,13表示：如果程序启动时，踏中02时和13时则运行，其它时间不运行，"
                "如果starttime为空，表示不启用，只要本程序启动，就会执行数据抽取任务，为了减少数据源数据库压力，"
                "抽取数据的时候，如果对时效性没有要求，一般在数据源数据库空闲的时候时进行\n"
    "incfield    递增字段名，它必须是fieldstr中的字段名，并且只能是整型，一般为自增字段"
                "如果incfield为空，表示不采用增量抽取的方案\n"
    "incfilename 已抽取数据的递增字段最大值存放的文件，如果该文件丢失，将重新抽取全部的数据\n"
    "connstr1    已抽取数据的递增字段最大值存放的数据库的连接参数。connstr1和incfilename二选一，connstr1优先\n"
    "timeout     本程序的超时时间，单位：秒\n"
    "pname       进程名，尽可能采用易懂的、与其它进程不同的名称，方便故障排查\n\n";
}   

bool _xmltoarg(const string& xmlbuffer)
{
    memset(&starg,0,sizeof(struct st_arg));

    getxmlbuffer(xmlbuffer,"connstr", starg.connstr, 127);       
    if (strlen(starg.connstr) == 0) { logfile.write("connstr is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"charset", starg.charset, 63);         
    if (strlen(starg.charset) == 0) { logfile.write("charset is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"selectsql", starg.selectsql, 1023);
    if (strlen(starg.selectsql) == 0) { logfile.write("selectsql is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"fieldstr", starg.fieldstr, 511);
    if (strlen(starg.fieldstr) == 0) { logfile.write("fieldstr is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"fieldlen", starg.fieldlen, 511);         
    if (strlen(starg.fieldlen) == 0) { logfile.write("fieldlen is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"bfilename",starg.bfilename, 31);   
    if (strlen(starg.bfilename) == 0) { logfile.write("bfilename is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"efilename",starg.efilename, 31);    
    if (strlen(starg.efilename) == 0) { logfile.write("efilename is null.\n"); return false; }

    getxmlbuffer(xmlbuffer,"outpath",starg.outpath, 255);       
    if (strlen(starg.outpath) == 0) { logfile.write("outpath is null.\n"); return false; }

    getxmlbuffer(xmlbuffer, "maxcount", starg.maxcount);       

    getxmlbuffer(xmlbuffer, "starttime", starg.starttime, 63);     

    getxmlbuffer(xmlbuffer, "incfield", starg.incfield, 31);          

    getxmlbuffer(xmlbuffer, "incfilename", starg.incfilename, 255);  

    getxmlbuffer(xmlbuffer, "connstr1", starg.connstr1, 127);          

    getxmlbuffer(xmlbuffer, "timeout", starg.timeout);       
    if (starg.timeout==0) { logfile.write("timeout is null.\n");  return false; }

    getxmlbuffer(xmlbuffer, "pname", starg.pname, 63);     
    if (strlen(starg.pname)==0) { logfile.write("pname is null.\n");  return false; }

    // 拆分starg.fieldstr到fieldname中。
    fieldname.splittocmd(starg.fieldstr, ",");

    // 拆分starg.fieldlen到fieldlen中。
    fieldlen.splittocmd(starg.fieldlen, ",");

    // 判断fieldname和fieldlen两个数组的大小是否相同。
    if (fieldlen.size() != fieldname.size())
    {
        logfile.write("fieldstr和fieldlen的元素个数不一致\n"); return false;
    }

    // 如果是增量抽取，incfilename和connstr1必二选一。
    if (strlen(starg.incfield) > 0)
    {
        if ((strlen(starg.incfilename) == 0) && (strlen(starg.connstr1) == 0))
        {
            logfile.write("如果是增量抽取，incfilename和connstr1必二选一，不能都为空\n"); return false;
        }
    }

    return true;
}