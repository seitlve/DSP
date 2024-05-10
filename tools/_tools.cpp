#include "_tools.h"

ctcols::ctcols()
{
    inidata();
}

void ctcols::inidata()
{
    m_vallcols.clear();
    m_vpkcols.clear();
    m_allcols.clear();
    m_pkcols.clear();
}

// 查找表字段名的方法
// 查询USER_TABLE_COLUMNS，表名要大写，返回的数据转成小写
// select lower(column_name),lower(data_type),data_length from USER_TAB_COLUMNS \
         where table_name=upper(:1) order by column_id
bool ctcols::allcols(connection& conn, char* tablename)
{
    m_vallcols.clear();
    m_allcols.clear();

    st_col stcol;

    sqlstatement stmtsel(&conn);
    stmtsel.prepare
    (
        "select lower(column_name),lower(data_type),data_length from USER_TAB_COLUMNS "
             "where table_name=upper(:1) order by column_id"
    );
    stmtsel.bindin(1, tablename, 31);
    stmtsel.bindout(1, stcol.colname, 31);
    stmtsel.bindout(2, stcol.datatype, 31);
    stmtsel.bindout(3, stcol.collen);

    // select sql执行失败的原因只有网络断开或数据库出了问题
    if (stmtsel.execute() != 0) return false;

    while (true)
    {
        memset(&stcol, 0, sizeof(struct st_col));

        if (stmtsel.next() != 0) break;

        // 列的数据类型，分为char、date和number三大类
        // 如果业务有需要，可以修改以下的代码，增加对更多数据类型的支持

        // 各种字符串类型，rowid当成字符串处理
        if (strcmp(stcol.datatype, "char") == 0)      strcpy(stcol.datatype, "char");
        if (strcmp(stcol.datatype, "nchar") == 0)     strcpy(stcol.datatype, "char");
        if (strcmp(stcol.datatype, "varchar2") == 0)  strcpy(stcol.datatype, "char");
        if (strcmp(stcol.datatype, "nvarchar2") == 0) strcpy(stcol.datatype, "char");
        if (strcmp(stcol.datatype, "rowid") == 0)   { strcpy(stcol.datatype, "char"); stcol.collen = 18; }

        // 日期时间类型yyyymmddhh24miss
        if (strcmp(stcol.datatype, "date") == 0)      stcol.collen = 14; 
     
        // 数字类型 
        if (strcmp(stcol.datatype, "number") == 0)    strcpy(stcol.datatype, "number");
        if (strcmp(stcol.datatype, "integer") == 0)   strcpy(stcol.datatype, "number");
        if (strcmp(stcol.datatype, "float") == 0)     strcpy(stcol.datatype, "number");  

        // 如果字段的数据类型不在上面列出来的中，忽略它
        if ((strcmp(stcol.datatype, "char") != 0) &&
            (strcmp(stcol.datatype, "date") != 0) &&
            (strcmp(stcol.datatype, "number") != 0)) continue;

        // 如果字段类型是number，把长度设置为22
        if (strcmp(stcol.datatype, "number") == 0) stcol.collen = 22;

        m_allcols = m_allcols + stcol.colname + ","; // 拼接全部字段字符串

        m_vallcols.push_back(stcol); // 把字段信息放入容器中
    }

    // 删除m_allcols最后一个多余的逗号
    if (stmtsel.rpc() > 0) deleterchr(m_allcols, ','); 

    return true;
}

// 查询主键的方法
// 从USER_CONSTRAINTS中查询主键名，再从USER_CONS_COLUMNS查询主键的字段
// select lower(column_name),position from USER_CONS_COLUMNS \
         where table_name=upper(:1) \
             and constraint_name=\
                 (select constraint_name from USER_CONSTRAINTS \
                     where table_name=upper(:2) and constraint_type='P' \
                         and generated='USER NAME')
bool ctcols::pkcols(connection& conn, char* tablename)
{
    m_vpkcols.clear();
    m_pkcols.clear();

    st_col stcol;

    sqlstatement stmtsel(&conn);
    stmtsel.prepare
    (
        "select lower(column_name),position from USER_CONS_COLUMNS \
             where table_name=upper(:1) \
                 and constraint_name=\
                     (select constraint_name from USER_CONSTRAINTS \
                         where table_name=upper(:2) and constraint_type='P' \
                             and generated='USER NAME')"
    );
    stmtsel.bindin(1, tablename);
    stmtsel.bindin(2, tablename);
    stmtsel.bindout(1, stcol.colname, 31);
    stmtsel.bindout(2, stcol.pkseq);

    if (stmtsel.execute() != 0) return false;

    while (true)
    {
        memset(&stcol, 0, sizeof(struct st_col));

        if (stmtsel.next() != 0) break;

        // 同步信息
        for (auto& e : m_vallcols)
        {
            if (strcmp(stcol.colname, e.colname) == 0)
            {
                strcpy(stcol.datatype, e.datatype);
                stcol.collen = e.collen;
                e.pkseq = stcol.pkseq;
            }
        }

        m_pkcols = m_pkcols + stcol.colname + ","; // 拼接主键字符串

        m_vpkcols.push_back(stcol); // 把主键信息放入m_vpkcols容器中
    }

    // 删除m_pkcols最后一个多余的逗号
    if (stmtsel.rpc() > 0) deleterchr(m_pkcols, ','); 

    return true;
}