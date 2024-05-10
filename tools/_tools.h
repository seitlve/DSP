/*
    _tools.h
    存放通用的工具类
*/

#ifndef _TOOLS_H
#define _TOOLS_H

#include "_public.h"
#include "_ooci.h"

using namespace idc;

// 获取表的字段和主键的工具类
// 维护两个容器，分布对应所有字段和主键
// 包含对应的获取函数，需要传入数据库连接对象和表名
class ctcols
{
private:
    // 存放字段信息的结构体
    struct st_col
    {
        char colname[32];       // 字段名
        char datatype[32];      // 数据类型
        int collen;             // 数据长度
        int pkseq;              // 如果是主键，存放主机字段的顺序，从1开始，为0表示不是主键
    };

public:
    ctcols();

    vector<st_col> m_vallcols;  // 存放所以字段信息的容器
    vector<st_col> m_vpkcols;   // 存放主键字段信息的容器

    string m_allcols;           // 所有字段名组成的字符串，以逗号分隔
    string m_pkcols;            // 主键字段名组成的字符串，以逗号分隔

    void inidata();             // 成员初始化的函数

    // 获取所有字段信息的函数
    bool allcols(connection& conn, char* tablename);

    // 获取主键字段信息的函数
    bool pkcols(connection& conn, char* tablename);
};

#endif // _TOOLS_H