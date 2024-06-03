# 生成的二进制文件存放的目录
BINDIR = /MDC/bin/tools/

########################
# 开发框架头文件路径，使用相对路径
PUBINCL = -I../public

# 开发框架cpp文件名，直接和程序的源代码文件一起编译，没有采用链接库，是为了方便调试。
PUBCPP = ../public/_public.cpp

##################################################
# oracle头文件路径
ORAINCL = -I$(ORACLE_HOME)/rdbms/public -I../public/db/oracle

# oracle库文件路径
ORALIB =  -L$(ORACLE_HOME)/lib -L.

# oracle的oci库
ORALIBS = -lclntsh

# 开发框架oracle的cpp文件名，这里直接包含进来，没有采用链接库，是为了方便调试。
ORACPP = ../public/db/oracle/_ooci.cpp
##################################################

# 编译选项
CFLAGS = -g
#CFLAGS = -O2

all:$(BINDIR)procctl $(BINDIR)checkproc $(BINDIR)deletefiles $(BINDIR)gzipfiles $(BINDIR)ftpgetfiles $(BINDIR)ftpputfiles \
	$(BINDIR)tcpgetfiles $(BINDIR)tcpputfiles $(BINDIR)fileserver $(BINDIR)dminingoracle $(BINDIR)xmltodb $(BINDIR)migratetable \
	$(BINDIR)syncref

$(BINDIR)procctl:procctl.cpp
	g++ $(CFLAGS) -o $(BINDIR)procctl procctl.cpp

$(BINDIR)checkproc:checkproc.cpp $(PUBCPP)
	g++ $(CFLAGS) -o $(BINDIR)checkproc checkproc.cpp $(PUBCPP) $(PUBINCL)

$(BINDIR)deletefiles:deletefiles.cpp $(PUBCPP)
	g++ $(CFLAGS) -o $(BINDIR)deletefiles deletefiles.cpp $(PUBCPP) $(PUBINCL)

$(BINDIR)gzipfiles:gzipfiles.cpp $(PUBCPP)
	g++ $(CFLAGS) -o $(BINDIR)gzipfiles gzipfiles.cpp $(PUBCPP) $(PUBINCL)

$(BINDIR)ftpgetfiles:ftpgetfiles.cpp $(PUBCPP)
	g++ $(CFLAGS) -o $(BINDIR)ftpgetfiles ftpgetfiles.cpp $(PUBCPP) ../public/_ftp.cpp $(PUBINCL) ../public/libftp.a

$(BINDIR)ftpputfiles:ftpputfiles.cpp $(PUBCPP)
	g++ $(CFLAGS) -o $(BINDIR)ftpputfiles ftpputfiles.cpp $(PUBCPP) ../public/_ftp.cpp $(PUBINCL) ../public/libftp.a

$(BINDIR)tcpgetfiles:tcpgetfiles.cpp $(PUBCPP)
	g++ $(CFLAGS) -o $(BINDIR)tcpgetfiles tcpgetfiles.cpp $(PUBCPP) $(PUBINCL)

$(BINDIR)tcpputfiles:tcpputfiles.cpp $(PUBCPP)
	g++ $(CFLAGS) -o $(BINDIR)tcpputfiles tcpputfiles.cpp $(PUBCPP) $(PUBINCL)

$(BINDIR)fileserver:fileserver.cpp $(PUBCPP)
	g++ $(CFLAGS) -o $(BINDIR)fileserver fileserver.cpp $(PUBCPP) $(PUBINCL)

$(BINDIR)dminingoracle:dminingoracle.cpp $(PUBCPP)
	g++ $(CFLAGS) -o $(BINDIR)dminingoracle dminingoracle.cpp $(PUBCPP) $(PUBINCL) $(ORACPP) $(ORAINCL) $(ORALIB) $(ORALIBS)

$(BINDIR)xmltodb:xmltodb.cpp $(PUBCPP) _tools.cpp
	g++ $(CFLAGS) -o $(BINDIR)xmltodb xmltodb.cpp $(PUBCPP) $(PUBINCL) $(ORACPP) $(ORAINCL) _tools.cpp $(ORALIB) $(ORALIBS)

$(BINDIR)migratetable:migratetable.cpp $(PUBCPP) _tools.cpp
	g++ $(CFLAGS) -o $(BINDIR)migratetable migratetable.cpp $(PUBCPP) $(PUBINCL) $(ORACPP) $(ORAINCL) _tools.cpp $(ORALIB) $(ORALIBS)

$(BINDIR)syncref:syncref.cpp $(PUBCPP) _tools.cpp
	g++ $(CFLAGS) -o $(BINDIR)syncref syncref.cpp $(PUBCPP) $(PUBINCL) $(ORACPP) $(ORAINCL) _tools.cpp $(ORALIB) $(ORALIBS)

clean:
	rm -rf $(BINDIR)procctl $(BINDIR)checkproc $(BINDIR)deletefiles $(BINDIR)gzipfiles $(BINDIR)ftpgetfiles $(BINDIR)ftpputfiles
	rm -rf $(BINDIR)tcpgetfiles $(BINDIR)tcpputfiles $(BINDIR)fileserver $(BINDIR)dminingoracle $(BINDIR)xmltodb $(BINDIR)migratetable
	rm -rf $(BINDIR)syncref