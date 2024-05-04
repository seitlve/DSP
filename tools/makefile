# 生成的二进制文件存放的目录
BINDIR = /DSP/bin/tools/

########################
# 开发框架头文件路径。
PUBINCL = -I/project/public

# 开发框架cpp文件名，直接和程序的源代码文件一起编译，没有采用链接库，是为了方便调试。
PUBCPP = /project/public/_public.cpp

##################################################
# oracle头文件路径
ORAINCL = -I$(ORACLE_HOME)/rdbms/public -I/project/public/db/oracle

# oracle库文件路径
ORALIB =  -L$(ORACLE_HOME)/lib -L.

# oracle的oci库
ORALIBS = -lclntsh

# 开发框架oracle的cpp文件名，这里直接包含进来，没有采用链接库，是为了方便调试。
ORACPP = /project/public/db/oracle/_ooci.cpp
##################################################

# 编译选项
CFLAGS = -g
#CFLAGS = -O2

all:procctl

procctl:procctl.cpp
	g++ $(CFLAGS) -o $(BINDIR)procctl procctl.cpp

clean:
	rm -f $(BINDIR)procctl