hadoop-customShell
==================
Note : apache-hadoop shell command의 기능 보강.
- cat, text시 snappy로 압축된 파일을 decompression으로 볼 수 있 는 기능.
- ...추가예정.

Compiling
- compiling requires : 
	- java JDK 1.6,
	- ant
- dependency
	- hadoop-core-x.jar,
	- hadoop-snappy-x.jar
	
Install
- 빌드 후 생성된 hadoop-customShell.jar를 $HAOOP_HOME/lib에 복사.
- $HADOOP_HOME/bin/hadoop edit.

	elif [ "$COMMAND" = "dfs" ] ; then
  		CLASS=org.apache.hadoop.fs.FsShell
  		HADOOP_OPTS="$HADOOP_OPTS $HADOOP_CLIENT_OPTS"		

위 부분 아래 다음항복 추가.(append under item.)

	elif [ "$COMMAND" = "cufs" ] ; then
  		CLASS=com.gruter.hadoop.customShell.CustomShell
  		HADOOP_OPTS="$HADOOP_OPTS $HADOOP_CLIENT_OPTS"


Usage
- 사용법은 기존은 dfs나 fs와 같다. 다만 fs대신 cufs를 사용한다.
- $HADOOP_HOME/bin/hadoop cufs [ls, cat, text] [filePath]

Test
- test on hadoop-core-0.20.x, hadoop-croe-1.0.x

hanyounsu@gmail.com