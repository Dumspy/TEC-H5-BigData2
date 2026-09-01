#!/usr/bin/env bash
set -euo pipefail

HADOOP_VERSION=3.5.0
JAVA_HOME_PATH=/usr/lib/jvm/java-21-openjdk-amd64

# ask where to install hadoop (default: this script's directory)
read -r -e -p "Where should hadoop be installed? " -i "$(cd "$(dirname "$0")" && pwd)" INSTALL_DIR
INSTALL_DIR="$(cd "$INSTALL_DIR" && pwd)"
HADOOP_HOME="$INSTALL_DIR/hadoop-${HADOOP_VERSION}"

# install openjdk 21
[ -d "$JAVA_HOME_PATH" ] || sudo apt-get install -y openjdk-21-jdk

# download to temp dir and unpack
TMP_DIR=$(mktemp -d)
wget -q "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -O "$TMP_DIR/hadoop.tar.gz"
tar -xzf "$TMP_DIR/hadoop.tar.gz" -C "$INSTALL_DIR"

# point hadoop-env.sh at the jdk
sed -i "s|^# export JAVA_HOME=.*|export JAVA_HOME=${JAVA_HOME_PATH}|" "$HADOOP_HOME/etc/hadoop/hadoop-env.sh"

# hdfs storage dirs
mkdir -p "$HADOOP_HOME/hdfs/namenode" "$HADOOP_HOME/hdfs/datanode"

# core-site.xml
cat > "$HADOOP_HOME/etc/hadoop/core-site.xml" << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
EOF

# hdfs-site.xml
cat > "$HADOOP_HOME/etc/hadoop/hdfs-site.xml" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>

  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file://${HADOOP_HOME}/hdfs/namenode</value>
  </property>

  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file://${HADOOP_HOME}/hdfs/datanode</value>
  </property>
</configuration>
EOF

# hadoop env vars
cat >> ~/.bashrc << EOF
# hadoop
export HADOOP_HOME=$HADOOP_HOME
export HADOOP_INSTALL=\$HADOOP_HOME
export HADOOP_MAPRED_HOME=\$HADOOP_HOME
export HADOOP_COMMON_HOME=\$HADOOP_HOME
export HADOOP_HDFS_HOME=\$HADOOP_HOME
export HADOOP_YARN_HOME=\$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=\$HADOOP_HOME/lib/native
export PATH=\$PATH:\$HADOOP_HOME/sbin:\$HADOOP_HOME/bin
export HADOOP_OPTS="-Djava.library.path=\$HADOOP_HOME/lib/native"
EOF

rm -rf "$TMP_DIR"
