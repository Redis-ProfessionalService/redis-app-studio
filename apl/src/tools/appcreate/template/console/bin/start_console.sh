#!/bin/bash
#
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-17.0.1.jdk/Contents/Home
export JAVA_CMD=$JAVA_HOME/bin/java
#
export APP_HOME=/home/apps/${maven_artifact_id}
#
pushd $APP_HOME
echo Starting ${maven_title}
nohup $JAVA_CMD -Xms256m -Xmx1024m -jar jar/${maven_artifact_id}.jar -run console > $APP_HOME/log/start-${maven_artifact_id}.log 2>/dev/null &
popd
exit 0

