* Install Spark

- In "/home/mine44" folder: mkdir spark
- In "spark" folder: wget https://download.java.net/java/GA/jdk11/13/GPL/openjdk-11.0.1_linux-x64_bin.tar.gz
- In "spark" folder: tar xzfv openjdk-11.0.1_linux-x64_bin.tar.gz
- In "spark" folder: rm openjdk-11.0.1_linux-x64_bin.tar.gz
- In "spark" folder:
    export JAVA_HOME="${HOME}/spark/jdk-11.0.1"
    export PATH="${JAVA_HOME}/bin:${PATH}"
- In "spark" folder: wget https://dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
- In "spark" folder: tar xzfv spark-3.3.1-bin-hadoop3.tgz
- In "spark" folder: rm spark-3.3.1-bin-hadoop3.tgz
- In "spark" folder:
    export SPARK_HOME="${HOME}/spark/spark-3.3.1-bin-hadoop3"
    export PATH="${SPARK_HOME}/bin:${PATH}"

- In "/home/mine44" folder: nano .bashrc
    add: 
        export JAVA_HOME="${HOME}/spark/jdk-11.0.1"
        export PATH="${JAVA_HOME}/bin:${PATH}"

        export SPARK_HOME="${HOME}/spark/spark-3.3.1-bin-hadoop3"
        export PATH="${SPARK_HOME}/bin:${PATH}"

- In "/home/mine44" folder: source .bashrc
- In "/home/mine44" folder: pip install pyspark
