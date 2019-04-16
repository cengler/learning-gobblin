## Doc Oficial:
https://gobblin.readthedocs.io/en/latest/

## Instalar:
Bajarse release de:
https://github.com/apache/incubator-gobblin/releases

Compilar:

    ./gradlew :gobblin-distribution:buildDistributionTar

El binario en un tar queda en -> descomprimir:

    build/gobblin-distribution/distributions/


## Test Wikipedia Job:
    bin/gobblin run wikipedia -lookback P10D -avroOutput /tmp/wikiSample LinkedIn

## Workflow general:
Source -> Extractor -> [ Converters | PolicyCheckers ] -> DataWriter -> DataPublisher

## Gobblin as a Daemon:
    
    GOBBLIN_JOB_CONFIG_DIR=..../learning-gobblin/gobblin-data/jobs
    GOBBLIN_WORK_DIR=..../learning-gobblin/gobblin-data/work
    JAVA_HOME=$(/usr/libexec/java_home) --> (MAC)

luego correr

    gobblin-dist/bin/gobblin-standalone.sh start