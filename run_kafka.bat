@echo off

:: Open a Command Prompt and start Zookeeper
title Zookeeper
start cmd /k "cd /d C:\\kafka && .\bin\windows\zookeeper-server-start.bat config\zookeeper.properties"

:: Wait 10 seconds
timeout /t 10 /nobreak > nul

:: Open another Command Prompt and start Kafka Server
title Kafka Server
start cmd /k "cd /d C:\\kafka && .\bin\windows\kafka-server-start.bat config\server.properties && timeout /t 5 /nobreak > nul && .\bin\windows\kafka-server-start.bat config\server.properties"

:: Wait 10 seconds
timeout /t 10 /nobreak > nul

:: Open another Command Prompt and start the Kafka Console Consumer for reddit_stream
title Reddit Stream Consumer
start cmd /k "cd /d C:\\kafka && .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic reddit_stream"

:: Ensure the script exits without closing any of the started windows
exit
