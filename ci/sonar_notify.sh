#!/bin/bash
#参考钉钉文档 https://open-doc.dingtalk.com/microapp/serverapi2/qf2nxq
 sonarreport=$(curl -s http://172.16.100.198:8082/?projectname=dt-insight-engine/flinkStreamSQL)
 curl -s "https://oapi.dingtalk.com/robot/send?access_token=71555061297a53d3ac922a6f4d94285d8e23bccdca0c00b4dc6df0a2d49da724" \
   -H "Content-Type: application/json" \
   -d "{
     \"msgtype\": \"markdown\",
     \"markdown\": {
         \"title\":\"sonar代码质量\",
         \"text\": \"## sonar代码质量报告: \n
> [sonar地址](http://172.16.100.198:9000/dashboard?id=dt-insight-engine/flinkStreamSQL) \n
> ${sonarreport} \n\"
     }
 }"