## PR规范

1. 建立issue,描述相关问题信息
2. 基于对应的release分支拉取开发分支
3. commit 信息：[type-issueid] [module] msg
   1. type 类别
   2. feat：表示是一个新功能（feature)
   3. hotfix：hotfix，修补bug
   4. docs：改动、增加文档
   5. opt：修改代码风格及opt imports这些，不改动原有执行的代码
   6. test：增加测试

<br/>  

eg:
  [hotfix-31280][core] 修复bigdecimal转decimal运行失败问题               
  [feat-31372][rdb] RDB结果表Upsert模式支持选择更新策略
  
1. 多次提交使用rebase 合并成一个。
2. pr 名称：[flinkx-issueid][module名称] 标题