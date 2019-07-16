项目说明
=====
数据中心mysql --> mongodb 的同步


建造镜像
---

```构建镜像
docker build -t registry.cn-shenzhen.aliyuncs.com/jzdev/jq_sync:v0.0.1 .  
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jq_sync:v0.0.1
```

启动服务
---
```bash
docker service create --name run_jqsync -itd --env-file config.env registry.cn-shenzhen.aliyuncs.com/jzdev/jq_sync:v0.0.1
或： 
docker run --name run_jqsync -itd --env-file config.env registry.cn-shenzhen.aliyuncs.com/jzdev/jq_sync:v0.0.1