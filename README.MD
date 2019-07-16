项目说明
=====
数据中心mysql --> mongodb 的同步


建造镜像
---

```构建镜像
docker build -t registry.cn-shenzhen.aliyuncs.com/jzdev/jqupdate:v0.0.1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jqupdate:v0.0.1
```

启动服务
---
```bash
docker run -itd --name jqrunner01 \
--env-file config.env \
-v /Users/furuiyang/codes/jq_service/update/logs:/jq_service/update/logs  \
registry.cn-shenzhen.aliyuncs.com/jzdev/jqupdate:v0.0.1 
```

查看日志
---
```查看(最后 200 行)命令行日志
docker logs -f -t --tail 200 run_jqsync
```
```进入运行容器查看日志文件 在启动的时候需要进行文件挂载 
docker exec -it run_jqsync /bin/bash 
```



``` 流程
docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jqupdate:v0.0.1

# 创建 config.env 

docker run -itd --name jqrunner01 \
--env-file config.env \
registry.cn-shenzhen.aliyuncs.com/jzdev/jqupdate:v0.0.1 
