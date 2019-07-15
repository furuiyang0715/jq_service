FROM registry.cn-shenzhen.aliyuncs.com/jzdev/tinibase:1.0.0

ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN mkdir /jq_service

WORKDIR /jq_service

ADD . /jq_service

WORKDIR /jq_service
RUN pip install -r requirements.txt -i https://pypi.douban.com/simple

ENTRYPOINT ["python", "run.py"]