根据https://hyperdash.info/里找到的想跟单的地址，自动跟单交易，支持滑点，使用订单薄的hvn和lvn做止盈和止损点，支持设置最大亏损点，支持telegram机器人通知。目前只支持在okx上复制交易

主要通过config.yaml来配置

本项目有3个子系统分别是

1.copy目录下的跟单系统

2.indicator目录下的指标系统，主要用于实时计算hvn,lvn，用来跟单时自动止盈止损

3.trade目录下的交易系统，主要使用okx交易所交易


下载源码后

1.安装rabbitmq,python-okx

2.设置时区是utc
sudo timedatectl set-timezone UTC

3.启动队列
sudo rabbitmqctl add_user monitor P%40ssw0rd
sudo rabbitmqctl set_user_tags monitor administrator
sudo rabbitmqctl set_permissions -p / monitor ".*" ".*" ".*"
sudo rabbitmqctl authenticate_user monitor 'P%40ssw0rd'

4.复制service文件下的服务到/etc/systemd/system/

5.启动服务
systemctl restart watcher
systemctl restart indicator
systemctl restart strategy



