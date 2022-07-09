#uncomment if everything broken
#sudo rabbitmqctl stop_app
#sudo rabbitmqctl reset
#sudo rabbitmqctl start_app

sudo rabbitmqctl add_user serv 1234
sudo rabbitmqctl set_user_tags serv administrator
sudo rabbitmqctl set_permissions -p / serv ".*" ".*" ".*"

sudo rabbitmqctl add_user work 1234
sudo rabbitmqctl set_user_tags work administrator
sudo rabbitmqctl set_permissions -p / work ".*" ".*" ".*"
