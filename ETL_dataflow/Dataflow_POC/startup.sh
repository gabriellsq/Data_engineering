#! /bin/bash
apt-get update
apt-get install -y nginx
service nginx start
sed -i -- 's/nginx/Google Cloud Platform - '"$HOSTNAME"'/' /var/www/html/index.nginx-debian.html

apt-get install python-pip
sudo pip install apache-beam[gcp] oauth2client==3.0.0 
sudo pip install -U pip
sudo pip install Faker==1.0.2