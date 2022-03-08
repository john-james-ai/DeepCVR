sudo mysql -u root -p
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'j2ssql';
grant all privileges on *.* to 'root@localhost' IDENTIFIED WITH mysql_native_password BY 'j2ssql' with grant option;
FLUSH PRIVILEGES;
exit from mysql
sudo /etc/init.d/mysql restart

# One line version
sudo mysql -e "ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'j2ssql'"