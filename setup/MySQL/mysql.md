# See if any mysql processes are running
sudo  ps -ef | grep mysql
# Kill the process if it is running
sudo kill -9 'pid' mysqld
# Run MySQL safe daemon with skipping grant tables
sudo mysqld_safe --skip-grant-tables &
sudo mysqld --skip-grant-tables &
# Login to MySQL as root with no password
sudo mysql -u root mysql
# Setting the root password and building the airflow metadata database
sudo mysql -u root
ALTER USER 'john'@'localhost' IDENTIFIED WITH mysql_native_password BY 'j2ssql';
FLUSH PRIVILEGES;
exit from mysql
sudo /etc/init.d/mysql restart

# Uninstall and reinstall
1. First, remove already installed mysql-server using--
sudo apt-get remove --purge mysql-server mysql-client mysql-common
2. Then clean all files
sudo apt-get autoremove
sudo apt-get remove -y mysql-*
sudo apt-get purge -y mysql-*
3. Update Packages
sudo apt update
4. Install MySQL
sudo apt install mysql-server
5. Start MySQL Server
sudo /etc/init.d/mysql start
6. Run Security Installation
sudo mysql_secure_installation
7. Open MySQL Prompt
sudo mysql -u root -p

# Bailout
airflow db reset

# Installing mysql from docs
# 1 Adding the MySQL APT Repository
sudo dpkg -i setup/MySQL/mysql-apt-config_0.8.22-1_all.deb
sudo apt-get update
sudo apt-get install mysql-server

# Start and login
sudo service mysql start
mysql -u john -p
