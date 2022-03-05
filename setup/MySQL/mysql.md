# Setting the root password and building the airflow metadata database
sudo mysql -u root
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'j2ssql';
FLUSH PRIVILEGES;
exit from mysql
sudo /etc/init.d/mysql restart

# Uninstall and reinstall
1. First, remove already installed mysql-server using--
sudo apt-get remove --purge mysql-server mysql-client mysql-common
2. Then clean all files
sudo apt-get autoremove
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