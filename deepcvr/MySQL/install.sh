# 1. First, remove already installed mysql-server using--
sudo apt-get remove --purge mysql-server mysql-client mysql-common
# 2. Then clean all files
sudo apt-get autoremove
# 3. Update Packages
sudo apt update
# 4. Install MySQL
sudo apt install mysql-server
# 5. Start MySQL Server
sudo /etc/init.d/mysql start
# 6. Run Security Installation
sudo mysql_secure_installation
# 7. Open MySQL Prompt
sudo mysql