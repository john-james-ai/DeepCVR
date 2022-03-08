# 1. First, remove already installed mysql-server using--
apt-get remove -y mysql-*
apt-get purge -y mysql-*
sudo apt-get autoclean
# Follow instructions at https://docs.microsoft.com/en-us/windows/wsl/tutorials/wsl-database
# use wsl terminal
# # 2. Upgrade distribution
# sudo apt-get dist-upgrade
# # 3. Update Packages
# sudo apt update
# # 4. Install MySQL
# sudo apt-get install mysql-server
# # 5. Start MySQL Server
# sudo /etc/init.d/mysql start
# # 6. Run Security Installation
# sudo mysql_secure_installation
# # 7. Open MySQL Prompt
# sudo mysql